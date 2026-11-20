package fleet

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/krishnapanjiyar/pgfleet/internal/pg"
	"github.com/krishnapanjiyar/pgfleet/internal/pool"
	"github.com/krishnapanjiyar/pgfleet/internal/runner"
)

// Check is the result of one verification step on one node.
type Check struct {
	Node      string  `json:"node"`
	Name      string  `json:"check"`
	OK        bool    `json:"ok"`
	Detail    string  `json:"detail,omitempty"`
	LatencyMS float64 `json:"latency_ms"`
}

// Report is what every command returns (and what --json prints).
type Report struct {
	Command   string  `json:"command"`
	OK        bool    `json:"ok"`
	Checks    []Check `json:"checks"`
	ElapsedMS float64 `json:"elapsed_ms"`
	Snapshot  string  `json:"snapshot,omitempty"`  // backup
	Candidate string  `json:"candidate,omitempty"` // failover-check: best promotion target
}

// Fleet binds a config to a runner.
type Fleet struct {
	Cfg          *Config
	R            runner.Runner
	Workers      int
	Log          *slog.Logger
	Now          func() time.Time
	PollInterval time.Duration
	ReadyTimeout time.Duration
}

func New(cfg *Config, r runner.Runner, workers int, log *slog.Logger) *Fleet {
	if log == nil {
		log = slog.New(slog.NewJSONHandler(os.Stderr, nil))
	}
	return &Fleet{Cfg: cfg, R: r, Workers: workers, Log: log, Now: time.Now,
		PollInterval: 500 * time.Millisecond, ReadyTimeout: 60 * time.Second}
}

// check times fn, records the outcome and emits one structured log line.
func (f *Fleet) check(node, name string, fn func() (string, error)) Check {
	start := time.Now()
	detail, err := fn()
	c := Check{Node: node, Name: name, OK: err == nil, Detail: detail,
		LatencyMS: float64(time.Since(start).Microseconds()) / 1000}
	if err != nil {
		c.Detail = err.Error()
	}
	lvl := slog.LevelInfo
	if !c.OK {
		lvl = slog.LevelWarn
	}
	f.Log.Log(context.Background(), lvl, "check", "node", node, "check", name, "ok", c.OK,
		"latency_ms", c.LatencyMS, "detail", c.Detail)
	return c
}

func (f *Fleet) finish(cmd string, start time.Time, checks []Check) *Report {
	r := &Report{Command: cmd, OK: true, Checks: checks, ElapsedMS: float64(time.Since(start).Microseconds()) / 1000}
	for _, c := range checks {
		if !c.OK {
			r.OK = false
		}
	}
	f.Log.Info("command_done", "command", cmd, "ok", r.OK, "elapsed_ms", r.ElapsedMS, "checks", len(checks))
	return r
}

func (f *Fleet) sql(ctx context.Context, node, q string) (string, error) {
	res, err := f.R.Exec(ctx, node, "psql", "-U", "postgres", "-tAq", "-c", q)
	return strings.TrimSpace(res.Stdout), err
}

func (f *Fleet) waitReady(ctx context.Context, node string) error {
	deadline := time.Now().Add(f.ReadyTimeout)
	for {
		if _, err := f.R.Exec(ctx, node, "pg_isready", "-U", "postgres"); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("not ready after %s: %w", f.ReadyTimeout, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.PollInterval):
		}
	}
}

// ---------------------------------------------------------------- provision

const replicaScript = `set -e
export PGDATA=/var/lib/postgresql/data
until pg_isready -h "$PRIMARY" -p 5432 -U postgres; do sleep 1; done
rm -rf "$PGDATA"/*
chown -R postgres:postgres /var/lib/postgresql
gosu postgres env PGPASSWORD="$REPL_PW" pg_basebackup -h "$PRIMARY" -U replicator -D "$PGDATA" -R -X stream -C -S "$SLOT"
chmod 700 "$PGDATA"
exec gosu postgres postgres -c hot_standby=on -c listen_addresses='*'`

// Provision starts the primary, then clones each replica from it in parallel.
func (f *Fleet) Provision(ctx context.Context) *Report {
	start := time.Now()
	cfg := f.Cfg
	pw := os.Getenv(cfg.PasswordEnv)
	var checks []Check

	checks = append(checks, f.check("-", "network", func() (string, error) {
		_, err := f.R.Docker(ctx, "network", "create", cfg.Network())
		if err != nil && strings.Contains(err.Error(), "already exists") {
			return "exists", nil
		}
		return cfg.Network(), err
	}))

	p := cfg.Primary()
	checks = append(checks, f.check(p.Name, "run", func() (string, error) {
		_, err := f.R.Docker(ctx, "run", "-d", "--name", p.Name, "--network", cfg.Network(),
			"--label", "pgfleet.cluster="+cfg.Cluster, "--label", "pgfleet.role=primary",
			"-p", strconv.Itoa(p.Port)+":5432", "-e", "POSTGRES_PASSWORD="+pw,
			"-v", "pgfleet-"+p.Name+"-data:/var/lib/postgresql/data", "-v", cfg.BackupDir+":/backups",
			cfg.Image,
			"-c", "wal_level=replica", "-c", "archive_mode=on",
			"-c", "archive_command=mkdir -p /backups/wal && test ! -f /backups/wal/%f && cp %p /backups/wal/%f",
			"-c", "max_wal_senders=10", "-c", "max_replication_slots=10", "-c", "hot_standby=on",
			"-c", "listen_addresses=*")
		return cfg.Image, err
	}))
	checks = append(checks, f.check(p.Name, "ready", func() (string, error) { return "", f.waitReady(ctx, p.Name) }))
	if !allOK(checks) {
		return f.finish("provision", start, checks)
	}
	checks = append(checks, f.check(p.Name, "replication_role", func() (string, error) {
		if _, err := f.sql(ctx, p.Name, "CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '"+strings.ReplaceAll(pw, "'", "''")+"'"); err != nil {
			return "", err
		}
		_, err := f.R.Exec(ctx, p.Name, "bash", "-c",
			`echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"`)
		if err != nil {
			return "", err
		}
		_, err = f.sql(ctx, p.Name, "SELECT pg_reload_conf()")
		return "", err
	}))

	reps := cfg.Replicas()
	perNode := pool.Map(ctx, f.Workers, reps, func(ctx context.Context, _ int, n Node) []Check {
		cs := []Check{f.check(n.Name, "run", func() (string, error) {
			_, err := f.R.Docker(ctx, "run", "-d", "--name", n.Name, "--network", cfg.Network(),
				"--label", "pgfleet.cluster="+cfg.Cluster, "--label", "pgfleet.role=replica",
				"-p", strconv.Itoa(n.Port)+":5432", "-e", "PRIMARY="+p.Name, "-e", "REPL_PW="+pw,
				"-e", "SLOT="+strings.ReplaceAll(n.Name, "-", "_"),
				"-v", "pgfleet-"+n.Name+"-data:/var/lib/postgresql/data",
				"--entrypoint", "bash", cfg.Image, "-c", replicaScript)
			return "pg_basebackup from " + p.Name, err
		})}
		if cs[0].OK {
			cs = append(cs, f.check(n.Name, "ready", func() (string, error) { return "", f.waitReady(ctx, n.Name) }))
		}
		return cs
	})
	for _, cs := range perNode {
		checks = append(checks, cs...)
	}
	return f.finish("provision", start, checks)
}

func allOK(cs []Check) bool {
	for _, c := range cs {
		if !c.OK {
			return false
		}
	}
	return true
}

// ------------------------------------------------------------------- verify

// Verify checks every node in parallel: readiness, a query, wal_level, that the
// actual role matches the configured role and, with wal=true, WAL integrity.
func (f *Fleet) Verify(ctx context.Context, wal bool) *Report {
	start := time.Now()
	perNode := pool.Map(ctx, f.Workers, f.Cfg.Nodes, func(ctx context.Context, _ int, n Node) []Check {
		return f.verifyNode(ctx, n, wal)
	})
	var checks []Check
	for _, cs := range perNode {
		checks = append(checks, cs...)
	}
	return f.finish("verify", start, checks)
}

func (f *Fleet) verifyNode(ctx context.Context, n Node, wal bool) []Check {
	ready := f.check(n.Name, "ready", func() (string, error) {
		r, err := f.R.Exec(ctx, n.Name, "pg_isready", "-U", "postgres")
		return strings.TrimSpace(r.Stdout), err
	})
	cs := []Check{ready}
	if !ready.OK {
		return cs
	}
	cs = append(cs,
		f.check(n.Name, "query", func() (string, error) {
			out, err := f.sql(ctx, n.Name, "SELECT 1")
			if err == nil && out != "1" {
				err = fmt.Errorf("SELECT 1 returned %q", out)
			}
			return "", err
		}),
		f.check(n.Name, "wal_level", func() (string, error) {
			out, err := f.sql(ctx, n.Name, "SHOW wal_level")
			if err == nil && out != "replica" && out != "logical" {
				err = fmt.Errorf("wal_level=%q, need replica or logical", out)
			}
			return out, err
		}),
		f.check(n.Name, "role", func() (string, error) {
			out, err := f.sql(ctx, n.Name, "SELECT pg_is_in_recovery()")
			if err != nil {
				return "", err
			}
			actual := map[string]string{"f": "primary", "t": "replica"}[out]
			if actual != n.Role {
				return "", fmt.Errorf("configured as %s but running as %s", n.Role, actual)
			}
			return actual, nil
		}))
	// WAL positions are only meaningful when the node runs in its configured role.
	if wal && cs[len(cs)-1].OK {
		cs = append(cs, f.check(n.Name, "wal_integrity", func() (string, error) { return f.walIntegrity(ctx, n) }))
	}
	return cs
}

// walIntegrity decodes every WAL record between the last checkpoint's REDO
// location and the current flush/receive position with pg_waldump.
func (f *Fleet) walIntegrity(ctx context.Context, n Node) (string, error) {
	cd, err := f.R.Exec(ctx, n.Name, "bash", "-c", `pg_controldata "$PGDATA"`)
	if err != nil {
		return "", err
	}
	from, err := pg.ParseControlRedo(cd.Stdout)
	if err != nil {
		return "", err
	}
	q := "SELECT pg_current_wal_flush_lsn()"
	if n.Role == "replica" {
		q = "SELECT pg_last_wal_receive_lsn()"
	}
	endStr, err := f.sql(ctx, n.Name, q)
	if err != nil {
		return "", err
	}
	to, err := pg.ParseLSN(endStr)
	if err != nil {
		return "", err
	}
	if to <= from {
		return "no WAL since checkpoint", nil
	}
	_, err = f.R.Exec(ctx, n.Name, "bash", "-c",
		fmt.Sprintf(`pg_waldump -p "$PGDATA/pg_wal" -s %s -e %s > /dev/null`, from, to))
	return fmt.Sprintf("%s..%s decoded", from, to), err
}

// ------------------------------------------------------------------- backup

// Backup takes a physical base backup (with streamed WAL) on the primary and
// verifies its manifest with pg_verifybackup.
func (f *Fleet) Backup(ctx context.Context) *Report {
	start := time.Now()
	p := f.Cfg.Primary()
	snap := "snap-" + f.Now().UTC().Format("20060102T150405Z")
	path := "/backups/" + snap
	checks := []Check{f.check(p.Name, "basebackup", func() (string, error) {
		_, err := f.R.Exec(ctx, p.Name, "pg_basebackup", "-U", "postgres", "-D", path,
			"-F", "p", "-X", "stream", "-c", "fast", "--manifest-checksums=SHA256")
		return path, err
	})}
	if checks[0].OK {
		checks = append(checks, f.check(p.Name, "verifybackup", func() (string, error) {
			_, err := f.R.Exec(ctx, p.Name, "pg_verifybackup", path)
			return "", err
		}))
	}
	rep := f.finish("backup", start, checks)
	if rep.OK {
		rep.Snapshot = snap
	}
	return rep
}

// ------------------------------------------------------------------ restore

const restoreScript = `set -e
cp -a "/backups/$SNAP" /tmp/restore
chown -R postgres:postgres /tmp/restore
chmod 700 /tmp/restore
exec gosu postgres postgres -D /tmp/restore -c listen_addresses='*'`

// Restore validates a snapshot by booting a throw-away server from it and
// running sanity queries. The scratch container is always removed unless keep.
func (f *Fleet) Restore(ctx context.Context, snapshot string, keep bool) *Report {
	start := time.Now()
	if snapshot == "" || strings.ContainsAny(snapshot, "/ \t") {
		return f.finish("restore", start, []Check{{Node: "-", Name: "snapshot", Detail: "invalid snapshot name"}})
	}
	name := "pgfleet-restore-" + snapshot
	cfg := f.Cfg
	var checks []Check
	checks = append(checks, f.check(name, "run", func() (string, error) {
		_, err := f.R.Docker(ctx, "run", "-d", "--name", name, "--label", "pgfleet.cluster="+cfg.Cluster,
			"-e", "SNAP="+snapshot, "-v", cfg.BackupDir+":/backups:ro",
			"--entrypoint", "bash", cfg.Image, "-c", restoreScript)
		return "booting " + snapshot, err
	}))
	if checks[0].OK {
		if !keep {
			defer func() { _, _ = f.R.Docker(context.Background(), "rm", "-f", name) }()
		}
		checks = append(checks, f.check(name, "ready", func() (string, error) { return "", f.waitReady(ctx, name) }))
		if checks[1].OK {
			checks = append(checks,
				f.check(name, "query", func() (string, error) {
					out, err := f.sql(ctx, name, "SELECT 1")
					if err == nil && out != "1" {
						err = fmt.Errorf("SELECT 1 returned %q", out)
					}
					return "", err
				}),
				f.check(name, "recovery_finished", func() (string, error) {
					out, err := f.sql(ctx, name, "SELECT pg_is_in_recovery()")
					if err == nil && out != "f" {
						err = fmt.Errorf("server still in recovery")
					}
					return "", err
				}))
		}
	}
	return f.finish("restore", start, checks)
}

// ----------------------------------------------------------- failover-check

// FailoverCheck answers "if the primary died right now, could we promote a replica?".
// It flags split-brain (more than one writable node), missing/detached replicas and
// replicas whose replay position is further than maxLag bytes behind the primary.
func (f *Fleet) FailoverCheck(ctx context.Context, maxLag uint64) *Report {
	start := time.Now()
	type state struct {
		node       Node
		up         bool
		inRecovery bool
		check      Check
	}
	states := pool.Map(ctx, f.Workers, f.Cfg.Nodes, func(ctx context.Context, _ int, n Node) state {
		s := state{node: n}
		s.check = f.check(n.Name, "health", func() (string, error) {
			if _, err := f.R.Exec(ctx, n.Name, "pg_isready", "-U", "postgres"); err != nil {
				return "", fmt.Errorf("down: %w", err)
			}
			s.up = true
			out, err := f.sql(ctx, n.Name, "SELECT pg_is_in_recovery()")
			s.inRecovery = out == "t"
			return map[bool]string{true: "replica", false: "writable"}[s.inRecovery], err
		})
		return s
	})
	var checks []Check
	var writable []state
	var replicas []state
	for _, s := range states {
		checks = append(checks, s.check)
		if !s.up || !s.check.OK {
			continue
		}
		if s.inRecovery {
			replicas = append(replicas, s)
		} else {
			writable = append(writable, s)
		}
	}
	// split-brain / primary presence
	checks = append(checks, f.check("-", "single_primary", func() (string, error) {
		switch len(writable) {
		case 1:
			return writable[0].node.Name, nil
		case 0:
			return "", fmt.Errorf("no writable primary found")
		}
		names := make([]string, len(writable))
		for i, w := range writable {
			names[i] = w.node.Name
		}
		return "", fmt.Errorf("SPLIT-BRAIN: %d writable nodes: %s", len(writable), strings.Join(names, ", "))
	}))
	rep := &Report{Command: "failover-check"}
	if len(writable) != 1 {
		return f.finishFailover(rep, start, checks)
	}
	primary := writable[0].node
	var primaryLSN pg.LSN
	checks = append(checks, f.check(primary.Name, "primary_lsn", func() (string, error) {
		out, err := f.sql(ctx, primary.Name, "SELECT pg_current_wal_lsn()")
		if err != nil {
			return "", err
		}
		primaryLSN, err = pg.ParseLSN(out)
		return out, err
	}))
	checks = append(checks, f.check(primary.Name, "streaming_replicas", func() (string, error) {
		out, err := f.sql(ctx, primary.Name, "SELECT count(*) FROM pg_stat_replication")
		if err != nil {
			return "", err
		}
		got, _ := strconv.Atoi(out)
		if want := len(f.Cfg.Replicas()); got < want {
			return "", fmt.Errorf("%d of %d replicas streaming", got, want)
		}
		return out + " streaming", nil
	}))
	if primaryLSN == 0 {
		return f.finishFailover(rep, start, checks)
	}
	type lagRes struct {
		check Check
		lag   uint64
	}
	lags := pool.Map(ctx, f.Workers, replicas, func(ctx context.Context, _ int, s state) lagRes {
		var lr lagRes
		lr.check = f.check(s.node.Name, "replay_lag", func() (string, error) {
			out, err := f.sql(ctx, s.node.Name, "SELECT pg_last_wal_replay_lsn()")
			if err != nil {
				return "", err
			}
			l, err := pg.ParseLSN(out)
			if err != nil {
				return "", err
			}
			lr.lag = pg.Lag(primaryLSN, l)
			if lr.lag > maxLag {
				return "", fmt.Errorf("%d bytes behind (max %d)", lr.lag, maxLag)
			}
			return fmt.Sprintf("%d bytes behind", lr.lag), nil
		})
		return lr
	})
	best, bestLag := "", ^uint64(0)
	for i, lr := range lags {
		checks = append(checks, lr.check)
		if lr.check.OK && lr.lag < bestLag {
			best, bestLag = replicas[i].node.Name, lr.lag
		}
	}
	checks = append(checks, f.check("-", "promotion_candidate", func() (string, error) {
		if best == "" {
			return "", fmt.Errorf("no healthy replica within %d bytes of the primary", maxLag)
		}
		return best, nil
	}))
	rep.Candidate = best
	return f.finishFailover(rep, start, checks)
}

func (f *Fleet) finishFailover(rep *Report, start time.Time, checks []Check) *Report {
	out := f.finish("failover-check", start, checks)
	out.Candidate = rep.Candidate
	return out
}
