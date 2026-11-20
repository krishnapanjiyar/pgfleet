package fleet

import (
	"context"
	"io"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/krishnapanjiyar/pgfleet/internal/runner"
)

func quiet() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func newFleet(t *testing.T, nodes int, latency time.Duration) (*Fleet, *runner.MockRunner) {
	t.Helper()
	cfg := SyntheticConfig(nodes)
	m := NewMockFromConfig(cfg, latency)
	f := New(cfg, m, 8, quiet())
	f.PollInterval = time.Millisecond
	f.Now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	return f, m
}

func failed(r *Report) []string {
	var out []string
	for _, c := range r.Checks {
		if !c.OK {
			out = append(out, c.Node+"/"+c.Name)
		}
	}
	sort.Strings(out)
	return out
}

func TestConfigValidation(t *testing.T) {
	good := func() *Config {
		return &Config{Nodes: []Node{{"a", "primary", 1}, {"b", "replica", 2}}}
	}
	if err := good().Validate(); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*Config){
		"two primaries": func(c *Config) { c.Nodes[1].Role = "primary" },
		"no primary":    func(c *Config) { c.Nodes[0].Role = "replica" },
		"dup name":      func(c *Config) { c.Nodes[1].Name = "a" },
		"dup port":      func(c *Config) { c.Nodes[1].Port = 1 },
		"bad role":      func(c *Config) { c.Nodes[1].Role = "witness" },
		"bad port":      func(c *Config) { c.Nodes[1].Port = 0 },
		"empty":         func(c *Config) { c.Nodes = nil },
	}
	for name, mut := range cases {
		c := good()
		mut(c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected validation error", name)
		}
	}
}

func TestProvisionThenVerify(t *testing.T) {
	cfg := SyntheticConfig(4)
	m := runner.NewMock(0) // empty: provision must create the containers
	f := New(cfg, m, 4, quiet())
	f.PollInterval = time.Millisecond
	if r := f.Provision(context.Background()); !r.OK {
		t.Fatalf("provision failed: %v", failed(r))
	}
	if r := f.Verify(context.Background(), true); !r.OK {
		t.Fatalf("verify failed: %v", failed(r))
	}
	// idempotency guard: a second provision must fail loudly, not silently re-create.
	if r := f.Provision(context.Background()); r.OK {
		t.Fatal("second provision should report the name conflict")
	}
}

func TestVerifyDetectsProblems(t *testing.T) {
	f, m := newFleet(t, 4, 0)
	m.Crash("pg-02")
	m.Promote("pg-03") // replica configured as replica, but writable
	m.CorruptWAL("pg-01")
	r := f.Verify(context.Background(), true)
	got := strings.Join(failed(r), ",")
	want := "pg-01/wal_integrity,pg-02/ready,pg-03/role"
	if got != want {
		t.Fatalf("failed checks = %s, want %s", got, want)
	}
	if r.OK {
		t.Fatal("report must be not-OK")
	}
	// ready failure short-circuits dependent checks on that node
	for _, c := range r.Checks {
		if c.Node == "pg-02" && c.Name != "ready" {
			t.Fatalf("unexpected check %s on down node", c.Name)
		}
	}
}

func TestBackupRestoreRoundTrip(t *testing.T) {
	f, m := newFleet(t, 3, 0)
	b := f.Backup(context.Background())
	if !b.OK || b.Snapshot != "snap-20260102T030405Z" {
		t.Fatalf("backup: ok=%v snap=%q failed=%v", b.OK, b.Snapshot, failed(b))
	}
	r := f.Restore(context.Background(), b.Snapshot, false)
	if !r.OK {
		t.Fatalf("restore failed: %v", failed(r))
	}
	// scratch container must have been cleaned up
	if _, err := m.Exec(context.Background(), "pgfleet-restore-"+b.Snapshot, "pg_isready"); err == nil {
		t.Fatal("restore container was not removed")
	}
}

func TestRestoreRejectsCorruptAndUnknownSnapshots(t *testing.T) {
	f, m := newFleet(t, 3, 0)
	f.ReadyTimeout = 20 * time.Millisecond
	b := f.Backup(context.Background())
	m.CorruptBackup("/backups/" + b.Snapshot)
	r := f.Restore(context.Background(), b.Snapshot, false)
	if r.OK || strings.Join(failed(r), ",") != "pgfleet-restore-"+b.Snapshot+"/ready" {
		t.Fatalf("corrupt snapshot must fail at ready, failed=%v", failed(r))
	}
	if r := f.Restore(context.Background(), "snap-does-not-exist", false); r.OK {
		t.Fatal("unknown snapshot must fail")
	}
	if r := f.Restore(context.Background(), "../etc", false); r.OK {
		t.Fatal("path-traversal snapshot name must be rejected")
	}
}

func TestBackupFailsWhenPrimaryDown(t *testing.T) {
	f, m := newFleet(t, 3, 0)
	m.Crash("pg-00")
	r := f.Backup(context.Background())
	if r.OK || r.Snapshot != "" {
		t.Fatal("backup against a dead primary must fail without a snapshot")
	}
}

func TestFailoverCheckHealthy(t *testing.T) {
	f, m := newFleet(t, 4, 0)
	m.SetLag("pg-01", 4096)
	m.SetLag("pg-02", 100)
	m.SetLag("pg-03", 9000)
	r := f.FailoverCheck(context.Background(), 1<<20)
	if !r.OK || r.Candidate != "pg-02" {
		t.Fatalf("ok=%v candidate=%q failed=%v", r.OK, r.Candidate, failed(r))
	}
}

func TestFailoverCheckLagAndDetached(t *testing.T) {
	f, m := newFleet(t, 4, 0)
	m.SetLag("pg-01", 5<<20)
	m.Detach("pg-03")
	r := f.FailoverCheck(context.Background(), 1<<20)
	got := strings.Join(failed(r), ",")
	if got != "pg-00/streaming_replicas,pg-01/replay_lag" {
		t.Fatalf("failed = %s", got)
	}
	if r.Candidate != "pg-02" {
		t.Fatalf("candidate = %q, want pg-02", r.Candidate)
	}
}

func TestFailoverCheckSplitBrain(t *testing.T) {
	f, m := newFleet(t, 3, 0)
	m.Promote("pg-02")
	r := f.FailoverCheck(context.Background(), 1<<20)
	if r.OK {
		t.Fatal("split-brain must fail")
	}
	for _, c := range r.Checks {
		if c.Name == "single_primary" {
			if !strings.Contains(c.Detail, "SPLIT-BRAIN") {
				t.Fatalf("detail = %q", c.Detail)
			}
			return
		}
	}
	t.Fatal("no single_primary check emitted")
}

func TestFailoverCheckNoPrimary(t *testing.T) {
	f, m := newFleet(t, 3, 0)
	m.Crash("pg-00")
	r := f.FailoverCheck(context.Background(), 1<<20)
	if r.OK || r.Candidate != "" {
		t.Fatal("no primary: must fail with no candidate")
	}
}

// Mock-cluster integration test behind the "sub-50ms command latency" claim:
// 16 nodes, 1ms simulated cost per docker exec, 8 workers.
func TestCommandLatencyUnder50msOnMockCluster(t *testing.T) {
	f, _ := newFleet(t, 16, time.Millisecond)
	ctx := context.Background()
	cmds := map[string]func() *Report{
		"verify":         func() *Report { return f.Verify(ctx, false) },
		"failover-check": func() *Report { return f.FailoverCheck(ctx, 1<<20) },
		"backup":         func() *Report { return f.Backup(ctx) },
	}
	for name, run := range cmds {
		var worst float64
		for i := 0; i < 5; i++ {
			r := run()
			if !r.OK {
				t.Fatalf("%s failed: %v", name, failed(r))
			}
			if r.ElapsedMS > worst {
				worst = r.ElapsedMS
			}
		}
		t.Logf("%-15s worst of 5 runs: %.1f ms", name, worst)
		if worst >= 50 {
			t.Errorf("%s took %.1f ms, want < 50 ms", name, worst)
		}
	}
}
