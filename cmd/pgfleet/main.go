// Command pgfleet provisions, verifies, backs up, restores and failover-checks
// a fleet of containerised PostgreSQL instances.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/krishnapanjiyar/pgfleet/internal/fleet"
	"github.com/krishnapanjiyar/pgfleet/internal/runner"
)

var version = "0.1.0"

const usage = `pgfleet - PostgreSQL fleet management

Usage: pgfleet <command> [flags]

Commands:
  provision        start the primary, clone replicas from it
  verify           readiness, query, wal_level, role (and --wal integrity) on every node
  backup           physical base backup on the primary + pg_verifybackup
  restore          boot a scratch server from a snapshot and sanity-check it
  failover-check   split-brain, replica lag and promotion-candidate report
  bench            measure command latency against a synthetic mock cluster
  version

Common flags: -config fleet.json  -driver docker|mock  -workers N  -json  -log-level info
Exit status: 0 all checks passed, 1 a check failed, 2 usage or config error.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "version", "-v", "--version":
		fmt.Println("pgfleet", version)
		return
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	}
	os.Exit(run(cmd, args))
}

func run(cmd string, args []string) int {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	configPath := fs.String("config", "fleet.json", "fleet config file")
	driver := fs.String("driver", "docker", "docker (real) or mock (in-memory cluster)")
	workers := fs.Int("workers", 8, "parallel workers")
	asJSON := fs.Bool("json", false, "print the report as JSON on stdout")
	logLevel := fs.String("log-level", "info", "debug|info|warn|error (JSON logs go to stderr)")
	timeout := fs.Duration("timeout", 2*time.Minute, "overall command timeout")
	wal := fs.Bool("wal", false, "verify: also decode WAL since the last checkpoint with pg_waldump")
	snapshot := fs.String("snapshot", "", "restore: snapshot name printed by backup")
	keep := fs.Bool("keep", false, "restore: keep the scratch container")
	maxLag := fs.Uint64("max-lag-bytes", 16<<20, "failover-check: max acceptable replay lag")
	nodes := fs.Int("nodes", 32, "bench: cluster size")
	iters := fs.Int("iterations", 20, "bench: iterations per command")
	mockLat := fs.Duration("mock-latency", time.Millisecond, "mock driver: simulated cost per command")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintln(os.Stderr, "bad -log-level:", err)
		return 2
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	if cmd == "bench" {
		return bench(ctx, *nodes, *iters, *workers, *mockLat)
	}

	cfg, err := fleet.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		return 2
	}
	var r runner.Runner
	switch *driver {
	case "docker":
		r = runner.DockerRunner{}
	case "mock":
		r = fleet.NewMockFromConfig(cfg, *mockLat)
	default:
		fmt.Fprintln(os.Stderr, "unknown -driver", *driver)
		return 2
	}
	f := fleet.New(cfg, r, *workers, log)

	var rep *fleet.Report
	switch cmd {
	case "provision":
		rep = f.Provision(ctx)
	case "verify":
		rep = f.Verify(ctx, *wal)
	case "backup":
		rep = f.Backup(ctx)
	case "restore":
		if *snapshot == "" {
			fmt.Fprintln(os.Stderr, "restore: -snapshot is required")
			return 2
		}
		rep = f.Restore(ctx, *snapshot, *keep)
	case "failover-check":
		rep = f.FailoverCheck(ctx, *maxLag)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", cmd, usage)
		return 2
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		printTable(rep)
	}
	if !rep.OK {
		return 1
	}
	return 0
}

func printTable(r *fleet.Report) {
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "NODE\tCHECK\tSTATUS\tLATENCY\tDETAIL")
	for _, c := range r.Checks {
		st := "ok"
		if !c.OK {
			st = "FAIL"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%.1fms\t%s\n", c.Node, c.Name, st, c.LatencyMS, c.Detail)
	}
	tw.Flush()
	verdict := "PASS"
	if !r.OK {
		verdict = "FAIL"
	}
	fmt.Printf("\n%s %s in %.1fms", verdict, r.Command, r.ElapsedMS)
	if r.Snapshot != "" {
		fmt.Printf("  snapshot=%s", r.Snapshot)
	}
	if r.Candidate != "" {
		fmt.Printf("  promote-candidate=%s", r.Candidate)
	}
	fmt.Println()
}

func bench(ctx context.Context, n, iters, workers int, lat time.Duration) int {
	cfg := fleet.SyntheticConfig(n)
	m := fleet.NewMockFromConfig(cfg, lat)
	f := fleet.New(cfg, m, workers, slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	f.Now = time.Now
	runs := map[string]func() *fleet.Report{
		"verify":         func() *fleet.Report { return f.Verify(ctx, false) },
		"verify --wal":   func() *fleet.Report { return f.Verify(ctx, true) },
		"backup":         func() *fleet.Report { return f.Backup(ctx) },
		"failover-check": func() *fleet.Report { return f.FailoverCheck(ctx, 1<<20) },
	}
	names := make([]string, 0, len(runs))
	for k := range runs {
		names = append(names, k)
	}
	sort.Strings(names)
	fmt.Printf("mock cluster: %d nodes, %d workers, %s simulated latency per command, %d iterations\n\n", n, workers, lat, iters)
	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "COMMAND\tp50\tp95\tmax\tOK")
	status := 0
	for _, name := range names {
		ms := make([]float64, 0, iters)
		ok := true
		for i := 0; i < iters; i++ {
			if name == "backup" {
				f.Now = func() time.Time { return time.Now().Add(time.Duration(i) * time.Second) }
			}
			r := runs[name]()
			ok = ok && r.OK
			ms = append(ms, r.ElapsedMS)
		}
		sort.Float64s(ms)
		fmt.Fprintf(tw, "%s\t%.1fms\t%.1fms\t%.1fms\t%v\n", name, ms[len(ms)/2], ms[len(ms)*95/100], ms[len(ms)-1], ok)
		if !ok {
			status = 1
		}
	}
	tw.Flush()
	return status
}
