package runner

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type mockNode struct {
	running    bool
	primary    bool
	lag        uint64 // replicas: bytes behind the primary
	detached   bool   // replica not streaming from the primary
	walCorrupt bool
}

// MockRunner is an in-memory PostgreSQL cluster that understands exactly the
// commands pgfleet issues. It lets the whole CLI be tested without Docker.
type MockRunner struct {
	Latency time.Duration // simulated cost of each command

	mu         sync.Mutex
	nodes      map[string]*mockNode
	backups    map[string]bool // path -> valid
	primaryLSN uint64
	calls      atomic.Int64
}

func NewMock(latency time.Duration) *MockRunner {
	return &MockRunner{
		Latency:    latency,
		nodes:      map[string]*mockNode{},
		backups:    map[string]bool{},
		primaryLSN: 0x3000060,
	}
}

// ---- test/demo controls ----------------------------------------------------

func (m *MockRunner) AddNode(name string, primary bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[name] = &mockNode{running: true, primary: primary}
}
func (m *MockRunner) Calls() int64 { return m.calls.Load() }
func (m *MockRunner) with(name string, f func(*mockNode)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n, ok := m.nodes[name]; ok {
		f(n)
	}
}
func (m *MockRunner) Crash(name string)            { m.with(name, func(n *mockNode) { n.running = false }) }
func (m *MockRunner) Promote(name string)          { m.with(name, func(n *mockNode) { n.primary = true }) }
func (m *MockRunner) SetLag(name string, b uint64) { m.with(name, func(n *mockNode) { n.lag = b }) }
func (m *MockRunner) Detach(name string)           { m.with(name, func(n *mockNode) { n.detached = true }) }
func (m *MockRunner) CorruptWAL(name string)       { m.with(name, func(n *mockNode) { n.walCorrupt = true }) }
func (m *MockRunner) CorruptBackup(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.backups[path] = false
}
func (m *MockRunner) AdvanceWAL(b uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.primaryLSN += b
}

// ---- Runner implementation -------------------------------------------------

func (m *MockRunner) sleep(ctx context.Context) error {
	m.calls.Add(1)
	if m.Latency <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(m.Latency)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fail(cmd string, code int, msg string) (Result, error) {
	r := Result{ExitCode: code, Stderr: msg}
	return r, &ExitError{Cmd: cmd, Result: r}
}

func flagVal(args []string, flag string) string {
	for i := 0; i < len(args)-1; i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func (m *MockRunner) Docker(ctx context.Context, args ...string) (Result, error) {
	if err := m.sleep(ctx); err != nil {
		return Result{}, err
	}
	if len(args) == 0 {
		return fail("docker", 1, "no command")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	switch args[0] {
	case "network", "volume", "pull":
		return Result{}, nil
	case "run":
		name := flagVal(args, "--name")
		if name == "" {
			return fail("docker run", 125, "mock: --name required")
		}
		if _, ok := m.nodes[name]; ok {
			return fail("docker run", 125, "Conflict. The container name is already in use")
		}
		role := ""
		for i := 0; i < len(args)-1; i++ {
			if args[i] == "--label" && strings.HasPrefix(args[i+1], "pgfleet.role=") {
				role = strings.TrimPrefix(args[i+1], "pgfleet.role=")
			}
		}
		running := true
		if strings.HasPrefix(name, "pgfleet-restore-") {
			role = "primary" // a restored base backup comes up as a standalone server...
			snap := strings.TrimPrefix(name, "pgfleet-restore-")
			running = m.backups["/backups/"+snap] // ...unless the backup is missing or corrupt
		}
		m.nodes[name] = &mockNode{running: running, primary: role == "primary"}
		return Result{Stdout: "mock-container-id\n"}, nil
	case "rm":
		for _, a := range args[1:] {
			delete(m.nodes, a)
		}
		return Result{}, nil
	case "stop":
		if n, ok := m.nodes[args[len(args)-1]]; ok {
			n.running = false
		}
		return Result{}, nil
	}
	return fail("docker "+args[0], 1, "mock: unsupported docker subcommand")
}

func (m *MockRunner) Exec(ctx context.Context, container string, argv ...string) (Result, error) {
	if err := m.sleep(ctx); err != nil {
		return Result{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	cmd := "docker exec " + container + " " + argv[0]
	n, ok := m.nodes[container]
	if !ok {
		return fail(cmd, 1, "Error: No such container: "+container)
	}
	if !n.running {
		return fail(cmd, 126, "Error response from daemon: container is not running")
	}
	switch argv[0] {
	case "pg_isready":
		return Result{Stdout: "/var/run/postgresql:5432 - accepting connections\n"}, nil
	case "psql":
		return m.psql(n, cmd, flagVal(argv, "-c"))
	case "pg_basebackup":
		if !n.primary {
			return fail(cmd, 1, "pg_basebackup: error: must run against the primary in this mock")
		}
		m.backups[flagVal(argv, "-D")] = true
		return Result{}, nil
	case "pg_verifybackup":
		path := argv[len(argv)-1]
		valid, exists := m.backups[path]
		if !exists {
			return fail(cmd, 1, "pg_verifybackup: error: could not open backup manifest")
		}
		if !valid {
			return fail(cmd, 1, "pg_verifybackup: error: checksum mismatch for file \"base/1/1259\"")
		}
		return Result{Stdout: "backup successfully verified\n"}, nil
	case "bash":
		script := strings.Join(argv, " ")
		if strings.Contains(script, "pg_controldata") {
			return Result{Stdout: fmt.Sprintf("Latest checkpoint's REDO location:    %s\n", lsnString(m.primaryLSN-0x38))}, nil
		}
		if strings.Contains(script, "pg_waldump") && n.walCorrupt {
			return fail(cmd, 1, "pg_waldump: error: error in WAL record at 0/3000060: incorrect resource manager data checksum")
		}
		return Result{}, nil
	}
	return Result{}, nil
}

func (m *MockRunner) psql(n *mockNode, cmd, q string) (Result, error) {
	q = strings.TrimSpace(q)
	switch {
	case q == "SELECT 1":
		return Result{Stdout: "1\n"}, nil
	case q == "SHOW wal_level":
		return Result{Stdout: "replica\n"}, nil
	case q == "SELECT pg_is_in_recovery()":
		if n.primary {
			return Result{Stdout: "f\n"}, nil
		}
		return Result{Stdout: "t\n"}, nil
	case q == "SELECT pg_current_wal_flush_lsn()" || q == "SELECT pg_current_wal_lsn()":
		if !n.primary {
			return fail(cmd, 1, "ERROR:  recovery is in progress")
		}
		return Result{Stdout: lsnString(m.primaryLSN) + "\n"}, nil
	case q == "SELECT pg_last_wal_receive_lsn()" || q == "SELECT pg_last_wal_replay_lsn()":
		if n.primary {
			return Result{Stdout: "\n"}, nil
		}
		return Result{Stdout: lsnString(m.primaryLSN-min64(m.primaryLSN, n.lag)) + "\n"}, nil
	case q == "SELECT count(*) FROM pg_stat_replication":
		c := 0
		for _, o := range m.nodes {
			if !o.primary && o.running && !o.detached {
				c++
			}
		}
		return Result{Stdout: fmt.Sprintf("%d\n", c)}, nil
	case strings.HasPrefix(q, "CREATE "), strings.HasPrefix(q, "ALTER "), q == "SELECT pg_reload_conf()":
		return Result{}, nil
	}
	return fail(cmd, 1, "mock: unsupported SQL: "+q)
}

func lsnString(v uint64) string { return fmt.Sprintf("%X/%X", v>>32, v&0xFFFFFFFF) }
func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
