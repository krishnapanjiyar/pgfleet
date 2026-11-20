package fleet

import (
	"fmt"
	"time"

	"github.com/krishnapanjiyar/pgfleet/internal/runner"
)

// NewMockFromConfig returns a mock runner pre-populated with the cluster the
// config describes (all nodes up, roles as configured, no replication lag).
func NewMockFromConfig(cfg *Config, latency time.Duration) *runner.MockRunner {
	m := runner.NewMock(latency)
	for _, n := range cfg.Nodes {
		m.AddNode(n.Name, n.Role == "primary")
	}
	return m
}

// SyntheticConfig builds an n-node cluster config (1 primary + n-1 replicas).
func SyntheticConfig(n int) *Config {
	c := &Config{Cluster: "bench", BackupDir: "/tmp/pgfleet-bench"}
	for i := 0; i < n; i++ {
		node := Node{Name: fmt.Sprintf("pg-%02d", i), Role: "replica", Port: 15432 + i}
		if i == 0 {
			node.Role = "primary"
		}
		c.Nodes = append(c.Nodes, node)
	}
	if err := c.Validate(); err != nil {
		panic(err)
	}
	return c
}
