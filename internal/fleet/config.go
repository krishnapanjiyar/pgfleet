// Package fleet implements the provision / verify / backup / restore /
// failover-check operations over a Runner.
package fleet

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// Node is one PostgreSQL container in the fleet.
type Node struct {
	Name string `json:"name"`
	Role string `json:"role"` // "primary" or "replica"
	Port int    `json:"port"` // host port mapped to 5432
}

// Config describes a cluster.
type Config struct {
	Cluster     string `json:"cluster"`
	Image       string `json:"image"`
	BackupDir   string `json:"backup_dir"`
	PasswordEnv string `json:"password_env"`
	Nodes       []Node `json:"nodes"`
}

// Load reads and validates a fleet config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

// Validate applies defaults and checks invariants (exactly one primary, unique names/ports).
func (c *Config) Validate() error {
	if c.Cluster == "" {
		c.Cluster = "pgfleet"
	}
	if c.Image == "" {
		c.Image = "postgres:16"
	}
	if c.BackupDir == "" {
		c.BackupDir = "./backups"
	}
	if abs, err := filepath.Abs(c.BackupDir); err == nil {
		c.BackupDir = abs
	}
	if c.PasswordEnv == "" {
		c.PasswordEnv = "PGFLEET_PASSWORD"
	}
	if len(c.Nodes) == 0 {
		return fmt.Errorf("no nodes configured")
	}
	names, ports, primaries := map[string]bool{}, map[int]bool{}, 0
	for _, n := range c.Nodes {
		switch {
		case n.Name == "":
			return fmt.Errorf("node with empty name")
		case names[n.Name]:
			return fmt.Errorf("duplicate node name %q", n.Name)
		case n.Port <= 0 || n.Port > 65535:
			return fmt.Errorf("node %q: invalid port %d", n.Name, n.Port)
		case ports[n.Port]:
			return fmt.Errorf("node %q: port %d already used", n.Name, n.Port)
		}
		switch n.Role {
		case "primary":
			primaries++
		case "replica":
		default:
			return fmt.Errorf("node %q: role must be primary or replica, got %q", n.Name, n.Role)
		}
		names[n.Name], ports[n.Port] = true, true
	}
	if primaries != 1 {
		return fmt.Errorf("exactly one primary required, found %d", primaries)
	}
	return nil
}

func (c *Config) Primary() Node {
	for _, n := range c.Nodes {
		if n.Role == "primary" {
			return n
		}
	}
	return Node{}
}

func (c *Config) Replicas() []Node {
	var out []Node
	for _, n := range c.Nodes {
		if n.Role == "replica" {
			out = append(out, n)
		}
	}
	return out
}

func (c *Config) Network() string { return "pgfleet-" + c.Cluster }
