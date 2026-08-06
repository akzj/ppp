// Package ctl implements the ppp control plane (ppp-ctl-server): the Control
// gRPC service, durable state, and the topology/banned-list/job push fanout.
//
// The store is behind an interface so a future HA deployment (raft/etcd) can
// replace the bbolt implementation without touching the RPC layer. This build
// is single-instance by design.
package ctl

import (
	"flag"
	"time"
)

// Config holds ppp-ctl-server runtime settings.
type Config struct {
	// Addr is the gRPC listen address, e.g. ":9090".
	Addr string
	// DBPath is the bbolt database file. In-memory operation is not supported;
	// tests use a temp file.
	DBPath string
	// HeartbeatTimeout is how long a node may stay silent before it is pruned
	// and its topology entry removed.
	HeartbeatTimeout time.Duration
	// DefaultGroupMembers and DefaultGroupChildren are applied to trees created
	// without explicit group sizes (Tree.GroupMembers/GroupChildren <= 0).
	DefaultGroupMembers  int
	DefaultGroupChildren int
}

// DefaultConfig returns the default runtime configuration.
func DefaultConfig() *Config {
	return &Config{
		Addr:                 ":9090",
		DBPath:               "ppp-ctl.db",
		HeartbeatTimeout:     30 * time.Second,
		DefaultGroupMembers:  16,
		DefaultGroupChildren: 8,
	}
}

// RegisterFlags binds the configuration to command-line flags.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Addr, "addr", c.Addr, "gRPC listen address")
	fs.StringVar(&c.DBPath, "db", c.DBPath, "bbolt database path")
	fs.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", c.HeartbeatTimeout, "node heartbeat timeout before pruning")
	fs.IntVar(&c.DefaultGroupMembers, "group-members", c.DefaultGroupMembers, "default members per group for new trees")
	fs.IntVar(&c.DefaultGroupChildren, "group-children", c.DefaultGroupChildren, "default child groups per group for new trees")
}
