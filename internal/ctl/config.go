// Package ctl implements the ppp control plane (ppp-ctl-server): the Control
// gRPC service, durable state (PostgreSQL, phase 7), and the topology /
// banned-list / job push fanout. Multiple instances share the same PG; a
// PG lease elects exactly one leader that serves mutations and watch streams
// (LB routes to it via the /leader health endpoint).
package ctl

import (
	"flag"
	"time"
)

// Config holds ppp-ctl-server runtime settings.
type Config struct {
	// Addr is the gRPC listen address, e.g. ":9090".
	Addr string
	// HTTPAddr is the /leader health-check listen address, e.g. ":9091".
	HTTPAddr string
	// PGDSN is the PostgreSQL connection string (the ctl's primary store).
	PGDSN string
	// InstanceID identifies this control-plane instance in leader election.
	InstanceID string
	// LeaderLease is how long a leader lease lasts before it expires (other
	// instances may take over after expiry).
	LeaderLease time.Duration
	// LeaderRenew is how often the leader renews its lease.
	LeaderRenew time.Duration
	// TLS options for mTLS (all empty = plaintext, development compatibility).
	TLSCA   string
	TLSCert string
	TLSKey  string
	// TLSClientCert/TLSClientKey/TLSClientCA are the client-side identity the
	// ctl uses when it dials peers (unused for now; the ctl only serves).
	TLSClientCA   string
	TLSClientCert string
	TLSClientKey  string
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
		HTTPAddr:             ":9091",
		PGDSN:                "postgres://ppp:ppp@127.0.0.1:25433/ppp",
		InstanceID:           "ctl-1",
		LeaderLease:          10 * time.Second,
		LeaderRenew:          2 * time.Second,
		HeartbeatTimeout:     30 * time.Second,
		DefaultGroupMembers:  16,
		DefaultGroupChildren: 8,
	}
}

// RegisterFlags binds the configuration to command-line flags.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Addr, "addr", c.Addr, "gRPC listen address")
	fs.StringVar(&c.HTTPAddr, "http-addr", c.HTTPAddr, "/leader health-check listen address")
	fs.StringVar(&c.PGDSN, "pg-dsn", c.PGDSN, "PostgreSQL DSN (the ctl's primary store)")
	fs.StringVar(&c.InstanceID, "instance-id", c.InstanceID, "control-plane instance id (leader election)")
	fs.DurationVar(&c.LeaderLease, "leader-lease", c.LeaderLease, "leader lease duration")
	fs.DurationVar(&c.LeaderRenew, "leader-renew", c.LeaderRenew, "leader lease renewal interval")
	fs.StringVar(&c.TLSCA, "tls-ca", c.TLSCA, "CA cert file for mTLS (empty = plaintext)")
	fs.StringVar(&c.TLSCert, "tls-cert", c.TLSCert, "server cert file for mTLS")
	fs.StringVar(&c.TLSKey, "tls-key", c.TLSKey, "server key file for mTLS")
	fs.DurationVar(&c.HeartbeatTimeout, "heartbeat-timeout", c.HeartbeatTimeout, "node heartbeat timeout before pruning")
	fs.IntVar(&c.DefaultGroupMembers, "group-members", c.DefaultGroupMembers, "default members per group for new trees")
	fs.IntVar(&c.DefaultGroupChildren, "group-children", c.DefaultGroupChildren, "default child groups per group for new trees")
}
