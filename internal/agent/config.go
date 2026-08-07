// Package agent implements the ppp edge data node (ppp-service): the ctl
// client (register/heartbeat/topology/banned/jobs), the Data gRPC service
// (GetPiece/DownloadFile/Subscribe/Unsubscribe), the local piece store, the
// per-file downloader and the local banned gate.
package agent

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// PieceSize is the size of one piece in bytes. Every agent in a tree uses the
// same value so piece indices and offsets are interchangeable.
const PieceSize int64 = 4 << 20 // 4 MiB

// maxGRPCMessageSize is the gRPC message limit for the Data plane. A full
// piece plus protobuf framing exceeds gRPC's 4 MiB default, so both the Data
// server and the peer client must raise it (headroom for future larger pieces).
const maxGRPCMessageSize = 16 << 20 // 16 MiB

// Config holds ppp-service runtime settings.
type Config struct {
	// ID is the node id registered with the control plane. Empty means the
	// hostname is used.
	ID string
	// Addr is the Data gRPC listen address; it is also the address advertised
	// to peers (use a reachable address in production).
	Addr string
	// CtlAddr is the control plane gRPC address.
	CtlAddr string
	// Tree is the tree id this node belongs to.
	Tree string
	// Role is this node's role in the tree.
	Role pppv1.Node_Role
	// DownloadPath is the directory for the local piece store.
	DownloadPath string
	// Store is a deprecated compatibility flag: "file" and "mmap" both select
	// the single unified sparse-file store. Kept for command-line compat.
	Store string
	// HeartbeatInterval is how often the agent heartbeats to the ctl.
	HeartbeatInterval time.Duration
	// DownloadConcurrency bounds concurrent piece fetches per file.
	DownloadConcurrency int
	// LeaseTTL is the default session-lease duration.
	LeaseTTL time.Duration

	role string // parsed from flags, converted by Finalize
}

// DefaultConfig returns the default runtime configuration.
func DefaultConfig() *Config {
	return &Config{
		Addr:                ":0",
		DownloadPath:        "./ppp-data",
		Store:               "mmap",
		HeartbeatInterval:   5 * time.Second,
		DownloadConcurrency: 4,
		LeaseTTL:            30 * time.Second,
	}
}

// RegisterFlags binds the configuration to command-line flags.
func (c *Config) RegisterFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.ID, "id", c.ID, "node id (default: hostname)")
	fs.StringVar(&c.Addr, "addr", c.Addr, "Data gRPC listen/advertised address")
	fs.StringVar(&c.CtlAddr, "ctl-addr", c.CtlAddr, "control plane gRPC address")
	fs.StringVar(&c.Tree, "tree", c.Tree, "tree id this node belongs to")
	fs.StringVar(&c.role, "role", "member", "node role: root|member")
	fs.StringVar(&c.DownloadPath, "download-path", c.DownloadPath, "directory for piece storage")
	fs.StringVar(&c.Store, "store", c.Store, "piece store: mmap (default) or file")
	fs.DurationVar(&c.HeartbeatInterval, "heartbeat-interval", c.HeartbeatInterval, "heartbeat interval")
	fs.IntVar(&c.DownloadConcurrency, "download-concurrency", c.DownloadConcurrency, "max concurrent piece fetches per file")
	fs.DurationVar(&c.LeaseTTL, "lease-ttl", c.LeaseTTL, "session lease TTL")
}

// Finalize converts parsed flag values into typed fields and validates the
// configuration. Call after flag.Parse.
func (c *Config) Finalize() error {
	switch c.role {
	case "root":
		c.Role = pppv1.Node_ROOT
	case "member":
		c.Role = pppv1.Node_MEMBER
	default:
		return fmt.Errorf("agent: unknown role %q (want root|member)", c.role)
	}
	if c.ID == "" {
		host, err := os.Hostname()
		if err != nil {
			return fmt.Errorf("agent: resolve node id: %w", err)
		}
		c.ID = host
	}
	if c.Tree == "" {
		return errors.New("agent: -tree is required")
	}
	if c.CtlAddr == "" {
		return errors.New("agent: -ctl-addr is required")
	}
	return nil
}
