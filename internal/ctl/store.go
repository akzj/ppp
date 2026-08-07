package ctl

import (
	"errors"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// Sentinel errors returned by Store implementations.
var (
	// ErrNotFound is returned when the requested record does not exist.
	ErrNotFound = errors.New("ctl: not found")
	// ErrExists is returned when a create collides with an existing record.
	ErrExists = errors.New("ctl: already exists")
)

// Store is the durable state behind the control plane. The PostgreSQL
// implementation (pgstore.go) is the ctl's primary store; the interface is the
// swap point so a future deployment can replace the backend without touching
// the RPC layer.
type Store interface {
	// Trees.
	CreateTree(t *pppv1.Tree) error
	GetTree(id string) (*pppv1.Tree, error)
	ListTrees() ([]*pppv1.Tree, error)
	DeleteTree(id string) error
	// DeleteTreeData cascade-removes every record of a tree (nodes, jobs,
	// banned, progress) without touching the tree record itself.
	DeleteTreeData(treeID string) error

	// Nodes are persisted so restarts keep the registered node set.
	// ListNodes returns the nodes of one tree, or all nodes when treeID is
	// empty (used to reload the registry at startup).
	PutNode(n *pppv1.Node) error
	DeleteNode(treeID, nodeID string) error
	ListNodes(treeID string) ([]*pppv1.Node, error)

	// Per-tree topology generation (incremented whenever the node set of a
	// tree changes and its topology is recomputed).
	TopologyGeneration(treeID string) (int64, error)
	BumpTopologyGeneration(treeID string) (int64, error)

	// Banned list: the durable cancellation state. Add/Remove return the new
	// generation so the caller can attach it to pushed updates.
	BannedGeneration(treeID string) (int64, error)
	AddBanned(b *pppv1.BannedFile) (gen int64, already bool, err error)
	RemoveBanned(treeID, filename string) (gen int64, removed bool, err error)
	GetBanned(treeID, filename string) (*pppv1.BannedFile, error)
	ListBanned(treeID string) ([]*pppv1.BannedFile, error)

	// Jobs (center-generated records only; local jobs are not stored).
	CreateJob(j *pppv1.Job) error
	GetJob(id string) (*pppv1.Job, error)
	UpdateJob(j *pppv1.Job) error
	ListJobs() ([]*pppv1.Job, error)
	JobsByFile(treeID, filename string) ([]*pppv1.Job, error)

	// UpsertProgress keeps the latest progress report per
	// (tree_id, job_id, filename, node_id). ListProgress returns the reports
	// of one tree (empty treeID returns all).
	UpsertProgress(p *pppv1.ProgressState, nodeID string) error
	ListProgress(treeID string) ([]*pppv1.ProgressState, error)

	Close() error
}
