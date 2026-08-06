package ctl

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// Sentinel errors returned by Store implementations.
var (
	// ErrNotFound is returned when the requested record does not exist.
	ErrNotFound = errors.New("ctl: not found")
	// ErrExists is returned when a create collides with an existing record.
	ErrExists = errors.New("ctl: already exists")
)

// Store is the durable state behind the control plane. It is an interface so
// a future HA deployment (raft/etcd) can swap the bbolt implementation.
type Store interface {
	// Trees.
	CreateTree(t *pppv1.Tree) error
	GetTree(id string) (*pppv1.Tree, error)
	ListTrees() ([]*pppv1.Tree, error)
	DeleteTree(id string) error

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
	// (tree_id, filename, node_id). It is kept in memory in phase 1 and made
	// durable in phase 4.
	UpsertProgress(p *pppv1.ProgressState, nodeID string) error

	Close() error
}

// bbolt buckets.
var (
	bucketTrees  = []byte("trees")
	bucketNodes  = []byte("nodes")
	bucketJobs   = []byte("jobs")
	bucketBanned = []byte("banned")
	bucketMeta   = []byte("meta")
)

// meta key prefixes.
var (
	metaTopologyGen = "topology_gen\x00"
	metaBannedGen   = "banned_gen\x00"
)

// bboltStore is the bbolt-backed Store implementation.
type bboltStore struct {
	db *bbolt.DB

	mu       sync.Mutex // guards progress (in-memory until phase 4)
	progress map[string]*pppv1.ProgressState
}

// OpenStore opens (or creates) the bbolt database at path and ensures all
// buckets exist.
func OpenStore(path string) (*bboltStore, error) {
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("ctl: open store %q: %w", path, err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{bucketTrees, bucketNodes, bucketJobs, bucketBanned, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("ctl: init store: %w", err)
	}
	return &bboltStore{db: db, progress: make(map[string]*pppv1.ProgressState)}, nil
}

// Close closes the underlying database.
func (s *bboltStore) Close() error { return s.db.Close() }

// ============ Trees ============

func (s *bboltStore) CreateTree(t *pppv1.Tree) error {
	if t.GetId() == "" {
		return errors.New("ctl: tree id is empty")
	}
	data, err := proto.Marshal(t)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketTrees)
		if b.Get([]byte(t.GetId())) != nil {
			return ErrExists
		}
		return b.Put([]byte(t.GetId()), data)
	})
}

func (s *bboltStore) GetTree(id string) (*pppv1.Tree, error) {
	if id == "" {
		return nil, errors.New("ctl: tree id is empty")
	}
	var out *pppv1.Tree
	err := s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketTrees).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		t := &pppv1.Tree{}
		if err := proto.Unmarshal(data, t); err != nil {
			return err
		}
		out = t
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) ListTrees() ([]*pppv1.Tree, error) {
	var out []*pppv1.Tree
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketTrees).ForEach(func(_, v []byte) error {
			t := &pppv1.Tree{}
			if err := proto.Unmarshal(v, t); err != nil {
				return err
			}
			out = append(out, t)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) DeleteTree(id string) error {
	if id == "" {
		return errors.New("ctl: tree id is empty")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		if tx.Bucket(bucketTrees).Get([]byte(id)) == nil {
			return ErrNotFound
		}
		return tx.Bucket(bucketTrees).Delete([]byte(id))
	})
}

// ============ Nodes ============

// nodeKey indexes a node by (tree_id, node_id) so per-tree listing is a
// prefix scan.
func nodeKey(treeID, nodeID string) []byte {
	return []byte(treeID + "\x00" + nodeID)
}

func (s *bboltStore) PutNode(n *pppv1.Node) error {
	if n.GetId() == "" || n.GetTreeId() == "" {
		return errors.New("ctl: node id and tree_id are required")
	}
	data, err := proto.Marshal(n)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketNodes).Put(nodeKey(n.GetTreeId(), n.GetId()), data)
	})
}

func (s *bboltStore) DeleteNode(treeID, nodeID string) error {
	if treeID == "" || nodeID == "" {
		return errors.New("ctl: node tree_id and id are required")
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketNodes).Delete(nodeKey(treeID, nodeID))
	})
}

func (s *bboltStore) ListNodes(treeID string) ([]*pppv1.Node, error) {
	var out []*pppv1.Node
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketNodes)
		if treeID == "" {
			return b.ForEach(func(_, v []byte) error {
				n := &pppv1.Node{}
				if err := proto.Unmarshal(v, n); err != nil {
					return err
				}
				out = append(out, n)
				return nil
			})
		}
		prefix := []byte(treeID + "\x00")
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			n := &pppv1.Node{}
			if err := proto.Unmarshal(v, n); err != nil {
				return err
			}
			out = append(out, n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ============ Generations ============

func genKey(prefix, treeID string) []byte { return []byte(prefix + treeID) }

func getGen(tx *bbolt.Tx, key []byte) int64 {
	v := tx.Bucket(bucketMeta).Get(key)
	if len(v) != 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(v))
}

func putGen(tx *bbolt.Tx, key []byte, gen int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(gen))
	return tx.Bucket(bucketMeta).Put(key, buf[:])
}

func (s *bboltStore) TopologyGeneration(treeID string) (int64, error) {
	var gen int64
	err := s.db.View(func(tx *bbolt.Tx) error {
		gen = getGen(tx, genKey(metaTopologyGen, treeID))
		return nil
	})
	return gen, err
}

func (s *bboltStore) BumpTopologyGeneration(treeID string) (int64, error) {
	var gen int64
	err := s.db.Update(func(tx *bbolt.Tx) error {
		gen = getGen(tx, genKey(metaTopologyGen, treeID)) + 1
		return putGen(tx, genKey(metaTopologyGen, treeID), gen)
	})
	return gen, err
}

func (s *bboltStore) BannedGeneration(treeID string) (int64, error) {
	var gen int64
	err := s.db.View(func(tx *bbolt.Tx) error {
		gen = getGen(tx, genKey(metaBannedGen, treeID))
		return nil
	})
	return gen, err
}

// ============ Banned list ============

// bannedKey indexes a banned file by (tree_id, filename).
func bannedKey(treeID, filename string) []byte {
	return []byte(treeID + "\x00" + filename)
}

func (s *bboltStore) AddBanned(b *pppv1.BannedFile) (int64, bool, error) {
	if b.GetTreeId() == "" || b.GetFilename() == "" {
		return 0, false, errors.New("ctl: banned tree_id and filename are required")
	}
	data, err := proto.Marshal(b)
	if err != nil {
		return 0, false, err
	}
	var gen int64
	var already bool
	err = s.db.Update(func(tx *bbolt.Tx) error {
		bb := tx.Bucket(bucketBanned)
		key := bannedKey(b.GetTreeId(), b.GetFilename())
		if bb.Get(key) != nil {
			already = true
			gen = getGen(tx, genKey(metaBannedGen, b.GetTreeId()))
			return nil
		}
		if err := bb.Put(key, data); err != nil {
			return err
		}
		gen = getGen(tx, genKey(metaBannedGen, b.GetTreeId())) + 1
		return putGen(tx, genKey(metaBannedGen, b.GetTreeId()), gen)
	})
	return gen, already, err
}

func (s *bboltStore) RemoveBanned(treeID, filename string) (int64, bool, error) {
	if treeID == "" || filename == "" {
		return 0, false, errors.New("ctl: banned tree_id and filename are required")
	}
	var gen int64
	var removed bool
	var err error
	err = s.db.Update(func(tx *bbolt.Tx) error {
		bb := tx.Bucket(bucketBanned)
		key := bannedKey(treeID, filename)
		if bb.Get(key) == nil {
			gen = getGen(tx, genKey(metaBannedGen, treeID))
			return nil
		}
		if err := bb.Delete(key); err != nil {
			return err
		}
		removed = true
		gen = getGen(tx, genKey(metaBannedGen, treeID)) + 1
		return putGen(tx, genKey(metaBannedGen, treeID), gen)
	})
	return gen, removed, err
}

func (s *bboltStore) GetBanned(treeID, filename string) (*pppv1.BannedFile, error) {
	var out *pppv1.BannedFile
	err := s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketBanned).Get(bannedKey(treeID, filename))
		if data == nil {
			return ErrNotFound
		}
		b := &pppv1.BannedFile{}
		if err := proto.Unmarshal(data, b); err != nil {
			return err
		}
		out = b
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) ListBanned(treeID string) ([]*pppv1.BannedFile, error) {
	if treeID == "" {
		return nil, errors.New("ctl: tree id is required")
	}
	prefix := []byte(treeID + "\x00")
	var out []*pppv1.BannedFile
	err := s.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(bucketBanned).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			b := &pppv1.BannedFile{}
			if err := proto.Unmarshal(v, b); err != nil {
				return err
			}
			out = append(out, b)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ============ Jobs ============

func (s *bboltStore) CreateJob(j *pppv1.Job) error {
	if j.GetId() == "" {
		return errors.New("ctl: job id is empty")
	}
	data, err := proto.Marshal(j)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		bb := tx.Bucket(bucketJobs)
		if bb.Get([]byte(j.GetId())) != nil {
			return ErrExists
		}
		return bb.Put([]byte(j.GetId()), data)
	})
}

func (s *bboltStore) GetJob(id string) (*pppv1.Job, error) {
	if id == "" {
		return nil, errors.New("ctl: job id is empty")
	}
	var out *pppv1.Job
	err := s.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bucketJobs).Get([]byte(id))
		if data == nil {
			return ErrNotFound
		}
		j := &pppv1.Job{}
		if err := proto.Unmarshal(data, j); err != nil {
			return err
		}
		out = j
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) UpdateJob(j *pppv1.Job) error {
	if j.GetId() == "" {
		return errors.New("ctl: job id is empty")
	}
	data, err := proto.Marshal(j)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		bb := tx.Bucket(bucketJobs)
		if bb.Get([]byte(j.GetId())) == nil {
			return ErrNotFound
		}
		return bb.Put([]byte(j.GetId()), data)
	})
}

func (s *bboltStore) ListJobs() ([]*pppv1.Job, error) {
	var out []*pppv1.Job
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketJobs).ForEach(func(_, v []byte) error {
			j := &pppv1.Job{}
			if err := proto.Unmarshal(v, j); err != nil {
				return err
			}
			out = append(out, j)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *bboltStore) JobsByFile(treeID, filename string) ([]*pppv1.Job, error) {
	all, err := s.ListJobs()
	if err != nil {
		return nil, err
	}
	var out []*pppv1.Job
	for _, j := range all {
		if j.GetTreeId() == treeID && j.GetFilename() == filename {
			out = append(out, j)
		}
	}
	return out, nil
}

// ============ Progress ============

// progressKey uniquely identifies a progress report.
func progressKey(treeID, filename, nodeID string) string {
	return treeID + "\x00" + filename + "\x00" + nodeID
}

func (s *bboltStore) UpsertProgress(p *pppv1.ProgressState, nodeID string) error {
	if p == nil || p.GetTreeId() == "" || p.GetFilename() == "" || nodeID == "" {
		return errors.New("ctl: progress tree_id, filename and node_id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.progress[progressKey(p.GetTreeId(), p.GetFilename(), nodeID)] = p
	return nil
}
