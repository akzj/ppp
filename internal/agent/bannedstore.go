package agent

import (
	"encoding/binary"
	"path/filepath"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// bannedSaveWindow coalesces bursty Save calls: full rewrites of the local
// banned list are debounced so a burst of increments collapses into one write.
const bannedSaveWindow = 500 * time.Millisecond

// bannedDiskStore persists the banned list locally (bbolt, one file per node)
// so a restarted node keeps rejecting banned files during the restart window,
// before the ctl full sync arrives. Save is debounced (P3-optimization): the
// latest snapshot is flushed after a short quiet window, and Close always
// flushes synchronously so restart recovery sees the latest state.
type bannedDiskStore struct {
	db *bbolt.DB

	mu      sync.Mutex
	gen     int64
	files   []*pppv1.BannedFile
	pending bool
	timer   *time.Timer
	closed  bool
}

var (
	bucketBannedLocal = []byte("banned")
	bannedLocalGenKey = []byte("__gen__")
)

// openBannedDiskStore opens (or creates) the local banned store in dir.
func openBannedDiskStore(dir string) (*bannedDiskStore, error) {
	db, err := bbolt.Open(filepath.Join(dir, "banned.db"), 0o600, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketBannedLocal)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &bannedDiskStore{db: db}, nil
}

// Close flushes any pending snapshot and releases the database.
func (b *bannedDiskStore) Close() error {
	b.mu.Lock()
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
	}
	pending := b.pending
	b.pending = false
	gen, files := b.gen, b.files
	b.mu.Unlock()
	if pending {
		if err := b.write(gen, files); err != nil {
			_ = b.db.Close()
			return err
		}
	}
	return b.db.Close()
}

// Save records the latest snapshot and schedules a coalesced flush. Multiple
// saves within bannedSaveWindow collapse into a single full rewrite. It never
// fails: the write happens asynchronously (or synchronously in Close).
func (b *bannedDiskStore) Save(gen int64, files []*pppv1.BannedFile) {
	b.mu.Lock()
	b.gen, b.files = gen, files
	if !b.pending && !b.closed {
		b.pending = true
		b.timer = time.AfterFunc(bannedSaveWindow, b.flush)
	}
	b.mu.Unlock()
}

// flush writes the latest pending snapshot (called from the debounce timer).
func (b *bannedDiskStore) flush() {
	b.mu.Lock()
	if b.closed || !b.pending {
		b.mu.Unlock()
		return
	}
	b.pending = false
	gen, files := b.gen, b.files
	b.mu.Unlock()
	// Best effort: a flush racing Close may fail on a closed db; the ctl
	// re-syncs the authoritative list on restart.
	_ = b.write(gen, files)
}

// write atomically replaces the persisted banned list and its generation.
func (b *bannedDiskStore) write(gen int64, files []*pppv1.BannedFile) error {
	return b.db.Update(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(bucketBannedLocal)
		// Clear all existing keys first (including the generation marker).
		c := bkt.Cursor()
		var keys [][]byte
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := bkt.Delete(k); err != nil {
				return err
			}
		}
		var genBuf [8]byte
		binary.BigEndian.PutUint64(genBuf[:], uint64(gen))
		if err := bkt.Put(bannedLocalGenKey, genBuf[:]); err != nil {
			return err
		}
		for _, f := range files {
			data, err := proto.Marshal(f)
			if err != nil {
				return err
			}
			if err := bkt.Put([]byte(bannedKey(f.GetTreeId(), f.GetFilename())), data); err != nil {
				return err
			}
		}
		return nil
	})
}

// Load returns the persisted generation and banned files.
func (b *bannedDiskStore) Load() (int64, []*pppv1.BannedFile, error) {
	var gen int64
	var files []*pppv1.BannedFile
	err := b.db.View(func(tx *bbolt.Tx) error {
		bkt := tx.Bucket(bucketBannedLocal)
		if v := bkt.Get(bannedLocalGenKey); len(v) == 8 {
			gen = int64(binary.BigEndian.Uint64(v))
		}
		return bkt.ForEach(func(k, v []byte) error {
			if string(k) == string(bannedLocalGenKey) {
				return nil
			}
			f := &pppv1.BannedFile{}
			if err := proto.Unmarshal(v, f); err != nil {
				return err
			}
			files = append(files, f)
			return nil
		})
	})
	return gen, files, err
}
