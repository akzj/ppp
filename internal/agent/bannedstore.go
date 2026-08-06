package agent

import (
	"encoding/binary"
	"path/filepath"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// bannedDiskStore persists the banned list locally (bbolt, one file per node)
// so a restarted node keeps rejecting banned files during the restart window,
// before the ctl full sync arrives.
type bannedDiskStore struct {
	db *bbolt.DB
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

// Close releases the database.
func (b *bannedDiskStore) Close() error { return b.db.Close() }

// Save atomically replaces the persisted banned list and its generation.
func (b *bannedDiskStore) Save(gen int64, files []*pppv1.BannedFile) error {
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
