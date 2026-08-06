package agent

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc64"
	"os"
	"path/filepath"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// mmapPieceStore implements PieceStore with a memory-mapped pieces file plus a
// bbolt index, porting the idea from tengen's piece_file.go (no tengen import):
//
//	<DownloadPath>/<tree-hex>/<file-hex>.cds.pieces  — mmap'd piece data
//	<DownloadPath>/<tree-hex>/<file-hex>.cds.index   — bbolt: piece index -> PieceInfo
//	<DownloadPath>/<tree-hex>/<file-hex>.cds.complete — final file after MarkComplete
//
// A completed file is opened read-only with every piece available; an
// in-progress file is opened read-write and its existence map is rebuilt from
// the bbolt index, so a crash mid-download resumes instead of restarting.
//
// Because PieceStore.Put carries no file size, the pieces file grows on demand
// (ftruncate + remap) instead of being pre-truncated; MarkComplete truncates to
// the authoritative size. A single mutex serializes every operation (the write
// path by design; reads are cheap), and Get always copies so no caller can hold
// a slice that a grow-remap would invalidate.
type mmapPieceStore struct {
	dir string
	mu  sync.Mutex
	// open caches the per-file state; key = treeID + "\x00" + filename.
	open map[string]*mmapFile
}

type mmapFile struct {
	treeID   string
	filename string
	treeDir  string // <DownloadPath>/<tree-hex>
	base     string // <file-hex>

	piecesPath   string
	indexPath    string
	completePath string

	size     int64
	file     *os.File
	data     []byte // mmap region (nil when not mapped)
	complete bool
	db       *bbolt.DB
	// present maps a piece index to its info for in-progress files (nil in
	// complete mode, where every piece exists by construction).
	present map[int64]*pppv1.PieceInfo
	// accessTS is the last completed-file access; idle completed files are
	// evicted from the open cache to bound address-space usage.
	accessTS time.Time
}

// completeIdleTTL evicts completed (read-only) mmaps from the open cache after
// this idle period, so long-lived agents do not accumulate mappings for every
// file they ever touched. In-progress files stay open while the downloader is
// active. A var so tests can lower it.
var completeIdleTTL = 60 * time.Second

var bucketPieces = []byte("pieces")

// NewMmapPieceStore creates the mmap-backed piece store rooted at dir.
func NewMmapPieceStore(dir string) (PieceStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("agent: create mmap piece dir: %w", err)
	}
	return &mmapPieceStore{dir: dir, open: make(map[string]*mmapFile)}, nil
}

func mmapFilePaths(dir, treeID, filename string) (treeDir, base, piecesPath, indexPath, completePath string) {
	treeDir = filepath.Join(dir, hex.EncodeToString([]byte(treeID)))
	base = hex.EncodeToString([]byte(filename))
	return treeDir, base,
		filepath.Join(treeDir, base+".cds.pieces"),
		filepath.Join(treeDir, base+".cds.index"),
		filepath.Join(treeDir, base+".cds.complete")
}

func mmapSlice(f *os.File, size int64, readOnly bool) ([]byte, error) {
	prot := unix.PROT_READ
	if !readOnly {
		prot |= unix.PROT_WRITE
	}
	return unix.Mmap(int(f.Fd()), 0, int(size), prot, unix.MAP_SHARED)
}

func mmapKey(index int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	return buf[:]
}

// evictIdleLocked closes completed files idle beyond completeIdleTTL and drops
// them from the open cache (reopened on demand). Opportunistically run when a
// file is opened or completed. Must hold the store mutex.
func (s *mmapPieceStore) evictIdleLocked(now time.Time) {
	for key, f := range s.open {
		if f.complete && now.Sub(f.accessTS) > completeIdleTTL {
			_ = f.closeLocked()
			delete(s.open, key)
		}
	}
}

// touchLocked refreshes a completed file's access time. Must hold the store
// mutex (or the caller must otherwise serialize with the eviction sweep).
func (f *mmapFile) touchLocked(now time.Time) {
	if f.complete {
		f.accessTS = now
	}
}

func (s *mmapPieceStore) openLocked(key, treeID, filename string) (*mmapFile, error) {
	if f, ok := s.open[key]; ok {
		f.touchLocked(time.Now())
		return f, nil
	}
	s.evictIdleLocked(time.Now())
	treeDir, base, piecesPath, indexPath, completePath := mmapFilePaths(s.dir, treeID, filename)
	f := &mmapFile{
		treeID: treeID, filename: filename,
		treeDir: treeDir, base: base,
		piecesPath: piecesPath, indexPath: indexPath, completePath: completePath,
	}

	if _, err := os.Stat(completePath); err == nil {
		// Completed file: mmap read-only, every piece exists by construction.
		file, err := os.Open(completePath)
		if err != nil {
			return nil, err
		}
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		f.file = file
		f.complete = true
		f.size = stat.Size()
		if f.size > 0 {
			if f.data, err = mmapSlice(file, f.size, true); err != nil {
				file.Close()
				return nil, err
			}
		}
		f.accessTS = time.Now()
		s.open[key] = f
		return f, nil
	}

	// In-progress file: mmap read-write (grows on demand) + bbolt index.
	if err := os.MkdirAll(treeDir, 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(piecesPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	f.file = file
	f.size = stat.Size()
	if f.size > 0 {
		if f.data, err = mmapSlice(file, f.size, false); err != nil {
			file.Close()
			return nil, err
		}
	}
	db, err := bbolt.Open(indexPath, 0o600, nil)
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketPieces)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	f.db = db
	f.present = make(map[int64]*pppv1.PieceInfo)
	// Crash recovery: rebuild the existence map from the index.
	if err := db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketPieces).ForEach(func(k, v []byte) error {
			if len(k) != 8 {
				return nil
			}
			info := &pppv1.PieceInfo{}
			if err := proto.Unmarshal(v, info); err != nil {
				return err
			}
			f.present[int64(binary.BigEndian.Uint64(k))] = info
			return nil
		})
	}); err != nil {
		db.Close()
		return nil, err
	}
	f.accessTS = time.Now()
	s.open[key] = f
	return f, nil
}

// growLocked grows the pieces file and remaps it. Must hold the store mutex.
func (f *mmapFile) growLocked(needed int64) error {
	if f.data != nil {
		if err := unix.Munmap(f.data); err != nil {
			return err
		}
		f.data = nil
	}
	if err := f.file.Truncate(needed); err != nil {
		return err
	}
	data, err := mmapSlice(f.file, needed, false)
	if err != nil {
		return err
	}
	f.data = data
	f.size = needed
	return nil
}

// closeLocked releases the mmap, file and index. Must hold the store mutex.
func (f *mmapFile) closeLocked() error {
	if f.data != nil {
		if err := unix.Munmap(f.data); err != nil {
			return err
		}
		f.data = nil
	}
	if f.file != nil {
		if err := f.file.Close(); err != nil {
			return err
		}
		f.file = nil
	}
	if f.db != nil {
		if err := f.db.Close(); err != nil {
			return err
		}
		f.db = nil
	}
	return nil
}

// Close releases every cached mmap (agent shutdown). Not part of the
// PieceStore interface.
func (s *mmapPieceStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.open {
		_ = f.closeLocked()
	}
	s.open = make(map[string]*mmapFile)
	return nil
}

// Put stores a piece: validate, grow the mmap if needed, write the bytes and
// the index.
func (s *mmapPieceStore) Put(treeID, filename string, index int64, data []byte) error {
	if index < 0 || len(data) == 0 || int64(len(data)) > PieceSize {
		return errors.New("agent: invalid piece index or size")
	}
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return err
	}
	if f.complete {
		return errors.New("agent: put to a completed file")
	}
	offset := index * PieceSize
	needed := offset + int64(len(data))
	if needed > int64(len(f.data)) {
		if err := f.growLocked(needed); err != nil {
			return err
		}
	}
	copy(f.data[offset:offset+int64(len(data))], data)
	info := &pppv1.PieceInfo{
		Hash:   crc64.Checksum(data, crcTable),
		Index:  index,
		Size:   int32(len(data)),
		Offset: offset,
	}
	val, err := proto.Marshal(info)
	if err != nil {
		return err
	}
	if err := f.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketPieces).Put(mmapKey(index), val)
	}); err != nil {
		return err
	}
	f.present[index] = info
	return nil
}

// Get returns a copy of a piece. Complete files derive the piece from the
// final file; in-progress files read the stored slice.
func (s *mmapPieceStore) Get(treeID, filename string, index int64) ([]byte, error) {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return nil, err
	}
	if f.complete {
		offset := index * PieceSize
		if offset < 0 || offset >= f.size {
			return nil, ErrPieceNotFound
		}
		end := offset + PieceSize
		if end > f.size {
			end = f.size
		}
		return append([]byte(nil), f.data[offset:end]...), nil
	}
	info, ok := f.present[index]
	if !ok {
		return nil, ErrPieceNotFound
	}
	return append([]byte(nil), f.data[info.GetOffset():info.GetOffset()+int64(info.GetSize())]...), nil
}

// HasPiece reports whether a piece exists.
func (s *mmapPieceStore) HasPiece(treeID, filename string, index int64) bool {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return false
	}
	if f.complete {
		return index >= 0 && index*PieceSize < f.size
	}
	_, ok := f.present[index]
	return ok
}

// PieceCount returns the number of stored pieces.
func (s *mmapPieceStore) PieceCount(treeID, filename string) int {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return 0
	}
	if f.complete {
		return int((f.size + PieceSize - 1) / PieceSize)
	}
	return len(f.present)
}

// Size returns the current file size (0 when the file has not been opened or
// written).
func (s *mmapPieceStore) Size(treeID, filename string) int64 {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return 0
	}
	return f.size
}

// IsComplete reports whether the file was finalized by MarkComplete.
func (s *mmapPieceStore) IsComplete(treeID, filename string) bool {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return false
	}
	return f.complete
}

// MarkComplete finalizes a file: flush the mmap, unmap, close the pieces file,
// drop the index and rename the pieces file to the final name.
func (s *mmapPieceStore) MarkComplete(treeID, filename string, size int64) error {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(key, treeID, filename)
	if err != nil {
		return err
	}
	if f.complete {
		return nil // idempotent
	}
	if f.data != nil {
		if err := unix.Msync(f.data, unix.MS_SYNC); err != nil {
			return err
		}
	}
	if err := f.closeLocked(); err != nil {
		return err
	}
	// The index is no longer needed once the file is finalized (closeLocked
	// already closed it).
	_ = os.Remove(f.indexPath)
	// Make the pieces file exactly the authoritative size before renaming.
	if size != f.size {
		if err := os.Truncate(f.piecesPath, size); err != nil {
			return err
		}
	}
	if err := os.Rename(f.piecesPath, f.completePath); err != nil {
		return err
	}
	// Switch the cached state to complete (read-only) mode.
	cf, err := s.openCompleteLocked(f)
	if err != nil {
		return err
	}
	s.open[key] = cf
	return nil
}

// openCompleteLocked opens the renamed final file read-only.
func (s *mmapPieceStore) openCompleteLocked(f *mmapFile) (*mmapFile, error) {
	file, err := os.Open(f.completePath)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	cf := &mmapFile{
		treeID: f.treeID, filename: f.filename,
		treeDir: f.treeDir, base: f.base,
		piecesPath: f.piecesPath, indexPath: f.indexPath, completePath: f.completePath,
		size: stat.Size(), file: file, complete: true,
	}
	if stat.Size() > 0 {
		if cf.data, err = mmapSlice(file, stat.Size(), true); err != nil {
			file.Close()
			return nil, err
		}
	}
	return cf, nil
}

// Delete removes every on-disk artifact of a file (and any cached state).
func (s *mmapPieceStore) Delete(treeID, filename string) error {
	key := treeID + "\x00" + filename
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.open[key]; ok {
		_ = f.closeLocked()
		delete(s.open, key)
	}
	_, _, piecesPath, indexPath, completePath := mmapFilePaths(s.dir, treeID, filename)
	_ = os.Remove(piecesPath)
	_ = os.Remove(indexPath)
	_ = os.Remove(completePath)
	return nil
}
