package agent

import (
	"encoding/binary"
	"errors"
	"hash/crc64"
	"os"
	"path/filepath"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

// sparsePieceStore is the single unified PieceStore: one sparse data file per
// file with a bbolt index, written with WriteAt (pwrite semantics) and read
// with ReadAt (pread semantics). Missing pieces are holes in the sparse file
// and occupy no disk space; the bbolt index is the source of truth for
// existence.
//
//	<DownloadPath>/<basename>            — final file after MarkComplete
//	<DownloadPath>/<basename>.cds.pieces — sparse piece data (in progress)
//	<DownloadPath>/<basename>.cds.index  — bbolt: piece index -> PieceInfo
//
// On open: if <basename> exists the file is complete and opened read-only
// (every piece exists by construction); otherwise the .cds.pieces + .cds.index
// pair is opened and the existence map is rebuilt from the index, so a crash
// mid-download resumes instead of restarting. MarkComplete fsyncs, truncates
// to the authoritative size, drops the index and renames the pieces file to
// <basename>. A single mutex serializes the write path; reads of indexed pieces
// are safe under the same mutex.
type sparsePieceStore struct {
	dir string
	mu  sync.Mutex
	// open caches the per-file state; key = basename.
	open map[string]*sparseFile
}

type sparseFile struct {
	filename string

	piecesPath string
	indexPath  string
	finalPath  string

	size     int64 // total file size once known (final size / data-file size)
	complete bool  // final <basename> file exists (all pieces present)
	file     *os.File
	db       *bbolt.DB
	// present maps a piece index to its info for in-progress files (nil in
	// complete mode, where every piece exists by construction).
	present map[int64]*pppv1.PieceInfo
	// accessTS is the last completed-file access; idle completed files are
	// evicted from the open cache to bound open file handles.
	accessTS time.Time
}

// completeIdleTTL evicts completed (read-only) file handles from the open cache
// after this idle period, so long-lived agents do not accumulate handles for
// every file they ever touched. In-progress files stay open while the
// downloader is active. A var so tests can lower it.
var completeIdleTTL = 60 * time.Second

var bucketPieces = []byte("pieces")

func sparseFilePaths(dir, filename string) (piecesPath, indexPath, finalPath string) {
	return filepath.Join(dir, filename+".cds.pieces"),
		filepath.Join(dir, filename+".cds.index"),
		filepath.Join(dir, filename)
}

func sparseKey(index int64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	return buf[:]
}

// evictIdleLocked closes completed files idle beyond completeIdleTTL and drops
// them from the open cache (reopened on demand). Opportunistically run when a
// file is opened or completed. Must hold the store mutex.
func (s *sparsePieceStore) evictIdleLocked(now time.Time) {
	for key, f := range s.open {
		if f.complete && now.Sub(f.accessTS) > completeIdleTTL {
			_ = f.closeLocked()
			delete(s.open, key)
		}
	}
}

// touchLocked refreshes a completed file's access time. Must hold the store
// mutex (or the caller must otherwise serialize with the eviction sweep).
func (f *sparseFile) touchLocked(now time.Time) {
	if f.complete {
		f.accessTS = now
	}
}

func (s *sparsePieceStore) openLocked(filename string) (*sparseFile, error) {
	if !validBasename(filename) {
		return nil, errors.New("agent: invalid filename")
	}
	if f, ok := s.open[filename]; ok {
		f.touchLocked(time.Now())
		return f, nil
	}
	s.evictIdleLocked(time.Now())
	piecesPath, indexPath, finalPath := sparseFilePaths(s.dir, filename)
	f := &sparseFile{
		filename:   filename,
		piecesPath: piecesPath,
		indexPath:  indexPath,
		finalPath:  finalPath,
	}

	if stat, err := os.Stat(finalPath); err == nil {
		// Completed file: open read-only, every piece exists by construction.
		file, err := os.Open(finalPath)
		if err != nil {
			return nil, err
		}
		f.file = file
		f.complete = true
		f.size = stat.Size()
		f.accessTS = time.Now()
		s.open[filename] = f
		return f, nil
	}

	// In-progress file: sparse data file + bbolt index.
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
	s.open[filename] = f
	return f, nil
}

// closeLocked releases the file and index. Must hold the store mutex.
func (f *sparseFile) closeLocked() error {
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

// Close releases every cached handle (agent shutdown).
func (s *sparsePieceStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.open {
		_ = f.closeLocked()
	}
	s.open = make(map[string]*sparseFile)
	return nil
}

// Put stores a piece: pwrite into the sparse file at the piece offset, then
// record the piece in the index (the index is the existence source of truth).
func (s *sparsePieceStore) Put(filename string, index int64, data []byte) error {
	if index < 0 || len(data) == 0 || int64(len(data)) > PieceSize {
		return errors.New("agent: invalid piece index or size")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
	if err != nil {
		return err
	}
	if f.complete {
		return errors.New("agent: put to a completed file")
	}
	offset := index * PieceSize
	if _, err := f.file.WriteAt(data, offset); err != nil {
		return err
	}
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
		return tx.Bucket(bucketPieces).Put(sparseKey(index), val)
	}); err != nil {
		return err
	}
	f.present[index] = info
	if end := offset + int64(len(data)); end > f.size {
		f.size = end
	}
	return nil
}

// Get returns a copy of a piece via pread. Complete files derive the piece
// from the final file; in-progress files read the stored (indexed) slice.
func (s *sparsePieceStore) Get(filename string, index int64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
	if err != nil {
		return nil, err
	}
	var offset, size int64
	if f.complete {
		offset = index * PieceSize
		if offset < 0 || offset >= f.size {
			return nil, ErrPieceNotFound
		}
		size = PieceSize
		if offset+size > f.size {
			size = f.size - offset
		}
	} else {
		info, ok := f.present[index]
		if !ok {
			return nil, ErrPieceNotFound
		}
		offset, size = info.GetOffset(), int64(info.GetSize())
	}
	buf := make([]byte, size)
	if _, err := f.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

// HasPiece reports whether a piece exists.
func (s *sparsePieceStore) HasPiece(filename string, index int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
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
func (s *sparsePieceStore) PieceCount(filename string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
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
func (s *sparsePieceStore) Size(filename string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
	if err != nil {
		return 0
	}
	return f.size
}

// IsComplete reports whether the file was finalized by MarkComplete.
func (s *sparsePieceStore) IsComplete(filename string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
	if err != nil {
		return false
	}
	return f.complete
}

// MarkComplete finalizes a file: fsync the data, truncate to the exact size,
// close and drop the index, then rename the pieces file to the final
// <basename> path and reopen read-only.
func (s *sparsePieceStore) MarkComplete(filename string, size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, err := s.openLocked(filename)
	if err != nil {
		return err
	}
	if f.complete {
		return nil // idempotent
	}
	if err := f.file.Sync(); err != nil {
		return err
	}
	if err := f.closeLocked(); err != nil {
		return err
	}
	// The index is no longer needed once the file is finalized (closeLocked
	// already closed it).
	_ = os.Remove(f.indexPath)
	// Make the data file exactly the authoritative size.
	if err := os.Truncate(f.piecesPath, size); err != nil {
		return err
	}
	if err := os.Rename(f.piecesPath, f.finalPath); err != nil {
		return err
	}
	// Switch the cached state to complete (read-only) mode.
	cf, err := s.openCompleteLocked(f)
	if err != nil {
		return err
	}
	s.open[filename] = cf
	return nil
}

// openCompleteLocked opens the renamed final file read-only.
func (s *sparsePieceStore) openCompleteLocked(f *sparseFile) (*sparseFile, error) {
	file, err := os.Open(f.finalPath)
	if err != nil {
		return nil, err
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	return &sparseFile{
		filename:   f.filename,
		piecesPath: f.piecesPath,
		indexPath:  f.indexPath,
		finalPath:  f.finalPath,
		size:       stat.Size(),
		file:       file,
		complete:   true,
		accessTS:   time.Now(),
	}, nil
}

// ============ metadata sidecar ============
//
// The file-distribution core stores the sealed artifact metadata as a
// compact sidecar next to the final file: <basename>.cds.metadata. It is
// keyed by basename (the .cds. marker is reserved, so a metadata filename can
// never collide with a user basename). The sidecar is immutable once written;
// the .cds.commit marker that makes it authoritative is deferred to C2.

// metadataSidecarExt is the sidecar file suffix.
const metadataSidecarExt = ".cds.metadata"

// WriteMetadata writes the metadata sidecar for a file.
func (s *sparsePieceStore) WriteMetadata(filename string, data []byte) error {
	if !validBasename(filename) {
		return errors.New("agent: invalid filename")
	}
	return os.WriteFile(filepath.Join(s.dir, filename+metadataSidecarExt), data, 0o644)
}

// ReadMetadata returns the metadata sidecar for a file (ok=false when absent).
func (s *sparsePieceStore) ReadMetadata(filename string) ([]byte, bool, error) {
	if !validBasename(filename) {
		return nil, false, errors.New("agent: invalid filename")
	}
	data, err := os.ReadFile(filepath.Join(s.dir, filename+metadataSidecarExt))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return data, true, nil
}

// DeleteMetadata removes the metadata sidecar for a file.
func (s *sparsePieceStore) DeleteMetadata(filename string) error {
	if !validBasename(filename) {
		return errors.New("agent: invalid filename")
	}
	err := os.Remove(filepath.Join(s.dir, filename+metadataSidecarExt))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Delete removes every on-disk artifact of a file (and any cached state).
func (s *sparsePieceStore) Delete(filename string) error {
	if !validBasename(filename) {
		return errors.New("agent: invalid filename")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.open[filename]; ok {
		_ = f.closeLocked()
		delete(s.open, filename)
	}
	piecesPath, indexPath, finalPath := sparseFilePaths(s.dir, filename)
	_ = os.Remove(piecesPath)
	_ = os.Remove(indexPath)
	_ = os.Remove(finalPath)
	_ = s.DeleteMetadata(filename) // the .cds.metadata sidecar is part of the artifact
	return nil
}
