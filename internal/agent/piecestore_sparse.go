package agent

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
//	<DownloadPath>/<basename>            — sealed final file after Seal
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

// artifactPaths returns the three-piece sealed artifact paths.
func artifactPaths(dir, filename string) (finalPath, metadataPath, commitPath string) {
	return filepath.Join(dir, filename),
		filepath.Join(dir, filename+metadataSidecarExt),
		filepath.Join(dir, filename+commitExt)
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

	if s.sealedOnDisk(filename) {
		// Sealed artifact (data + metadata + commit all consistent): open
		// read-only (openCompleteLocked also verifies the final's size).
		cf, err := s.openCompleteLocked(f)
		if err != nil {
			return nil, err
		}
		s.open[filename] = cf
		return cf, nil
	}

	// Interrupted-Seal recovery (§9 / C2.5): a crash between the rename and
	// the commit-marker write must never fall through to the empty-pieces
	// path with a stale index (that would serve holes as real pieces and let
	// a later Seal destroy the real data). Complete the commit when the data
	// matches, or clean the stale staging otherwise.
	if err := s.recoverInterruptedSealLocked(filename, finalPath); err != nil {
		return nil, err
	}
	if s.sealedOnDisk(filename) {
		cf, err := s.openCompleteLocked(f)
		if err != nil {
			return nil, err
		}
		s.open[filename] = cf
		return cf, nil
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

// sealedOnDisk reports whether the three-piece sealed artifact is present and
// consistent: <basename> + .cds.metadata + .cds.commit exist AND the commit's
// metadata_id equals the SHA-256 of the local metadata sidecar. A partial
// publish (e.g. renamed data but no commit) is NOT complete and must never be
// served. Must hold s.mu.
func (s *sparsePieceStore) sealedOnDisk(filename string) bool {
	finalPath, metadataPath, commitPath := artifactPaths(s.dir, filename)
	stat, err := os.Stat(finalPath)
	if err != nil {
		return false
	}
	meta, err := os.ReadFile(metadataPath)
	if err != nil {
		return false
	}
	m, err := DecodeMetadata(meta)
	if err != nil {
		return false
	}
	// P3 defense: the final's size must match the metadata's FileSize, so a
	// truncated/corrupt data file is never reported sealed.
	if stat.Size() != m.FileSize {
		return false
	}
	commitData, err := os.ReadFile(commitPath)
	if err != nil {
		return false
	}
	commitID, err := DecodeCommit(commitData)
	if err != nil {
		return false
	}
	return bytes.Equal(commitID, MetadataID(meta))
}

// IsComplete reports whether the file was atomically published (Seal). It also
// triggers the interrupted-seal recovery, so a crash between the rename and
// the commit-marker write is completed (or cleaned) before the sealed check.
func (s *sparsePieceStore) IsComplete(filename string) bool {
	if !validBasename(filename) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.open[filename]; ok {
		return f.complete
	}
	_, _, finalPath := sparseFilePaths(s.dir, filename)
	if err := s.recoverInterruptedSealLocked(filename, finalPath); err != nil {
		return false
	}
	return s.sealedOnDisk(filename)
}

// Seal atomically publishes the completed artifact (design §9): the metadata
// sidecar is written and fsynced first, then the data, then the rename to
// <basename>, and the .cds.commit marker is written and fsynced LAST — only
// when the marker exists and its metadata_id matches the local metadata is the
// artifact visible and served. A crash before the marker leaves the artifact
// incomplete (never served; rebuilt on the next attempt). The index is no
// longer needed once the file is finalized.
func (s *sparsePieceStore) Seal(filename string, size int64, metadataBytes []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validBasename(filename) {
		return errors.New("agent: invalid filename")
	}
	f, err := s.openLocked(filename)
	if err != nil {
		return err
	}
	if f.complete {
		return nil // idempotent
	}
	// 1. metadata sidecar + fsync.
	_, metadataPath, commitPath := artifactPaths(s.dir, filename)
	if err := writeFileSync(metadataPath, metadataBytes); err != nil {
		return err
	}
	// 2. fsync the data file.
	if err := f.file.Sync(); err != nil {
		return err
	}
	// 3. close the index, truncate to the authoritative size, rename to the
	// final path (same filesystem), then make the rename durable.
	if err := f.closeLocked(); err != nil {
		return err
	}
	if err := os.Truncate(f.piecesPath, size); err != nil {
		return err
	}
	if err := os.Rename(f.piecesPath, f.finalPath); err != nil {
		return err
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	// 4. commit marker LAST + fsync (the atomic commit point).
	commitData, err := EncodeCommit(MetadataID(metadataBytes))
	if err != nil {
		return err
	}
	if err := writeFileSync(commitPath, commitData); err != nil {
		return err
	}
	if err := fsyncDir(s.dir); err != nil {
		return err
	}
	// 5. the index is no longer needed once finalized (closeLocked already
	// closed it).
	_ = os.Remove(f.indexPath)
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
	// P3 defense: the final's size must match the sealed metadata's FileSize;
	// a truncated/corrupt data file is never served.
	if meta, ok, err := s.ReadMetadata(f.filename); err == nil && ok {
		if m, derr := DecodeMetadata(meta); derr == nil && m.FileSize != stat.Size() {
			file.Close()
			return nil, fmt.Errorf("agent: sealed artifact size mismatch: final %d, metadata %d", stat.Size(), m.FileSize)
		}
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

// recoverInterruptedSealLocked recognizes a crash between the rename and the
// commit-marker write (<basename> + .cds.metadata present, .cds.commit
// missing) and either completes the seal or cleans the stale staging:
//   - the final's size equals the metadata's FileSize -> the data is intact
//     and authoritative; the commit marker is written to finish the publish
//     (no re-download, no data loss);
//   - otherwise -> the staged data is suspect; the stale index, metadata and
//     final are removed so the normal in-progress/re-download path starts
//     clean.
//
// This NEVER falls through to "rebuild an empty .cds.pieces over a stale
// index": that would serve empty holes as real pieces (HasPiece false-positives)
// and let a later Seal rename the empty file over the real data. Must hold s.mu.
func (s *sparsePieceStore) recoverInterruptedSealLocked(filename, finalPath string) error {
	finalStat, err := os.Stat(finalPath)
	if err != nil {
		return nil // no final: nothing to recover
	}
	_, metadataPath, commitPath := artifactPaths(s.dir, filename)
	meta, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil // no metadata: not an interrupted seal
	}
	if _, err := os.Stat(commitPath); err == nil {
		return nil // commit present: sealedOnDisk handles it
	}
	m, err := DecodeMetadata(meta)
	if err != nil {
		// Unreadable metadata: cannot complete; drop the stale staging.
		s.clearStaleStagingLocked(filename, finalPath)
		return nil
	}
	if finalStat.Size() != m.FileSize {
		// Data does not match the metadata: stale/corrupt staging.
		s.clearStaleStagingLocked(filename, finalPath)
		return nil
	}
	// Data matches the metadata: complete the seal by writing the commit.
	commitData, err := EncodeCommit(MetadataID(meta))
	if err != nil {
		return err
	}
	if err := writeFileSync(commitPath, commitData); err != nil {
		return err
	}
	return fsyncDir(s.dir)
}

// clearStaleStagingLocked removes the stale index, metadata and final of an
// interrupted seal whose data does not match, so the in-progress path starts
// clean. Must hold s.mu.
func (s *sparsePieceStore) clearStaleStagingLocked(filename, finalPath string) {
	_, indexPath, _ := sparseFilePaths(s.dir, filename)
	_, metadataPath, _ := artifactPaths(s.dir, filename)
	_ = os.Remove(indexPath)
	_ = os.Remove(metadataPath)
	_ = os.Remove(finalPath)
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

// commitExt is the .cds.commit marker suffix (written LAST at Seal).
const commitExt = ".cds.commit"

// writeFileSync writes a file and fsyncs it, so a later step can rely on it.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// fsyncDir fsyncs a directory so a rename/creation inside it is durable.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

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
	_, metadataPath, commitPath := artifactPaths(s.dir, filename)
	_ = os.Remove(metadataPath)
	_ = os.Remove(commitPath)
	return nil
}
