package agent

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newSparseTestStore(t *testing.T, dir string) PieceStore {
	t.Helper()
	if dir == "" {
		dir = filepath.Join(t.TempDir(), "sparse")
	}
	st, err := NewPieceStore(dir)
	if err != nil {
		t.Fatalf("NewPieceStore: %v", err)
	}
	t.Cleanup(func() {
		if sc, ok := st.(*sparsePieceStore); ok {
			_ = sc.Close()
		}
	})
	return st
}

// TestSparsePieceStoreRoundtrip verifies put/get/complete/delete on the sparse
// store and that Seal atomically publishes the three-piece artifact.
func TestSparsePieceStoreRoundtrip(t *testing.T) {
	st := newSparseTestStore(t, "")

	if _, err := st.Get("a.bin", 0); !errors.Is(err, ErrPieceNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrPieceNotFound", err)
	}

	p0 := make([]byte, 10)
	copy(p0, "0123456789")
	if err := st.Put("a.bin", 0, p0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := st.Get("a.bin", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, p0) {
		t.Fatalf("Get = %q, want %q", got, p0)
	}
	if !st.HasPiece("a.bin", 0) {
		t.Fatal("HasPiece(0) = false after Put")
	}
	if st.HasPiece("a.bin", 1) {
		t.Fatal("HasPiece(1) = true, want false")
	}
	if st.PieceCount("a.bin") != 1 {
		t.Fatalf("PieceCount = %d, want 1", st.PieceCount("a.bin"))
	}

	// Complete: size becomes authoritative, the final file is <basename>.
	sealTestFile(t, st, "a.bin", 10)
	if !st.IsComplete("a.bin") {
		t.Fatal("IsComplete = false after Seal")
	}
	if st.Size("a.bin") != 10 {
		t.Fatalf("Size = %d, want 10", st.Size("a.bin"))
	}
	got, err = st.Get("a.bin", 0)
	if err != nil {
		t.Fatalf("Get after complete: %v", err)
	}
	if !bytes.Equal(got, p0) {
		t.Fatalf("Get after complete = %q, want %q", got, p0)
	}

	// The final file lives at <DownloadPath>/<basename> (flat, no tree, no
	// .cds suffix) with the exact content; the pieces/index are gone.
	sc := st.(*sparsePieceStore)
	sc.mu.Lock()
	final := sc.open["a.bin"].finalPath
	sc.mu.Unlock()
	if filepath.Base(final) != "a.bin" || strings.Contains(final, ".cds") {
		t.Fatalf("final path = %q, want <DownloadPath>/a.bin", final)
	}
	if data, err := os.ReadFile(final); err != nil || !bytes.Equal(data, p0) {
		t.Fatalf("final file content = %q, %v; want %q", data, err, p0)
	}
	if _, err := os.Stat(sc.dir + "/a.bin.cds.pieces"); !os.IsNotExist(err) {
		t.Fatalf("pieces file still exists after Seal (err=%v)", err)
	}
	if _, err := os.Stat(sc.dir + "/a.bin.cds.index"); !os.IsNotExist(err) {
		t.Fatalf("index file still exists after Seal (err=%v)", err)
	}

	if err := st.Delete("a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if st.IsComplete("a.bin") || st.HasPiece("a.bin", 0) {
		t.Fatal("file state survives Delete")
	}
	if _, err := os.Stat(final); !os.IsNotExist(err) {
		t.Fatalf("final file survives Delete (err=%v)", err)
	}
}

// TestSparsePieceStoreHoles verifies cross-piece holes: writing pieces 0 and 2
// leaves piece 1 missing (a hole in the sparse file) and the data is only
// readable for indexed pieces.
func TestSparsePieceStoreHoles(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sparse")
	st := newSparseTestStore(t, dir)

	p0 := make([]byte, 100)
	p2 := make([]byte, 50)
	for i := range p0 {
		p0[i] = 1
	}
	for i := range p2 {
		p2[i] = 2
	}
	if err := st.Put("a.bin", 0, p0); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := st.Put("a.bin", 2, p2); err != nil {
		t.Fatalf("Put(2): %v", err)
	}

	// Piece 1 is a hole: missing, and the data file has a hole at its offset.
	if st.HasPiece("a.bin", 1) {
		t.Fatal("HasPiece(1) = true, want false (hole)")
	}
	if _, err := st.Get("a.bin", 1); !errors.Is(err, ErrPieceNotFound) {
		t.Fatalf("Get(1) = %v, want ErrPieceNotFound", err)
	}
	if st.PieceCount("a.bin") != 2 {
		t.Fatalf("PieceCount = %d, want 2", st.PieceCount("a.bin"))
	}

	// The hole occupies no disk space (apparent size > allocated blocks).
	fi, err := os.Stat(filepath.Join(dir, "a.bin.cds.pieces"))
	if err != nil {
		t.Fatalf("stat pieces: %v", err)
	}
	if fi.Size() != 2*PieceSize+int64(len(p2)) {
		t.Fatalf("pieces file size = %d, want %d", fi.Size(), 2*PieceSize+int64(len(p2)))
	}

	// Fill the hole and verify the complete file content matches the pieces.
	if err := st.Put("a.bin", 0, append(append([]byte{}, p0...), make([]byte, int(PieceSize)-len(p0))...)); err != nil {
		t.Fatalf("fill Put(0): %v", err)
	}
	if err := st.Put("a.bin", 1, make([]byte, PieceSize)); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	sealTestFile(t, st, "a.bin", 2*PieceSize+int64(len(p2)))
	final, err := os.ReadFile(filepath.Join(dir, "a.bin"))
	if err != nil {
		t.Fatalf("read final: %v", err)
	}
	if !bytes.Equal(final[:100], p0) || !bytes.Equal(final[2*PieceSize:], p2) {
		t.Fatal("final content mismatch at written pieces")
	}
	for _, i := range []int{100, int(PieceSize)} {
		if final[i] != 0 {
			t.Fatalf("hole byte at %d = %d, want 0", i, final[i])
		}
	}
}

// TestSparsePieceStoreCrashRecovery verifies that reopening an in-progress file
// rebuilds the existence map from the index (a crash mid-download resumes).
func TestSparsePieceStoreCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sparse")
	st := newSparseTestStore(t, dir)

	p0 := make([]byte, PieceSize)
	p1 := make([]byte, PieceSize)
	p2 := make([]byte, 100)
	for i := range p0 {
		p0[i] = byte(i)
	}
	for i := range p1 {
		p1[i] = byte(i + 1)
	}
	for i := range p2 {
		p2[i] = byte(i + 2)
	}
	total := 2*PieceSize + 100
	// Write piece 0 and piece 2 (out of order) then "crash": the first store
	// releases its handles/bbolt lock, simulating process death releasing OS
	// locks.
	if err := st.Put("a.bin", 0, p0); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := st.Put("a.bin", 2, p2); err != nil {
		t.Fatalf("Put(2): %v", err)
	}
	if sc, ok := st.(*sparsePieceStore); ok {
		_ = sc.Close()
	}

	// A fresh store instance on the same dir simulates a restart.
	st2 := newSparseTestStore(t, dir)
	if !st2.HasPiece("a.bin", 0) || !st2.HasPiece("a.bin", 2) {
		t.Fatal("reopen did not restore written pieces")
	}
	if st2.HasPiece("a.bin", 1) {
		t.Fatal("reopen reports an unwritten piece")
	}
	if st2.PieceCount("a.bin") != 2 {
		t.Fatalf("PieceCount after reopen = %d, want 2", st2.PieceCount("a.bin"))
	}
	got, err := st2.Get("a.bin", 0)
	if err != nil || !bytes.Equal(got, p0) {
		t.Fatalf("Get(0) after reopen = %q, %v; want p0", got, err)
	}

	// Resume: write the missing piece and complete.
	if err := st2.Put("a.bin", 1, p1); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	sealTestFile(t, st2, "a.bin", total)
	if !st2.IsComplete("a.bin") {
		t.Fatal("file not complete after resume")
	}
	got2, err := st2.Get("a.bin", 2)
	if err != nil || !bytes.Equal(got2, p2) {
		t.Fatalf("Get(2) after complete = %q, %v; want p2", got2, err)
	}
}

// TestSparsePieceStoreConcurrent exercises concurrent writes and reads (the
// single-mutex discipline and the index round-trip under the race detector).
func TestSparsePieceStoreConcurrent(t *testing.T) {
	st := newSparseTestStore(t, "")
	const pieces = 32
	var wg sync.WaitGroup
	for i := 0; i < pieces; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := make([]byte, 64)
			for j := range data {
				data[j] = byte(i)
			}
			if err := st.Put("f.bin", int64(i), data); err != nil {
				t.Errorf("Put(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if st.PieceCount("f.bin") != pieces {
		t.Fatalf("PieceCount = %d, want %d", st.PieceCount("f.bin"), pieces)
	}
	var rwg sync.WaitGroup
	for i := 0; i < pieces; i++ {
		rwg.Add(1)
		go func(i int) {
			defer rwg.Done()
			got, err := st.Get("f.bin", int64(i))
			if err != nil {
				t.Errorf("Get(%d): %v", i, err)
				return
			}
			if len(got) != 64 || got[0] != byte(i) {
				t.Errorf("Get(%d) content mismatch", i)
			}
		}(i)
	}
	rwg.Wait()
}

// TestSparseStoreMetadataSidecar verifies the .cds.metadata sidecar:
// write/read/delete, and the reserved-marker safety (a user basename can never
// collide with a sidecar path).
func TestSparseStoreMetadataSidecar(t *testing.T) {
	st := newSparseTestStore(t, "").(*sparsePieceStore)

	if _, ok, err := st.ReadMetadata("a.bin"); err != nil || ok {
		t.Fatalf("ReadMetadata(absent) = (%v, %v), want (nil, false)", ok, err)
	}
	data := []byte("canonical-metadata-bytes")
	if err := st.WriteMetadata("a.bin", data); err != nil {
		t.Fatalf("WriteMetadata: %v", err)
	}
	got, ok, err := st.ReadMetadata("a.bin")
	if err != nil || !ok || !bytes.Equal(got, data) {
		t.Fatalf("ReadMetadata = (%q, %v, %v), want the written bytes", got, ok, err)
	}
	if err := st.DeleteMetadata("a.bin"); err != nil {
		t.Fatalf("DeleteMetadata: %v", err)
	}
	if _, ok, err := st.ReadMetadata("a.bin"); err != nil || ok {
		t.Fatalf("ReadMetadata after delete = (%v, %v), want (false, nil)", ok, err)
	}

	// Unsafe names are rejected.
	if err := st.WriteMetadata("../evil", data); err == nil {
		t.Fatal("WriteMetadata accepted an unsafe name")
	}
}

// TestSparseStoreDeleteCleansSidecar verifies the P2 fix: Delete removes the
// .cds.metadata sidecar along with the pieces/index/final artifact.
func TestSparseStoreDeleteCleansSidecar(t *testing.T) {
	st := newSparseTestStore(t, "").(*sparsePieceStore)
	if err := st.Put("a.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	sealTestFile(t, st, "a.bin", 1)
	if _, ok, err := st.ReadMetadata("a.bin"); err != nil || !ok {
		t.Fatalf("ReadMetadata before delete = (%v, %v), want present", ok, err)
	}
	if err := st.Delete("a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, err := st.ReadMetadata("a.bin"); err != nil || ok {
		t.Fatalf("ReadMetadata after Delete = (%v, %v), want (false, nil)", ok, err)
	}
}

// sealTestFile atomically publishes a file with a C2-style self-built
// metadata: it reads the stored pieces, computes the SHA-256 piece digests +
// the file digest, builds the FileMetadataV1, and Seals the artifact.
func sealTestFile(t *testing.T, st PieceStore, filename string, size int64) {
	t.Helper()
	pieceCount := int((size + PieceSize - 1) / PieceSize)
	digests := make([][]byte, pieceCount)
	fileHash := sha256.New()
	for i := 0; i < pieceCount; i++ {
		data, err := st.Get(filename, int64(i))
		if err != nil {
			t.Fatalf("sealTestFile Get(%d): %v", i, err)
		}
		h := sha256.Sum256(data)
		digests[i] = h[:]
		fileHash.Write(data)
	}
	m, err := BuildMetadata(filename, size, PieceSize, digests, fileHash.Sum(nil))
	if err != nil {
		t.Fatalf("sealTestFile BuildMetadata: %v", err)
	}
	metaBytes, err := m.Encode()
	if err != nil {
		t.Fatalf("sealTestFile Encode: %v", err)
	}
	if err := st.Seal(filename, size, metaBytes); err != nil {
		t.Fatalf("sealTestFile Seal: %v", err)
	}
}

func TestSparseStoreSealValidatesMetadataAndData(t *testing.T) {
	content := []byte("artifact bytes")
	newStore := func(t *testing.T) PieceStore {
		t.Helper()
		st, err := NewPieceStore(t.TempDir())
		if err != nil {
			t.Fatalf("NewPieceStore: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		if err := st.Put("a.bin", 0, content); err != nil {
			t.Fatalf("Put: %v", err)
		}
		return st
	}
	metadata := func(t *testing.T, filename string, fileDigest, pieceDigest []byte) []byte {
		t.Helper()
		m, err := BuildMetadata(filename, int64(len(content)), PieceSize, [][]byte{pieceDigest}, fileDigest)
		if err != nil {
			t.Fatalf("BuildMetadata: %v", err)
		}
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return b
	}
	good := sha256.Sum256(content)
	bad := sha256.Sum256([]byte("different"))

	if err := newStore(t).Seal("a.bin", int64(len(content)), metadata(t, "other.bin", good[:], good[:])); err == nil {
		t.Fatal("Seal accepted metadata for another filename")
	}
	if err := newStore(t).Seal("a.bin", int64(len(content)), metadata(t, "a.bin", good[:], bad[:])); !errors.Is(err, errPieceDigestMismatch) {
		t.Fatalf("Seal piece mismatch error = %v, want errPieceDigestMismatch", err)
	}
	if err := newStore(t).Seal("a.bin", int64(len(content)), metadata(t, "a.bin", bad[:], good[:])); err == nil {
		t.Fatal("Seal accepted an incorrect whole-file digest")
	}
}

func TestSparseStoreSealIdempotenceRejectsDifferentMetadata(t *testing.T) {
	content := []byte("artifact bytes")
	st := newSparseTestStore(t, "")
	if err := st.Put("a.bin", 0, content); err != nil {
		t.Fatalf("Put: %v", err)
	}
	sealTestFile(t, st, "a.bin", int64(len(content)))
	if err := st.Seal("a.bin", int64(len(content)), []byte("not metadata")); err == nil {
		t.Fatal("idempotent Seal accepted invalid metadata")
	}

	digest := sha256.Sum256(content)
	other, err := BuildMetadata("a.bin", int64(len(content)), PieceSize, [][]byte{digest[:]}, bytes.Repeat([]byte{0xff}, MetadataDigestSize))
	if err != nil {
		t.Fatalf("BuildMetadata: %v", err)
	}
	otherBytes, err := other.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if err := st.Seal("a.bin", int64(len(content)), otherBytes); !errors.Is(err, errContentConflict) {
		t.Fatalf("Seal conflict error = %v, want errContentConflict", err)
	}
}

// TestSparseStoreSealThreePiece verifies Seal publishes the three-piece
// artifact (final + .cds.metadata + .cds.commit) and IsComplete requires all
// three with a consistent metadata_id.
func TestSparseStoreSealThreePiece(t *testing.T) {
	dir := t.TempDir()
	st, err := NewFilePieceStore(dir)
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	p0 := bytes.Repeat([]byte("a"), int(PieceSize))
	p1 := bytes.Repeat([]byte("b"), 10)
	if err := st.Put("a.bin", 0, p0); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := st.Put("a.bin", 1, p1); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	sealTestFile(t, st, "a.bin", int64(len(p0)+len(p1)))

	sp := st.(*sparsePieceStore)
	finalPath, metadataPath, commitPath := artifactPaths(sp.dir, "a.bin")
	for _, p := range []string{finalPath, metadataPath, commitPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("missing sealed artifact %s: %v", p, err)
		}
	}
	if !st.IsComplete("a.bin") {
		t.Fatal("sealed artifact not complete")
	}
	// The commit's metadata_id must equal SHA-256 of the local metadata.
	meta, _, err := sp.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	commitData, err := os.ReadFile(commitPath)
	if err != nil {
		t.Fatalf("ReadFile commit: %v", err)
	}
	commitID, err := DecodeCommit(commitData)
	if err != nil {
		t.Fatalf("DecodeCommit: %v", err)
	}
	if !bytes.Equal(commitID, MetadataID(meta)) {
		t.Fatal("commit metadata_id != SHA-256(local metadata)")
	}
	// A fresh store over the same directory still sees it sealed.
	st2, err := NewFilePieceStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st2.Close() })
	if !st2.IsComplete("a.bin") {
		t.Fatal("reopened store does not see the sealed artifact")
	}
	got, err := st2.Get("a.bin", 0)
	if err != nil || !bytes.Equal(got, p0) {
		t.Fatalf("reopened Get(0) = %q, %v; want p0", got, err)
	}
	// Delete cleans the whole three-piece set.
	if err := st2.Delete("a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	for _, p := range []string{finalPath, metadataPath, commitPath} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("Delete left %s (err=%v)", p, err)
		}
	}
}

// TestSparseStoreSealCrashRecovery injects crashes at each point of Seal and
// asserts only the fully-consistent three-piece state is complete; every
// partial state is NOT complete (never served), exactly per design §9.
func TestSparseStoreSealCrashRecovery(t *testing.T) {
	content := [][]byte{bytes.Repeat([]byte("a"), int(PieceSize)), bytes.Repeat([]byte("b"), 10)}
	size := int64(len(content[0]) + len(content[1]))
	metaBytes := func(t *testing.T) []byte {
		t.Helper()
		d0 := sha256.Sum256(content[0])
		d1 := sha256.Sum256(content[1])
		fileHash := sha256.New()
		fileHash.Write(content[0])
		fileHash.Write(content[1])
		m, err := BuildMetadata("a.bin", size, PieceSize, [][]byte{d0[:], d1[:]}, fileHash.Sum(nil))
		if err != nil {
			t.Fatalf("BuildMetadata: %v", err)
		}
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return b
	}(t)

	mkStore := func(t *testing.T) (*sparsePieceStore, string) {
		t.Helper()
		dir := t.TempDir()
		st, err := NewFilePieceStore(dir)
		if err != nil {
			t.Fatalf("NewFilePieceStore: %v", err)
		}
		t.Cleanup(func() { _ = st.Close() })
		if err := st.Put("a.bin", 0, content[0]); err != nil {
			t.Fatalf("Put(0): %v", err)
		}
		if err := st.Put("a.bin", 1, content[1]); err != nil {
			t.Fatalf("Put(1): %v", err)
		}
		return st.(*sparsePieceStore), dir
	}

	// State 1: data only (pieces + index). Incomplete, resumable.
	st1, _ := mkStore(t)
	if st1.IsComplete("a.bin") {
		t.Fatal("data-only state reported complete")
	}
	if data, err := st1.Get("a.bin", 0); err != nil || !bytes.Equal(data, content[0]) {
		t.Fatalf("data-only Get(0) = %q, %v; want resume-able piece", data, err)
	}

	// State 2: data + metadata (no commit, no rename). Incomplete.
	st2, _ := mkStore(t)
	_, metadataPath, _ := artifactPaths(st2.dir, "a.bin")
	if err := os.WriteFile(metadataPath, metaBytes, 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if st2.IsComplete("a.bin") {
		t.Fatal("data+metadata (no commit) reported complete")
	}

	// State 3: renamed data + metadata, no commit. Incomplete (never served).
	st3, _ := mkStore(t)
	piecesPath, _, finalPath := sparseFilePaths(st3.dir, "a.bin")
	_, metadataPath3, _ := artifactPaths(st3.dir, "a.bin")
	if err := os.Rename(piecesPath, finalPath); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := os.WriteFile(metadataPath3, metaBytes, 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if st3.IsComplete("a.bin") {
		t.Fatal("renamed-no-commit state reported complete")
	}

	// State 4: commit present but its metadata_id does NOT match the sidecar.
	// Incomplete (consistency check rejects it).
	st4, _ := mkStore(t)
	_, metadataPath4, commitPath4 := artifactPaths(st4.dir, "a.bin")
	if err := os.WriteFile(metadataPath4, metaBytes, 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	piecesPath4, _, finalPath4 := sparseFilePaths(st4.dir, "a.bin")
	if err := os.Rename(piecesPath4, finalPath4); err != nil {
		t.Fatalf("rename: %v", err)
	}
	wrongCommit, err := EncodeCommit(make([]byte, MetadataDigestSize))
	if err != nil {
		t.Fatalf("EncodeCommit: %v", err)
	}
	if err := os.WriteFile(commitPath4, wrongCommit, 0o644); err != nil {
		t.Fatalf("write commit: %v", err)
	}
	if st4.IsComplete("a.bin") {
		t.Fatal("mismatched-commit state reported complete")
	}

	// State 5: all three consistent -> complete.
	st5, dir5 := mkStore(t)
	sealTestFile(t, st5, "a.bin", size)
	if !st5.IsComplete("a.bin") {
		t.Fatal("fully sealed artifact not complete")
	}
	st5b, err := NewFilePieceStore(dir5)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = st5b.Close() })
	if !st5b.IsComplete("a.bin") {
		t.Fatal("reopened sealed artifact not complete")
	}
}

// TestSparseStoreRecoverInterruptedSeal locks the C2.5 P1 fix: a crash
// between the rename and the commit-marker write is recognized on the next
// open.
//   - final size matches the metadata -> the seal is COMPLETED (commit
//     written), the data is preserved and a later re-Seal keeps it;
//   - final size mismatches -> the stale index/metadata/final are cleaned so
//     the re-download path starts fresh (HasPiece must NOT trust a stale
//     index over an empty pieces file, and a re-Seal must not destroy data).
func TestSparseStoreRecoverInterruptedSeal(t *testing.T) {
	content := [][]byte{bytes.Repeat([]byte("a"), int(PieceSize)), bytes.Repeat([]byte("b"), 10)}
	size := int64(len(content[0]) + len(content[1]))
	metaBytes := func(t *testing.T) []byte {
		t.Helper()
		d0 := sha256.Sum256(content[0])
		d1 := sha256.Sum256(content[1])
		fileHash := sha256.New()
		fileHash.Write(content[0])
		fileHash.Write(content[1])
		m, err := BuildMetadata("a.bin", size, PieceSize, [][]byte{d0[:], d1[:]}, fileHash.Sum(nil))
		if err != nil {
			t.Fatalf("BuildMetadata: %v", err)
		}
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		return b
	}(t)

	// Case A: crash after rename before commit (size matches) -> completed.
	dirA := t.TempDir()
	stA, err := NewFilePieceStore(dirA)
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	if err := stA.Put("a.bin", 0, content[0]); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := stA.Put("a.bin", 1, content[1]); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	spA := stA.(*sparsePieceStore)
	piecesA, _, finalA := sparseFilePaths(spA.dir, "a.bin")
	_, metadataA, commitA := artifactPaths(spA.dir, "a.bin")
	if err := os.Rename(piecesA, finalA); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := os.WriteFile(metadataA, metaBytes, 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if err := stA.Close(); err != nil {
		t.Fatalf("close stA: %v", err)
	}

	stA2, err := NewFilePieceStore(dirA)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = stA2.Close() })
	if !stA2.IsComplete("a.bin") {
		t.Fatal("interrupted seal not completed on reopen")
	}
	got0, err := stA2.Get("a.bin", 0)
	got1, err2 := stA2.Get("a.bin", 1)
	if err != nil || err2 != nil || !bytes.Equal(got0, content[0]) || !bytes.Equal(got1, content[1]) {
		t.Fatalf("data lost in interrupted-seal recovery: (%q,%v) (%q,%v)", got0, err, got1, err2)
	}
	commitData, err := os.ReadFile(commitA)
	if err != nil {
		t.Fatalf("read recovered commit: %v", err)
	}
	commitID, err := DecodeCommit(commitData)
	if err != nil || !bytes.Equal(commitID, MetadataID(metaBytes)) {
		t.Fatalf("recovered commit metadata_id mismatch: %v", err)
	}
	// Re-Seal is idempotent + keeps the content.
	if err := spA2Seal(stA2, size, metaBytes); err != nil {
		t.Fatalf("re-Seal: %v", err)
	}
	if got0, _ := stA2.Get("a.bin", 0); !bytes.Equal(got0, content[0]) {
		t.Fatal("data changed after re-Seal")
	}

	// Case B: crash with a size-mismatched final -> stale staging is cleaned;
	// the re-download path starts fresh (no empty-file over stale-index trap).
	dirB := t.TempDir()
	stB, err := NewFilePieceStore(dirB)
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	if err := stB.Put("a.bin", 0, content[0]); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := stB.Put("a.bin", 1, content[1]); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	spB := stB.(*sparsePieceStore)
	piecesB, _, finalB := sparseFilePaths(spB.dir, "a.bin")
	_, metadataB, _ := artifactPaths(spB.dir, "a.bin")
	if err := os.Rename(piecesB, finalB); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if err := os.WriteFile(metadataB, metaBytes, 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	if err := os.Truncate(finalB, 1); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := stB.Close(); err != nil {
		t.Fatalf("close stB: %v", err)
	}

	stB2, err := NewFilePieceStore(dirB)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = stB2.Close() })
	if stB2.IsComplete("a.bin") {
		t.Fatal("size-mismatch state reported complete")
	}
	for _, p := range []string{finalB, metadataB} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("stale artifact %s not cleaned (err=%v)", p, err)
		}
	}
	spB2 := stB2.(*sparsePieceStore)
	if spB2.HasPiece("a.bin", 0) {
		t.Fatal("stale index trusted after size-mismatch recovery")
	}
	// A fresh re-download + Seal works and preserves the content.
	if err := stB2.Put("a.bin", 0, content[0]); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := stB2.Put("a.bin", 1, content[1]); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	sealTestFile(t, stB2, "a.bin", size)
	if !stB2.IsComplete("a.bin") {
		t.Fatal("re-download + Seal failed")
	}
}

func spA2Seal(st PieceStore, size int64, metaBytes []byte) error {
	return st.(*sparsePieceStore).Seal("a.bin", size, metaBytes)
}
