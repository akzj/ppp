package agent

import (
	"bytes"
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
// store and that MarkComplete renames the pieces file to <basename>.
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
	if err := st.MarkComplete("a.bin", 10); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if !st.IsComplete("a.bin") {
		t.Fatal("IsComplete = false after MarkComplete")
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
		t.Fatalf("pieces file still exists after MarkComplete (err=%v)", err)
	}
	if _, err := os.Stat(sc.dir + "/a.bin.cds.index"); !os.IsNotExist(err) {
		t.Fatalf("index file still exists after MarkComplete (err=%v)", err)
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
	if err := st.Put("a.bin", 1, make([]byte, PieceSize)); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	if err := st.MarkComplete("a.bin", 2*PieceSize+int64(len(p2))); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
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
	if err := st2.MarkComplete("a.bin", total); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
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
