package agent

import (
	"bytes"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func newMmapTestStore(t *testing.T, dir string) PieceStore {
	t.Helper()
	if dir == "" {
		dir = filepath.Join(t.TempDir(), "mmap")
	}
	st, err := NewMmapPieceStore(dir)
	if err != nil {
		t.Fatalf("NewMmapPieceStore: %v", err)
	}
	t.Cleanup(func() {
		if mc, ok := st.(*mmapPieceStore); ok {
			_ = mc.Close()
		}
	})
	return st
}

// TestMmapPieceStoreRoundtrip verifies put/get/complete/delete on the mmap
// store.
func TestMmapPieceStoreRoundtrip(t *testing.T) {
	st := newMmapTestStore(t, "")

	if _, err := st.Get("t1", "a.bin", 0); !errors.Is(err, ErrPieceNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrPieceNotFound", err)
	}

	p0 := make([]byte, 10)
	copy(p0, "0123456789")
	if err := st.Put("t1", "a.bin", 0, p0); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := st.Get("t1", "a.bin", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, p0) {
		t.Fatalf("Get = %q, want %q", got, p0)
	}
	if !st.HasPiece("t1", "a.bin", 0) {
		t.Fatal("HasPiece(0) = false after Put")
	}
	if st.HasPiece("t1", "a.bin", 1) {
		t.Fatal("HasPiece(1) = true, want false")
	}
	if st.PieceCount("t1", "a.bin") != 1 {
		t.Fatalf("PieceCount = %d, want 1", st.PieceCount("t1", "a.bin"))
	}

	// Complete: size becomes authoritative, all pieces readable from the
	// final file.
	if err := st.MarkComplete("t1", "a.bin", 10); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if !st.IsComplete("t1", "a.bin") {
		t.Fatal("IsComplete = false after MarkComplete")
	}
	if st.Size("t1", "a.bin") != 10 {
		t.Fatalf("Size = %d, want 10", st.Size("t1", "a.bin"))
	}
	got, err = st.Get("t1", "a.bin", 0)
	if err != nil {
		t.Fatalf("Get after complete: %v", err)
	}
	if !bytes.Equal(got, p0) {
		t.Fatalf("Get after complete = %q, want %q", got, p0)
	}

	if err := st.Delete("t1", "a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if st.IsComplete("t1", "a.bin") || st.HasPiece("t1", "a.bin", 0) {
		t.Fatal("file state survives Delete")
	}
}

// TestMmapPieceStoreCrashRecovery verifies that reopening an in-progress file
// rebuilds the existence map from the index (a crash mid-download resumes).
func TestMmapPieceStoreCrashRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "mmap")
	st := newMmapTestStore(t, dir)

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
	// Write piece 0 and piece 2 (out of order) then "crash" (no graceful
	// close of the first store; its files persist).
	if err := st.Put("t1", "a.bin", 0, p0); err != nil {
		t.Fatalf("Put(0): %v", err)
	}
	if err := st.Put("t1", "a.bin", 2, p2); err != nil {
		t.Fatalf("Put(2): %v", err)
	}
	// Release the first store's file handles and bbolt lock (a real crash
	// would release OS locks; a live second store on the same bbolt path
	// would otherwise block forever).
	if mc, ok := st.(*mmapPieceStore); ok {
		_ = mc.Close()
	}

	// A fresh store instance on the same dir simulates a restart.
	st2 := newMmapTestStore(t, dir)
	if st2.HasPiece("t1", "a.bin", 0) != true || st2.HasPiece("t1", "a.bin", 2) != true {
		t.Fatal("reopen did not restore written pieces")
	}
	if st2.HasPiece("t1", "a.bin", 1) {
		t.Fatal("reopen reports an unwritten piece")
	}
	if st2.PieceCount("t1", "a.bin") != 2 {
		t.Fatalf("PieceCount after reopen = %d, want 2", st2.PieceCount("t1", "a.bin"))
	}
	got, err := st2.Get("t1", "a.bin", 0)
	if err != nil || !bytes.Equal(got, p0) {
		t.Fatalf("Get(0) after reopen = %q, %v; want %q", got, err, p0)
	}
	// Resume: write the missing piece and complete.
	if err := st2.Put("t1", "a.bin", 1, p1); err != nil {
		t.Fatalf("Put(1): %v", err)
	}
	if err := st2.MarkComplete("t1", "a.bin", total); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if !st2.IsComplete("t1", "a.bin") {
		t.Fatal("file not complete after resume")
	}
	got2, err := st2.Get("t1", "a.bin", 2)
	if err != nil || !bytes.Equal(got2, p2) {
		t.Fatalf("Get(2) after complete = %q, %v; want %q", got2, err, p2)
	}
}

// TestMmapPieceStoreConcurrent exercises concurrent writes and reads (race
// detector validates the single-mutex discipline and Get's copy semantics).
func TestMmapPieceStoreConcurrent(t *testing.T) {
	st := newMmapTestStore(t, "")
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
			if err := st.Put("t1", "f.bin", int64(i), data); err != nil {
				t.Errorf("Put(%d): %v", i, err)
			}
		}(i)
	}
	wg.Wait()
	if st.PieceCount("t1", "f.bin") != pieces {
		t.Fatalf("PieceCount = %d, want %d", st.PieceCount("t1", "f.bin"), pieces)
	}
	var rwg sync.WaitGroup
	for i := 0; i < pieces; i++ {
		rwg.Add(1)
		go func(i int) {
			defer rwg.Done()
			got, err := st.Get("t1", "f.bin", int64(i))
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
