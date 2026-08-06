package agent

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestStore(t *testing.T) PieceStore {
	t.Helper()
	st, err := NewFilePieceStore(filepath.Join(t.TempDir(), "pieces"))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	return st
}

// TestPieceStorePutGet verifies write/read round trips and missing pieces.
func TestPieceStorePutGet(t *testing.T) {
	st := newTestStore(t)

	if _, err := st.Get("t1", "a.bin", 0); !errors.Is(err, ErrPieceNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrPieceNotFound", err)
	}

	data := []byte("hello piece")
	if err := st.Put("t1", "a.bin", 0, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := st.Get("t1", "a.bin", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
	if !st.HasPiece("t1", "a.bin", 0) {
		t.Fatal("HasPiece = false after Put")
	}
	if st.HasPiece("t1", "a.bin", 1) {
		t.Fatal("HasPiece(1) = true, want false")
	}
	if st.PieceCount("t1", "a.bin") != 1 {
		t.Fatalf("PieceCount = %d, want 1", st.PieceCount("t1", "a.bin"))
	}
}

// TestPieceStoreCompleteMarker verifies MarkComplete/IsComplete/Size/Delete.
func TestPieceStoreCompleteMarker(t *testing.T) {
	st := newTestStore(t)
	if st.IsComplete("t1", "a.bin") {
		t.Fatal("IsComplete before download = true")
	}
	if err := st.Put("t1", "a.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.MarkComplete("t1", "a.bin", 100); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	if !st.IsComplete("t1", "a.bin") {
		t.Fatal("IsComplete after MarkComplete = false")
	}
	if st.Size("t1", "a.bin") != 100 {
		t.Fatalf("Size = %d, want 100", st.Size("t1", "a.bin"))
	}
	if err := st.Delete("t1", "a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if st.IsComplete("t1", "a.bin") || st.HasPiece("t1", "a.bin", 0) {
		t.Fatal("file state survives Delete")
	}
}

// TestPieceStorePathTraversal verifies hostile names cannot escape the
// download path.
func TestPieceStorePathTraversal(t *testing.T) {
	root := filepath.Join(t.TempDir(), "pieces")
	st, err := NewFilePieceStore(root)
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	evil := "../../../../etc/passwd"
	if err := st.Put("t1", evil, 0, []byte("x")); err != nil {
		t.Fatalf("Put(evil name): %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "..", "etc", "passwd")); err == nil {
		t.Fatal("piece escaped the download path")
	}
	got, err := st.Get("t1", evil, 0)
	if err != nil || string(got) != "x" {
		t.Fatalf("Get(evil name) = %q, %v; want x", got, err)
	}
	// Ensure the piece lives inside root.
	found := false
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && filepath.Ext(path) == ".piece" && info.Size() == 1 {
			found = true
		}
		return nil
	})
	if !found {
		t.Fatal("piece not found inside the download path")
	}
}
