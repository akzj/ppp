package agent

import (
	"bytes"
	"errors"
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

	if _, err := st.Get("a.bin", 0); !errors.Is(err, ErrPieceNotFound) {
		t.Fatalf("Get(missing) = %v, want ErrPieceNotFound", err)
	}

	data := []byte("hello piece")
	if err := st.Put("a.bin", 0, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := st.Get("a.bin", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
	if !st.HasPiece("a.bin", 0) {
		t.Fatal("HasPiece = false after Put")
	}
	if st.HasPiece("a.bin", 1) {
		t.Fatal("HasPiece(1) = true, want false")
	}
	if st.PieceCount("a.bin") != 1 {
		t.Fatalf("PieceCount = %d, want 1", st.PieceCount("a.bin"))
	}
}

// TestPieceStoreCompleteMarker verifies MarkComplete/IsComplete/Size/Delete.
func TestPieceStoreCompleteMarker(t *testing.T) {
	st := newTestStore(t)
	if st.IsComplete("a.bin") {
		t.Fatal("IsComplete before download = true")
	}
	if err := st.Put("a.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	sealTestFile(t, st, "a.bin", 100)
	if !st.IsComplete("a.bin") {
		t.Fatal("IsComplete after Seal = false")
	}
	if st.Size("a.bin") != 100 {
		t.Fatalf("Size = %d, want 100", st.Size("a.bin"))
	}
	if err := st.Delete("a.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if st.IsComplete("a.bin") || st.HasPiece("a.bin", 0) {
		t.Fatal("file state survives Delete")
	}
}

// TestPieceStorePathTraversal verifies hostile names cannot escape the
// download path.
// TestValidBasename verifies the basename rule: unsafe names (path
// separators, ".", "..", empty) and the reserved ".cds." marker are rejected.
func TestValidBasename(t *testing.T) {
	bad := []string{"", ".", "..", "a/b", "a\\b", "/abs", "abs/", "dir/file.bin",
		"foo.cds.pieces", "foo.cds.index", "a.cds.b"}
	for _, name := range bad {
		if validBasename(name) {
			t.Errorf("validBasename(%q) = true, want false", name)
		}
	}
	good := []string{"a.bin", "app.tar", "file", "with spaces.bin", "a-b_c.txt", "foo.cdspieces"}
	for _, name := range good {
		if !validBasename(name) {
			t.Errorf("validBasename(%q) = false, want true", name)
		}
	}
}

// TestPieceStoreRejectsUnsafeNames verifies unsafe filenames cannot reach the
// store (defense in depth beyond the data-plane entry validation).
func TestPieceStoreRejectsUnsafeNames(t *testing.T) {
	for _, st := range []PieceStore{mustFileStore(t), mustMmapStore(t)} {
		for _, evil := range []string{"../../../../etc/passwd", "foo.cds.pieces", "foo.cds.index"} {
			if err := st.Put(evil, 0, []byte("x")); err == nil {
				t.Fatalf("Put(%q) = nil, want rejection", evil)
			}
			if st.HasPiece(evil, 0) {
				t.Fatalf("HasPiece(%q) = true, want false", evil)
			}
			if st.IsComplete(evil) {
				t.Fatalf("IsComplete(%q) = true, want false", evil)
			}
			if _, err := st.Get(evil, 0); err == nil {
				t.Fatalf("Get(%q) = nil error, want rejection", evil)
			}
		}
	}
}

func mustFileStore(t *testing.T) PieceStore {
	t.Helper()
	st, err := NewFilePieceStore(filepath.Join(t.TempDir(), "pieces"))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	return st
}

func mustMmapStore(t *testing.T) PieceStore {
	t.Helper()
	st, err := NewMmapPieceStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewMmapPieceStore: %v", err)
	}
	return st
}
