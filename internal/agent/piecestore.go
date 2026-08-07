package agent

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

// ErrPieceNotFound is returned by PieceStore.Get for a missing piece.
var ErrPieceNotFound = errors.New("agent: piece not found")

// validBasename reports whether name is a safe, tree-agnostic file basename:
// non-empty, not "." or "..", free of path separators (so it cannot escape the
// download path), and not containing the reserved ".cds." marker. The marker is
// used by the sparse store for internal files (<basename>.cds.pieces and
// <basename>.cds.index), so a file literally named "foo.cds.pieces" would
// collide with the pieces path of "foo"; such names are rejected. The store
// keys every file by its basename.
func validBasename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	if strings.Contains(name, ".cds.") {
		return false
	}
	return true
}

// PieceStore stores and retrieves file pieces locally, keyed by the file
// basename. The store is deliberately tree-agnostic: tree identity is a
// control-plane concept and never appears on disk (the download path is
// <DownloadPath>/<basename>). The single implementation is a sparse data file
// plus a bbolt index (see piecestore_sparse.go).
type PieceStore interface {
	// Put stores a piece.
	Put(filename string, index int64, data []byte) error
	// Get returns a piece or ErrPieceNotFound.
	Get(filename string, index int64) ([]byte, error)
	// HasPiece reports whether the piece exists.
	HasPiece(filename string, index int64) bool
	// Seal atomically publishes the completed artifact: data + the immutable
	// metadata sidecar + the .cds.commit marker (written last). Until Seal
	// succeeds the artifact is not served; a partial Seal is recovered as
	// incomplete on the next open.
	Seal(filename string, size int64, metadataBytes []byte) error
	// IsComplete reports whether the file is fully downloaded.
	IsComplete(filename string) bool
	// ReadMetadata returns the sealed metadata sidecar bytes (ok=false when
	// absent).
	ReadMetadata(filename string) (data []byte, ok bool, err error)
	// Size returns the recorded file size (0 when unknown).
	Size(filename string) int64
	// PieceCount returns how many pieces are stored.
	PieceCount(filename string) int
	// Delete removes every piece and the completion marker of a file.
	Delete(filename string) error
	// Close releases any underlying resources (handles). Call at agent
	// shutdown.
	Close() error
}

// NewPieceStore creates the unified sparse-file piece store rooted at dir.
func NewPieceStore(dir string) (PieceStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("agent: create piece dir: %w", err)
	}
	return &sparsePieceStore{dir: dir, open: make(map[string]*sparseFile)}, nil
}

// NewFilePieceStore is a deprecated alias: the unified sparse store is the
// single implementation (the -store file|mmap flags are accepted for
// compatibility and map here).
func NewFilePieceStore(dir string) (PieceStore, error) { return NewPieceStore(dir) }

// NewMmapPieceStore is a deprecated alias: the unified sparse store is the
// single implementation (the -store file|mmap flags are accepted for
// compatibility and map here).
func NewMmapPieceStore(dir string) (PieceStore, error) { return NewPieceStore(dir) }
