package agent

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ErrPieceNotFound is returned by PieceStore.Get for a missing piece.
var ErrPieceNotFound = errors.New("agent: piece not found")

// PieceStore stores and retrieves file pieces locally. The interface isolates
// the storage engine: phase 4 swaps the file implementation for mmap+bbolt.
type PieceStore interface {
	// Put stores a piece.
	Put(treeID, filename string, index int64, data []byte) error
	// Get returns a piece or ErrPieceNotFound.
	Get(treeID, filename string, index int64) ([]byte, error)
	// HasPiece reports whether the piece exists.
	HasPiece(treeID, filename string, index int64) bool
	// MarkComplete records the file as fully downloaded with its total size.
	MarkComplete(treeID, filename string, size int64) error
	// IsComplete reports whether the file is fully downloaded.
	IsComplete(treeID, filename string) bool
	// Size returns the recorded file size (0 when unknown).
	Size(treeID, filename string) int64
	// PieceCount returns how many pieces are stored.
	PieceCount(treeID, filename string) int
	// Delete removes every piece and the completion marker of a file.
	Delete(treeID, filename string) error
}

// filePieceStore is a simple on-disk PieceStore: pieces live as
// <dir>/<hex(tree\x00file)>/<index>.piece plus a meta file holding the total
// size once the file is complete. Tree and file names are hex-encoded so
// untrusted names cannot escape the download path.
type filePieceStore struct {
	dir string
}

// NewFilePieceStore creates the piece store rooted at dir.
func NewFilePieceStore(dir string) (PieceStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("agent: create piece dir: %w", err)
	}
	return &filePieceStore{dir: dir}, nil
}

func (s *filePieceStore) fileDir(treeID, filename string) string {
	key := hex.EncodeToString([]byte(treeID + "\x00" + filename))
	return filepath.Join(s.dir, key)
}

func (s *filePieceStore) piecePath(treeID, filename string, index int64) string {
	return filepath.Join(s.fileDir(treeID, filename), strconv.FormatInt(index, 10)+".piece")
}

func (s *filePieceStore) metaPath(treeID, filename string) string {
	return filepath.Join(s.fileDir(treeID, filename), "meta")
}

func (s *filePieceStore) Put(treeID, filename string, index int64, data []byte) error {
	dir := s.fileDir(treeID, filename)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	// Write to a temp file and rename so a crash cannot leave a torn piece.
	tmp := s.piecePath(treeID, filename, index) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.piecePath(treeID, filename, index))
}

func (s *filePieceStore) Get(treeID, filename string, index int64) ([]byte, error) {
	data, err := os.ReadFile(s.piecePath(treeID, filename, index))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPieceNotFound
		}
		return nil, err
	}
	return data, nil
}

func (s *filePieceStore) HasPiece(treeID, filename string, index int64) bool {
	_, err := os.Stat(s.piecePath(treeID, filename, index))
	return err == nil
}

func (s *filePieceStore) MarkComplete(treeID, filename string, size int64) error {
	dir := s.fileDir(treeID, filename)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(s.metaPath(treeID, filename), []byte(strconv.FormatInt(size, 10)), 0o644)
}

func (s *filePieceStore) IsComplete(treeID, filename string) bool {
	_, err := os.Stat(s.metaPath(treeID, filename))
	return err == nil
}

func (s *filePieceStore) Size(treeID, filename string) int64 {
	data, err := os.ReadFile(s.metaPath(treeID, filename))
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func (s *filePieceStore) PieceCount(treeID, filename string) int {
	entries, err := os.ReadDir(s.fileDir(treeID, filename))
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.Type().IsRegular() && filepath.Ext(e.Name()) == ".piece" {
			n++
		}
	}
	return n
}

func (s *filePieceStore) Delete(treeID, filename string) error {
	return os.RemoveAll(s.fileDir(treeID, filename))
}
