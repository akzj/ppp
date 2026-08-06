package agent

import (
	"strings"
	"sync"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// bannedKey joins a tree id and filename into one map key.
func bannedKey(treeID, filename string) string { return treeID + "\x00" + filename }

// BannedList is the node-local banned gate, kept in sync with the control
// plane. A full snapshot is authoritative; added/removed updates are applied
// incrementally on top of it.
type BannedList struct {
	mu    sync.Mutex
	gen   int64
	files map[string]bool
}

// NewBannedList returns an empty banned list.
func NewBannedList() *BannedList {
	return &BannedList{files: make(map[string]bool)}
}

// ApplyInitial replaces the whole list with the authoritative snapshot.
func (b *BannedList) ApplyInitial(gen int64, banned []*pppv1.BannedFile) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gen = gen
	b.files = make(map[string]bool, len(banned))
	for _, f := range banned {
		b.files[bannedKey(f.GetTreeId(), f.GetFilename())] = true
	}
}

// ApplyUpdate applies one watch update. When full_snapshot is set the
// snapshot is authoritative and added/removed are ignored.
func (b *BannedList) ApplyUpdate(up *pppv1.BannedListUpdate) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if up.GetFullSnapshot() {
		b.gen = up.GetGeneration()
		b.files = make(map[string]bool, len(up.GetSnapshot()))
		for _, f := range up.GetSnapshot() {
			b.files[bannedKey(f.GetTreeId(), f.GetFilename())] = true
		}
		return
	}
	b.gen = up.GetGeneration()
	for _, f := range up.GetAdded() {
		b.files[bannedKey(f.GetTreeId(), f.GetFilename())] = true
	}
	for _, r := range up.GetRemoved() {
		delete(b.files, bannedKey(r.GetTreeId(), r.GetFilename()))
	}
}

// IsBanned reports whether the (tree, file) is currently banned.
func (b *BannedList) IsBanned(treeID, filename string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.files[bannedKey(treeID, filename)]
}

// Generation returns the last applied banned-list generation.
func (b *BannedList) Generation() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.gen
}

// Snapshot returns the generation and the full list for local persistence.
func (b *BannedList) Snapshot() (int64, []*pppv1.BannedFile) {
	b.mu.Lock()
	defer b.mu.Unlock()
	files := make([]*pppv1.BannedFile, 0, len(b.files))
	for k := range b.files {
		parts := strings.SplitN(k, "\x00", 2)
		if len(parts) == 2 {
			files = append(files, &pppv1.BannedFile{TreeId: parts[0], Filename: parts[1]})
		}
	}
	return b.gen, files
}
