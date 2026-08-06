package agent

import (
	"testing"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestBannedListHitMiss verifies the gate answers correctly.
func TestBannedListHitMiss(t *testing.T) {
	b := NewBannedList()
	if b.IsBanned("t1", "a.bin") {
		t.Fatal("empty list reports banned")
	}
	b.ApplyInitial(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})
	if !b.IsBanned("t1", "a.bin") {
		t.Fatal("banned file not reported")
	}
	if b.IsBanned("t1", "b.bin") {
		t.Fatal("unbanned file reported banned")
	}
	if b.IsBanned("t2", "a.bin") {
		t.Fatal("tree isolation violated")
	}
	if b.Generation() != 1 {
		t.Fatalf("generation = %d, want 1", b.Generation())
	}
}

// TestBannedListIncremental verifies added/removed updates apply on top of the
// current list.
func TestBannedListIncremental(t *testing.T) {
	b := NewBannedList()
	b.ApplyInitial(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})

	b.ApplyUpdate(&pppv1.BannedListUpdate{
		Generation: 2,
		Added:      []*pppv1.BannedFile{{TreeId: "t1", Filename: "b.bin"}},
	})
	if !b.IsBanned("t1", "a.bin") || !b.IsBanned("t1", "b.bin") {
		t.Fatal("added file not reflected")
	}

	b.ApplyUpdate(&pppv1.BannedListUpdate{
		Generation: 3,
		Removed:    []*pppv1.TreeKey{{TreeId: "t1", Filename: "a.bin"}},
	})
	if b.IsBanned("t1", "a.bin") {
		t.Fatal("removed file still banned")
	}
	if !b.IsBanned("t1", "b.bin") {
		t.Fatal("unrelated file lost")
	}
}

// TestBannedListFullSnapshotAuthoritative verifies a full snapshot replaces
// the whole list regardless of added/removed fields.
func TestBannedListFullSnapshotAuthoritative(t *testing.T) {
	b := NewBannedList()
	b.ApplyInitial(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "old.bin"}})

	b.ApplyUpdate(&pppv1.BannedListUpdate{
		Generation:   4,
		FullSnapshot: true,
		Snapshot:     []*pppv1.BannedFile{{TreeId: "t1", Filename: "new.bin"}},
		Added:        []*pppv1.BannedFile{{TreeId: "t1", Filename: "ignored.bin"}},
	})
	if b.IsBanned("t1", "old.bin") {
		t.Fatal("full snapshot should drop old entries")
	}
	if b.IsBanned("t1", "ignored.bin") {
		t.Fatal("added must be ignored when full_snapshot is set")
	}
	if !b.IsBanned("t1", "new.bin") {
		t.Fatal("snapshot entry missing")
	}
	if b.Generation() != 4 {
		t.Fatalf("generation = %d, want 4", b.Generation())
	}
}
