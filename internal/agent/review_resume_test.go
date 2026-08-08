package agent

import (
	"bytes"
	"context"
	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"testing"
	"time"
)

func TestResumeCachedPieceNotFalseConflict(t *testing.T) {
	content := c3Content()
	fake := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}}
	addr, stop := startFakeData(t, fake)
	defer stop()

	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// The store has a cached piece (a previous session) but no sealed artifact.
	if err := store.Put("a.bin", 0, content[:int(PieceSize)]); err != nil {
		t.Fatalf("Put: %v", err)
	}
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{addr}},
		&fakeSource{data: nil}, nil, "member", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := NewDataServer("member", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second))

	// The leaf knows the correct metadata_id (from a prior GetFileInfo) and
	// requests the cached piece: it must be served, not falsely conflicted.
	wantID := mustGetMetadataID(t, mustDialForTest(t, addr), "t1", "a.bin")
	resp, err := ds.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: int64(len(content)), MetadataId: wantID,
	})
	if err != nil {
		t.Fatalf("GetPiece(resume): %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("GetPiece(resume) errored: %v", resp.GetError())
	}
	if !bytes.Equal(resp.GetPiece().GetData(), content[:int(PieceSize)]) {
		t.Fatal("resumed piece content mismatch")
	}

	// A WRONG metadata_id is still CONTENT_CONFLICT (strict verification kept).
	wrongID := bytes.Repeat([]byte{0xEE}, MetadataDigestSize)
	resp, err = ds.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: int64(len(content)), MetadataId: wrongID,
	})
	if err != nil {
		t.Fatalf("GetPiece(wrong id): %v", err)
	}
	if resp.GetError().GetCode() != pppv1.Error_CONTENT_CONFLICT {
		t.Fatalf("GetPiece(wrong id) = %v, want CONTENT_CONFLICT", resp.GetError().GetCode())
	}
}

// TestUnresolvableCachedPieceNotServed locks P2-C's no-upstream branch: when
// the artifact identity cannot be resolved (no sealed artifact, no upstream),
// a cached piece is NOT served (NOT_READY) instead of a false conflict.
func TestUnresolvableCachedPieceNotServed(t *testing.T) {
	content := c3Content()
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Put("a.bin", 0, content[:int(PieceSize)]); err != nil {
		t.Fatalf("Put: %v", err)
	}
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: nil},
		&fakeSource{data: nil}, nil, "member", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := NewDataServer("member", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second))

	resp, err := ds.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: int64(len(content)), MetadataId: testMetaID(),
	})
	if err != nil {
		t.Fatalf("GetPiece(unresolvable): %v", err)
	}
	if resp.GetError().GetCode() != pppv1.Error_NOT_READY {
		t.Fatalf("GetPiece(unresolvable) = %v, want NOT_READY", resp.GetError().GetCode())
	}
}

// TestHandshakeDownloaderIdleReclaim locks P2-A: a metadata-only GetFileInfo
// handshake leaves a need=0 downloader that the manager reclaims after the
// idle TTL (a subsequent Ensure triggers the sweep).
