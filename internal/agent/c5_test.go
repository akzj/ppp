package agent

import (
	"bytes"
	"context"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestNonRootPipelineServesPresentPieces locks the root-below pipeline
// semantics (decision 1): after a non-root binds the metadata, a piece it
// already has is served downstream IMMEDIATELY (even while the file is still
// downloading), and a missing piece waits/fetches.
func TestNonRootPipelineServesPresentPieces(t *testing.T) {
	content := c3Content()
	// The upstream serves the full artifact but blocks piece 1, so the member
	// has piece 0 while piece 1 is still downloading (the pipeline state).
	fake := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}, release: make(chan struct{}), blockPiece: 1}
	addr, stop := startFakeData(t, fake)
	defer stop()

	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{addr}},
		&fakeSource{data: nil}, nil, "member", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := NewDataServer("member", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second))

	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	// Wait until the member has piece 0 (piece 1 is blocked in the upstream).
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && !store.HasPiece("a.bin", 0) {
		time.Sleep(20 * time.Millisecond)
	}
	if !store.HasPiece("a.bin", 0) {
		t.Fatal("piece 0 never arrived")
	}
	// The downloader has bound the metadata (the identity for serving).
	boundID := dm.BoundMetadataID("t1", "a.bin")
	if len(boundID) != MetadataDigestSize {
		t.Fatalf("bound metadata id missing (len=%d)", len(boundID))
	}

	// The present piece 0 is served downstream immediately.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := ds.GetPiece(ctx, &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: int64(len(content)), MetadataId: boundID,
	})
	if err != nil {
		t.Fatalf("GetPiece(0): %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("GetPiece(0) errored: %v", resp.GetError())
	}
	if !bytes.Equal(resp.GetPiece().GetData(), content[:int(PieceSize)]) {
		t.Fatal("piece 0 content mismatch")
	}

	// The genuinely missing piece (1) is waited for: the upstream blocks it,
	// so the back-to-source WaitPiece does not return within the short ctx.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel2()
	_, err = ds.GetPiece(ctx2, &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 1, Size: int64(len(content)), MetadataId: boundID,
	})
	if err == nil {
		t.Fatal("GetPiece(1) succeeded while the upstream blocked it; the missing piece should wait")
	}
}
