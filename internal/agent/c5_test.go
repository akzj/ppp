package agent

import (
	"bytes"
	"context"
	"errors"
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

// TestRootSequentialRebuildConflict locks the sequential-rebuild semantics
// (C5/C6): a root that rebuilds a filename already sealed with a DIFFERENT
// metadata_id is rejected (CONTENT_CONFLICT) and the existing artifact is
// never overwritten; rebuilding the SAME content re-seals idempotently.
func TestRootSequentialRebuildConflict(t *testing.T) {
	contentA := c3Content()
	contentB := bytes.Repeat([]byte("y"), len(contentA))
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// 1. Build + seal content A (the primary root self-builds from the source).
	dmA := NewDownloaderManager(store, NewBannedList(), &fakeTopology{pullFromSource: true},
		&fakeSource{data: contentA}, &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	dA := dmA.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(contentA))})
	dA.addNeed()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	dA.releaseNeed()
	if !store.IsComplete("a.bin") {
		t.Fatal("content A did not seal")
	}
	metaA, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}

	// 2. Rebuild with DIFFERENT content B -> the conflict must refuse to
	// overwrite the sealed A.
	dmB := NewDownloaderManager(store, NewBannedList(), &fakeTopology{pullFromSource: true},
		&fakeSource{data: contentB}, &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	dB := dmB.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(contentB))})
	dB.addNeed()
	time.Sleep(2 * time.Second) // the fetch + the seal attempt happen
	dB.releaseNeed()
	got0, err := store.Get("a.bin", 0)
	if err != nil || !bytes.Equal(got0, contentA[:int(PieceSize)]) {
		t.Fatalf("the rebuild overwrote the sealed content A (err=%v)", err)
	}
	metaAfter, _, err := store.ReadMetadata("a.bin")
	if err != nil || !bytes.Equal(metaAfter, metaA) {
		t.Fatal("the rebuild replaced the sealed metadata")
	}

	// 3. Rebuild the SAME content A -> idempotent (the artifact stays A).
	dmA2 := NewDownloaderManager(store, NewBannedList(), &fakeTopology{pullFromSource: true},
		&fakeSource{data: contentA}, &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	dA2 := dmA2.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(contentA))})
	dA2.addNeed()
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	dA2.releaseNeed()
	got0, err = store.Get("a.bin", 0)
	if err != nil || !bytes.Equal(got0, contentA[:int(PieceSize)]) {
		t.Fatalf("same-content rebuild corrupted the artifact (err=%v)", err)
	}
}

// TestDownloaderGetMetadataBannedConsistency locks the banned consistency
// (C5): GetMetadata returns PermissionDenied for a banned file (the
// MetadataChunk message has no error field) and the copy-flow bind maps it to
// errFileBanned, exactly like the GetFileInfo BANNED path.
func TestDownloaderGetMetadataBannedConsistency(t *testing.T) {
	content := c3Content()
	fake := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}, bannedMeta: true}
	addr, stop := startFakeData(t, fake)
	defer stop()

	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{addr}},
		&fakeSource{data: nil}, nil, "member", 2, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = d.WaitPiece(ctx, 0)
	if !errors.Is(err, errFileBanned) {
		t.Fatalf("WaitPiece err = %v, want errFileBanned (GetMetadata PermissionDenied must map)", err)
	}
}
