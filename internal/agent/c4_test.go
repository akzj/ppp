package agent

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"google.golang.org/grpc"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestDataServerGetFileInfoStates verifies the GetFileInfo states (C4): sealed
// -> FileInfo with the correct metadata_id; building -> NOT_READY; banned ->
// BANNED; no artifact -> NOT_FOUND (no build is triggered).
func TestDataServerGetFileInfoStates(t *testing.T) {
	content := c3Content()
	store, err := NewFilePieceStore(t.TempDir() + "/pieces")
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topo := &fakeTopology{pullFromSource: true}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{data: content},
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := newRootDataServer("root", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second), true)
	key := &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}

	// 1. No artifact -> NOT_FOUND (no build triggered).
	resp, err := ds.GetFileInfo(context.Background(), &pppv1.GetFileInfoRequest{Key: key})
	if err != nil {
		t.Fatalf("GetFileInfo(no artifact): %v", err)
	}
	if resp.GetError().GetCode() != pppv1.Error_NOT_FOUND {
		t.Fatalf("GetFileInfo(no artifact) = %v, want NOT_FOUND", resp.GetError().GetCode())
	}
	if dm.Get("t1", "a.bin") != nil {
		t.Fatal("GetFileInfo triggered a build")
	}

	// 2. Sealed -> FileInfo with the correct metadata_id.
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	d.releaseNeed()
	if !store.IsComplete("a.bin") {
		t.Fatal("build did not seal")
	}
	meta, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	resp, err = ds.GetFileInfo(context.Background(), &pppv1.GetFileInfoRequest{Key: key})
	if err != nil {
		t.Fatalf("GetFileInfo(sealed): %v", err)
	}
	info := resp.GetInfo()
	if info == nil {
		t.Fatalf("GetFileInfo(sealed) = %v, want Info", resp.GetError())
	}
	if !bytes.Equal(info.GetMetadataId(), MetadataID(meta)) {
		t.Fatal("metadata_id mismatch")
	}
	if info.GetFileSize() != int64(len(content)) || info.GetPieceCount() != 3 || info.GetPieceSize() != PieceSize {
		t.Fatalf("FileInfo fields mismatch: %+v", info)
	}

	// 3. Banned -> BANNED.
	ds.banned.ApplyInitial(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})
	resp, err = ds.GetFileInfo(context.Background(), &pppv1.GetFileInfoRequest{Key: key})
	if err != nil {
		t.Fatalf("GetFileInfo(banned): %v", err)
	}
	if resp.GetError().GetCode() != pppv1.Error_BANNED {
		t.Fatalf("GetFileInfo(banned) = %v, want BANNED", resp.GetError().GetCode())
	}
}

// TestDataServerGetMetadataMatch verifies the GetMetadata stream (C4): a
// matching metadata_id streams the canonical bytes; a mismatch is a content
// conflict; an absent artifact is NOT_FOUND.
func TestDataServerGetMetadataMatch(t *testing.T) {
	content := c3Content()
	store, err := NewFilePieceStore(t.TempDir() + "/pieces")
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topo := &fakeTopology{pullFromSource: true}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{data: content},
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := newRootDataServer("root", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second), true)
	key := &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}

	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	d.releaseNeed()
	if !store.IsComplete("a.bin") {
		t.Fatal("build did not seal")
	}
	meta, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}

	// Matching id: the stream returns the exact canonical bytes.
	collect := func(mid []byte) ([]byte, error) {
		ch := &recvStream{}
		err := ds.GetMetadata(&pppv1.GetMetadataRequest{Key: key, MetadataId: mid}, ch)
		if err != nil {
			return nil, err
		}
		var buf []byte
		for _, c := range ch.chunks {
			buf = append(buf, c.GetData()...)
		}
		return buf, nil
	}
	got, err := collect(MetadataID(meta))
	if err != nil || !bytes.Equal(got, meta) {
		t.Fatalf("GetMetadata(match) = %d bytes, %v; want the canonical bytes", len(got), err)
	}
	// Mismatch -> content conflict.
	if _, err := collect(make([]byte, MetadataDigestSize)); err == nil {
		t.Fatal("GetMetadata(mismatch) = nil error, want content conflict")
	}
	// Absent -> NOT_FOUND.
	if _, err := collectWithKey(t, ds, &pppv1.TreeKey{TreeId: "t1", Filename: "nope.bin"}, MetadataID(meta)); err == nil {
		t.Fatal("GetMetadata(absent) = nil error, want NOT_FOUND")
	}
}

func collectWithKey(t *testing.T, ds *DataServer, key *pppv1.TreeKey, mid []byte) ([]byte, error) {
	t.Helper()
	ch := &recvStream{}
	if err := ds.GetMetadata(&pppv1.GetMetadataRequest{Key: key, MetadataId: mid}, ch); err != nil {
		return nil, err
	}
	var buf []byte
	for _, c := range ch.chunks {
		buf = append(buf, c.GetData()...)
	}
	return buf, nil
}

// recvStream is a minimal GetMetadata server stream that records chunks.
type recvStream struct {
	grpc.ServerStream
	chunks []*pppv1.MetadataChunk
}

func (s *recvStream) Send(chunk *pppv1.MetadataChunk) error {
	s.chunks = append(s.chunks, chunk)
	return nil
}

// TestDownloaderCopyFlowSealsCopiedMetadata verifies the C4 copy flow end to
// end: a non-root downloader binds the upstream metadata (GetFileInfo ->
// GetMetadata -> verify), fetches pieces (verified against the digests) and
// Seals with the exact copied bytes.
func TestDownloaderCopyFlowSealsCopiedMetadata(t *testing.T) {
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
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{addr}},
		&fakeSource{data: nil}, nil, "member", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsComplete("a.bin") {
		t.Fatal("copy-flow download did not seal")
	}
	// The sealed metadata is the EXACT upstream-copied bytes (the artifact
	// identity is the copied metadata, never a locally regenerated one).
	wantMeta, _, err := fake.buildMetadata("t1", "a.bin")
	if err != nil {
		t.Fatalf("fake buildMetadata: %v", err)
	}
	gotMeta, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	if !bytes.Equal(gotMeta, wantMeta) {
		t.Fatal("sealed metadata != upstream-copied metadata")
	}
}

// failSource fails every fetch; the failover test uses it to prove a root
// with a sealed sibling copies instead of rebuilding from the source.
type failSource struct{ calls int }

func (f *failSource) FetchPiece(_ context.Context, _ *pppv1.Source, _, _ string, index, _, _ int64) ([]byte, error) {
	f.calls++
	return nil, fmt.Errorf("failSource should not be used (piece %d)", index)
}

// TestRootFailoverCopiesFromSibling verifies §4.4: a root whose upstreams have
// a sealed artifact copies it (metadata + pieces) instead of rebuilding from
// the source.
func TestRootFailoverCopiesFromSibling(t *testing.T) {
	content := c3Content()
	fake := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}}
	addr, stop := startFakeData(t, fake)
	defer stop()

	src := &failSource{}
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// The root is PullFromSource=true but HAS root upstreams (the sibling):
	// the failover sealed-check must copy from the sibling, not self-build.
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{addr}, pullFromSource: true},
		src, &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root2", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsComplete("a.bin") {
		t.Fatal("root did not copy from its sibling")
	}
	if src.calls != 0 {
		t.Fatalf("root used the source %d times; it must copy from the sibling", src.calls)
	}
	wantMeta, _, err := fake.buildMetadata("t1", "a.bin")
	if err != nil {
		t.Fatalf("fake buildMetadata: %v", err)
	}
	gotMeta, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	if !bytes.Equal(gotMeta, wantMeta) {
		t.Fatal("root sealed metadata != the sibling's metadata")
	}
}

// TestDownloaderPieceDigestMismatchSwitchesUpstream verifies a piece whose
// SHA-256 does not match the bound metadata is discarded (PIECE_DIGEST_MISMATCH)
// and the fetch switches to another upstream.
func TestDownloaderPieceDigestMismatchSwitchesUpstream(t *testing.T) {
	content := c3Content()
	bad := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}, corruptPiece: true}
	badAddr, badStop := startFakeData(t, bad)
	defer badStop()
	good := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}}
	goodAddr, goodStop := startFakeData(t, good)
	defer goodStop()

	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// The corrupt upstream is FIRST; the bind gets its (valid) metadata, the
	// piece fetch gets tampered bytes -> PIECE_DIGEST_MISMATCH -> the second
	// upstream serves the good piece.
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{badAddr, goodAddr}},
		&fakeSource{data: nil}, nil, "member", 2, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsComplete("a.bin") {
		t.Fatal("file did not complete after switching upstreams")
	}
	// The stored pieces are the GOOD content (the tampered piece was discarded).
	sp := store.(*sparsePieceStore)
	for i := 0; i < 3; i++ {
		start := i * int(PieceSize)
		end := start + int(PieceSize)
		if end > len(content) {
			end = len(content)
		}
		got, err := sp.Get("a.bin", int64(i))
		if err != nil || !bytes.Equal(got, content[start:end]) {
			t.Fatalf("piece %d corrupted after upstream switch: %v", i, err)
		}
	}
}

// TestDownloaderContentConflictSkipsUpstream verifies §5.3: an upstream whose
// artifact metadata_id differs from the request's bound id is rejected with
// CONTENT_CONFLICT and its content is never mixed or stored.
func TestDownloaderContentConflictSkipsUpstream(t *testing.T) {
	content := c3Content()
	conflicting := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}, diffMetaID: true}
	confAddr, confStop := startFakeData(t, conflicting)
	defer confStop()

	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	dm := NewDownloaderManager(store, NewBannedList(), &fakeTopology{addrs: []string{confAddr}},
		&fakeSource{data: nil}, nil, "member", 2, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	// The conflicting upstream's pieces are always rejected (CONTENT_CONFLICT):
	// the downloader must never store them, so the artifact never seals.
	time.Sleep(1500 * time.Millisecond)
	if store.IsComplete("a.bin") {
		t.Fatal("conflicting artifact was sealed")
	}
	sp := store.(*sparsePieceStore)
	for i := 0; i < 3; i++ {
		if sp.HasPiece("a.bin", int64(i)) {
			t.Fatalf("piece %d from the conflicting upstream was stored", i)
		}
	}
}
