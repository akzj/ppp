package agent

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
)

// mockStream is a minimal stream for server-streaming RPC tests.
type mockStream struct {
	grpc.ServerStream
	msgs []*pppv1.ProgressState
}

func (m *mockStream) Send(p *pppv1.ProgressState) error {
	m.msgs = append(m.msgs, p)
	return nil
}

func (m *mockStream) Context() context.Context { return context.Background() }

func newTestDataServer(t *testing.T) *DataServer {
	t.Helper()
	store, err := NewFilePieceStore(t.TempDir() + "/pieces")
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topo := &fakeTopology{addrs: nil}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{}, nil, "me", 2, 30*time.Second, nil)
	return NewDataServer("me", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second))
}

func bannedResp(t *testing.T, resp *pppv1.GetPieceResponse) bool {
	t.Helper()
	switch r := resp.GetResult().(type) {
	case *pppv1.GetPieceResponse_Error:
		return r.Error.GetCode() == pppv1.Error_BANNED
	case *pppv1.GetPieceResponse_Piece:
		return false
	}
	return false
}

// TestDataServerLoopDetection verifies a request whose hop chain already
// contains this node is rejected.
func TestDataServerLoopDetection(t *testing.T) {
	ds := newTestDataServer(t)
	resp, err := ds.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"},
		Index: 0, Size: 100,
		From: []*pppv1.Hop{{NodeId: "me", JobId: "job:1"}},
	})
	if err != nil {
		t.Fatalf("GetPiece: %v", err)
	}
	switch r := resp.GetResult().(type) {
	case *pppv1.GetPieceResponse_Error:
		if r.Error.GetCode() != pppv1.Error_LOOP_DETECTED {
			t.Fatalf("error code = %v, want LOOP_DETECTED", r.Error.GetCode())
		}
	default:
		t.Fatalf("response = %v, want LOOP_DETECTED error", resp)
	}
}

// TestDataServerBannedGate verifies banned files are not served and not
// subscribed.
func TestDataServerBannedGate(t *testing.T) {
	ds := newTestDataServer(t)
	ds.banned.ApplyInitial(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})

	// GetPiece -> BANNED.
	resp, err := ds.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: 100,
	})
	if err != nil {
		t.Fatalf("GetPiece: %v", err)
	}
	if !bannedResp(t, resp) {
		t.Fatalf("GetPiece(banned) = %v, want BANNED", resp)
	}

	// Subscribe -> rejected with banned=true.
	sub, err := ds.Subscribe(context.Background(), &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, JobId: "job:1", ChildNodeId: "c1",
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if sub.GetAccepted() || !sub.GetBanned() {
		t.Fatalf("Subscribe(banned) = %v, want accepted=false banned=true", sub)
	}
}

// TestDataServerDownloadFileBanned verifies DownloadFile reports the banned
// state.
func TestDataServerDownloadFileBanned(t *testing.T) {
	ds := newTestDataServer(t)
	ds.banned.ApplyInitial(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})

	stream := &mockStream{}
	err := ds.DownloadFile(&pppv1.DownloadFileRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Size: 100,
	}, stream)
	if err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if len(stream.msgs) != 1 || stream.msgs[0].GetState() != pppv1.ProgressState_BANNED {
		t.Fatalf("DownloadFile(banned) msgs = %v, want one BANNED state", stream.msgs)
	}
	// local_path is populated as <download-path>/<basename>.
	wantPath := filepath.Join(ds.downloadPath, "a.bin")
	if stream.msgs[0].GetLocalPath() != wantPath {
		t.Fatalf("DownloadFile(banned) local_path = %q, want %q", stream.msgs[0].GetLocalPath(), wantPath)
	}
}

// TestDataServerDownloadFileSuccess verifies the DownloadFile happy path (B-1
// regression): the stream must reach SUCCESS and the file must land on disk —
// the caller's addNeed starts the fetch loop that Ensure alone never starts.
func TestDataServerDownloadFileSuccess(t *testing.T) {
	store, err := NewFilePieceStore(t.TempDir() + "/pieces")
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	content := []byte("the quick brown fox jumps over the lazy dog")
	topo := &fakeTopology{pullFromSource: true}
	// The fakeSource serves the content; the treeSource message only needs to
	// be non-nil for the pull-from-source guard.
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{data: content},
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "me", 2, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := NewDataServer("me", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second))

	stream := &mockStream{}
	if err := ds.DownloadFile(&pppv1.DownloadFileRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Size: int64(len(content)), JobId: "job:1",
	}, stream); err != nil {
		t.Fatalf("DownloadFile: %v", err)
	}
	if len(stream.msgs) == 0 {
		t.Fatal("DownloadFile produced no progress states")
	}
	last := stream.msgs[len(stream.msgs)-1]
	if last.GetState() != pppv1.ProgressState_SUCCESS {
		t.Fatalf("last state = %v, want SUCCESS (msgs=%v)", last.GetState(), stream.msgs)
	}
	if last.GetLocalPath() != filepath.Join(ds.downloadPath, "a.bin") {
		t.Fatalf("local_path = %q, want <download>/a.bin", last.GetLocalPath())
	}
	// The file must be on disk (the sparse store's final path) with the exact
	// content, and the store must report it complete.
	final := filepath.Join(ds.store.(*sparsePieceStore).dir, "a.bin")
	data, err := os.ReadFile(final)
	if err != nil || !bytes.Equal(data, content) {
		t.Fatalf("final file = %q, %v; want content", data, err)
	}
	if !ds.store.IsComplete("a.bin") {
		t.Fatal("file not complete after DownloadFile")
	}
}

// TestDataServerResolvePath verifies the ResolvePath RPC: local_path for a
// present and an absent file, and rejection of a tree mismatch or unsafe
// filename.
func TestDataServerResolvePath(t *testing.T) {
	ds := newTestDataServer(t)

	// Absent file: local_path is still resolved, exist=false.
	resp, err := ds.ResolvePath(context.Background(), &pppv1.ResolvePathRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "absent.bin"},
	})
	if err != nil {
		t.Fatalf("ResolvePath(absent): %v", err)
	}
	if resp.GetExist() {
		t.Fatal("ResolvePath(absent).exist = true, want false")
	}
	wantPath := filepath.Join(ds.downloadPath, "absent.bin")
	if resp.GetLocalPath() != wantPath {
		t.Fatalf("ResolvePath(absent).local_path = %q, want %q", resp.GetLocalPath(), wantPath)
	}

	// Present file (Put + MarkComplete): exist=true.
	if err := ds.store.Put("a.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := ds.store.MarkComplete("a.bin", 1); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	resp, err = ds.ResolvePath(context.Background(), &pppv1.ResolvePathRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"},
	})
	if err != nil {
		t.Fatalf("ResolvePath(present): %v", err)
	}
	if !resp.GetExist() || resp.GetLocalPath() != filepath.Join(ds.downloadPath, "a.bin") {
		t.Fatalf("ResolvePath(present) = %+v, want exist=true local_path=<download>/a.bin", resp)
	}

	// Tree mismatch and unsafe filename are rejected.
	for _, key := range []*pppv1.TreeKey{
		{TreeId: "other", Filename: "a.bin"},
		{TreeId: "t1", Filename: "a/b.bin"},
		{TreeId: "t1", Filename: ".."},
	} {
		if _, err := ds.ResolvePath(context.Background(), &pppv1.ResolvePathRequest{Key: key}); err == nil {
			t.Fatalf("ResolvePath(%v) = nil error, want rejection", key)
		}
	}
}
