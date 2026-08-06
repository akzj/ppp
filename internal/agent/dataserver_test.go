package agent

import (
	"context"
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
	topo := &fakeTopology{addrs: nil}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{}, nil, "me", 2)
	return NewDataServer("me", store, NewBannedList(), dm, NewLeaseManager(30*time.Second))
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
}
