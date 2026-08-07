package agent

import (
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
)

// fakeTopology is a topologyProvider stub.
type fakeTopology struct {
	addrs          []string
	pullFromSource bool
}

func (f *fakeTopology) UpstreamAddrs() []string { return f.addrs }
func (f *fakeTopology) PullFromSource() bool    { return f.pullFromSource }

// fakeDataServer is an in-process Data server for downloader tests.
type fakeDataServer struct {
	pppv1.UnimplementedDataServer

	mu            sync.Mutex
	pieces        map[string][]byte // tree\x00file\x00index -> data
	errCode       pppv1.Error_ErrorCode
	failures      int
	zeroHashFirst int // serve this many pieces with hash==0
	requests      int
	subscribes    int
	unsubscribes  int
	release       chan struct{} // when set, GetPiece blocks until released or ctx done
	lastFrom      []*pppv1.Hop
}

func (f *fakeDataServer) setErr(code pppv1.Error_ErrorCode) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errCode = code
}

func (f *fakeDataServer) setFailures(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failures = n
}

func (f *fakeDataServer) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *fakeDataServer) subscribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subscribes
}

func (f *fakeDataServer) unsubscribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unsubscribes
}

func (f *fakeDataServer) Subscribe(_ context.Context, _ *pppv1.SubscribeRequest) (*pppv1.SubscribeResponse, error) {
	f.mu.Lock()
	f.subscribes++
	f.mu.Unlock()
	return &pppv1.SubscribeResponse{Accepted: true}, nil
}

func (f *fakeDataServer) Unsubscribe(_ context.Context, _ *pppv1.UnsubscribeRequest) (*pppv1.UnsubscribeResponse, error) {
	f.mu.Lock()
	f.unsubscribes++
	f.mu.Unlock()
	return &pppv1.UnsubscribeResponse{Ok: true}, nil
}

func (f *fakeDataServer) GetPiece(ctx context.Context, req *pppv1.GetPieceRequest) (*pppv1.GetPieceResponse, error) {
	f.mu.Lock()
	f.requests++
	f.lastFrom = append([]*pppv1.Hop(nil), req.GetFrom()...)
	fail := f.failures > 0
	if fail {
		f.failures--
	}
	zeroHash := f.zeroHashFirst > 0
	if zeroHash {
		f.zeroHashFirst--
	}
	code := f.errCode
	release := f.release
	f.mu.Unlock()

	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
		}
	}
	if fail {
		return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Error{
			Error: &pppv1.Error{Code: pppv1.Error_INTERNAL, Message: "transient"},
		}}, nil
	}
	if code != pppv1.Error_ERROR_CODE_UNSPECIFIED {
		return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Error{
			Error: &pppv1.Error{Code: code, Message: "peer error"},
		}}, nil
	}
	key := req.GetKey().GetTreeId() + "\x00" + req.GetKey().GetFilename() + "\x00" + strconv.FormatInt(req.GetIndex(), 10)
	data, ok := f.pieces[key]
	if !ok {
		return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Error{
			Error: &pppv1.Error{Code: pppv1.Error_NOT_FOUND},
		}}, nil
	}
	h := crc64.Checksum(data, crcTable)
	if zeroHash {
		h = 0
	}
	return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Piece{
		Piece: &pppv1.Piece{
			Info: &pppv1.PieceInfo{Hash: h, Index: req.GetIndex(), Size: int32(len(data)), Offset: req.GetIndex() * PieceSize},
			Data: data,
		},
	}}, nil
}

func startFakeData(t *testing.T, fake *fakeDataServer) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	pppv1.RegisterDataServer(gs, fake)
	go func() { _ = gs.Serve(lis) }()
	return lis.Addr().String(), func() { gs.Stop() }
}

// newTestStoreDir creates a store root dir whose removal is retried: a
// downloader goroutine can be mid-open (creating store files) when the test
// ends, and the retry absorbs that cleanup race (t.TempDir does not retry).
func newTestStoreDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ppp-store-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() {
		for i := 0; i < 100; i++ {
			if err := os.RemoveAll(dir); err == nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return dir
}

func newTestManager(t *testing.T, topo *fakeTopology, src Source) (*DownloaderManager, PieceStore) {
	t.Helper()
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	if src == nil {
		src = &fakeSource{data: []byte("x")}
	}
	dm := NewDownloaderManager(store, NewBannedList(), topo, src, nil, "me", 4, 30*time.Second)
	// Cleanup order (LIFO): stop the downloaders first (so no goroutine can
	// create store files), then close the store's bbolt handles, then let the
	// removal run on an empty directory.
	t.Cleanup(dm.Close)
	t.Cleanup(func() { _ = store.Close() })
	return dm, store
}

// fakeSource serves a whole file split by pieceSize.
type fakeSource struct {
	data []byte
}

func (f *fakeSource) FetchPiece(_ context.Context, _ *pppv1.Source, _ string, _ string, index, size, pieceSize int64) ([]byte, error) {
	offset := index * pieceSize
	if offset >= int64(len(f.data)) {
		return nil, fmt.Errorf("fake source: piece %d out of range", index)
	}
	end := offset + pieceSize
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return f.data[offset:end], nil
}

// TestDownloaderFetchesFromPeer verifies a piece is pulled from an upstream
// parent and served to a waiter.
func TestDownloaderFetchesFromPeer(t *testing.T) {
	content := []byte("0123456789abcdef")
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": content}}
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, store := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	got, err := d.WaitPiece(context.Background(), 0)
	if err != nil {
		t.Fatalf("WaitPiece: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("piece = %q, want %q", got, content)
	}
	// A single-piece file is complete once its only piece is stored.
	if !store.IsComplete("a.bin") {
		t.Fatal("single-piece file not marked complete after fetch")
	}
}

// TestDownloaderSourcePull verifies the primary root pulls pieces from the
// source (a file larger than one 4 MiB piece) and marks the file complete.
func TestDownloaderSourcePull(t *testing.T) {
	content := make([]byte, PieceSize+100)
	for i := range content {
		content[i] = byte(i % 251)
	}
	src := &fakeSource{data: content}
	dm, store := newTestManager(t, &fakeTopology{pullFromSource: true}, src)
	// The tree source arrives with the register response in production.
	dm.SetTreeSource(&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}})
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p0, err := d.WaitPiece(ctx, 0)
	if err != nil {
		t.Fatalf("WaitPiece(0): %v", err)
	}
	p1, err := d.WaitPiece(ctx, 1)
	if err != nil {
		t.Fatalf("WaitPiece(1): %v", err)
	}
	if int64(len(p0)) != PieceSize || int64(len(p1)) != 100 {
		t.Fatalf("piece lengths = %d, %d; want %d, 100", len(p0), len(p1), PieceSize)
	}
	for i, b := range p0 {
		if b != content[i] {
			t.Fatalf("piece 0 byte %d = %d, want %d", i, b, content[i])
		}
	}
	for i, b := range p1 {
		if b != content[PieceSize+int64(i)] {
			t.Fatalf("piece 1 byte %d = %d, want %d", i, b, content[PieceSize+int64(i)])
		}
	}
	// Both pieces cached: file is complete.
	done := false
	for i := 0; i < 200 && !done; i++ {
		_, _, done, _ = d.Progress()
		time.Sleep(10 * time.Millisecond)
	}
	if !done {
		t.Fatal("file not marked complete after both pieces")
	}
	if !store.IsComplete("a.bin") || store.Size("a.bin") != int64(len(content)) {
		t.Fatal("completion marker not written")
	}
}

// TestDownloaderRetriesTransientFailure verifies a piece fetch retries and
// eventually succeeds.
func TestDownloaderRetriesTransientFailure(t *testing.T) {
	content := []byte("payload")
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": content}}
	addr, stop := startFakeData(t, fake)
	defer stop()
	fake.setFailures(2) // succeed on the 3rd attempt

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	got, err := d.WaitPiece(context.Background(), 0)
	if err != nil {
		t.Fatalf("WaitPiece after retries: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("piece = %q, want payload", got)
	}
	if fake.requestCount() < 3 {
		t.Fatalf("requests = %d, want >= 3 (retried)", fake.requestCount())
	}
}

// TestDownloaderBannedPropagation verifies a BANNED response from an upstream
// stops the file and propagates to waiters.
func TestDownloaderBannedPropagation(t *testing.T) {
	fake := &fakeDataServer{pieces: map[string][]byte{}}
	addr, stop := startFakeData(t, fake)
	defer stop()
	fake.setErr(pppv1.Error_BANNED)

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})
	_, err := d.WaitPiece(context.Background(), 0)
	if !errors.Is(err, errFileBanned) {
		t.Fatalf("WaitPiece err = %v, want errFileBanned", err)
	}
}

// TestDownloaderNoUpstream verifies a node with no upstream and no source
// stops fetching (waits) instead of hammering anyone, and a waiter times out.
func TestDownloaderNoUpstream(t *testing.T) {
	fake := &fakeDataServer{}
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: nil}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := d.WaitPiece(ctx, 0)
	if err == nil {
		t.Fatal("WaitPiece with no upstream = nil error, want timeout")
	}
	// The agent never even dialed the (unused) parent address.
	if fake.requestCount() != 0 {
		t.Fatalf("requests to parent = %d, want 0 (no upstream)", fake.requestCount())
	}
	_ = addr
}

// TestDownloaderCancel verifies canceling the file fails waiters promptly.
func TestDownloaderCancel(t *testing.T) {
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": []byte("x")}}
	fake.release = make(chan struct{})
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})

	type res struct {
		data []byte
		err  error
	}
	ch := make(chan res, 1)
	go func() {
		data, err := d.WaitPiece(context.Background(), 0)
		ch <- res{data, err}
	}()
	time.Sleep(50 * time.Millisecond) // let the fetch block on the peer
	dm.CancelFile("t1", "a.bin")

	select {
	case r := <-ch:
		if !errors.Is(r.err, errFileBanned) {
			t.Fatalf("WaitPiece err = %v, want errFileBanned", r.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter not released by cancel")
	}
}

// TestDownloaderEmptyTopologyNotPrimaryStops verifies the from-chain on
// upstream requests contains the node's own hop.
func TestDownloaderAppendsOwnHop(t *testing.T) {
	content := []byte("payload")
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": content}}
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{
		TreeID: "t1", Filename: "a.bin", Size: int64(len(content)),
		From: []*pppv1.Hop{{NodeId: "child", JobId: "job:1"}},
	})
	if _, err := d.WaitPiece(context.Background(), 0); err != nil {
		t.Fatalf("WaitPiece: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.lastFrom) != 2 || fake.lastFrom[0].GetNodeId() != "child" || fake.lastFrom[1].GetNodeId() != "me" {
		t.Fatalf("from chain = %v, want [child, me]", fake.lastFrom)
	}
}
