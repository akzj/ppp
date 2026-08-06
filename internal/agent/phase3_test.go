package agent

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/ctl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// startCtlGRPC starts an in-process control plane and returns a client, a
// cleanup func and the listener (for the agent's -ctl-addr).
func startCtlGRPC(t *testing.T) (pppv1.ControlClient, func(), net.Listener) {
	t.Helper()
	cfg := ctl.DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "ctl.db")
	ctx, cancel := context.WithCancel(context.Background())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, done, err := ctl.ServeControl(ctx, cfg, lis)
	if err != nil {
		cancel()
		t.Fatalf("ServeControl: %v", err)
	}
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		cancel()
		t.Fatalf("grpc.NewClient: %v", err)
	}
	cleanup := func() {
		_ = conn.Close()
		cancel()
		<-done
	}
	return pppv1.NewControlClient(conn), cleanup, lis
}

// newTestAgent builds an agent over a temp download path without starting it
// (so the locally persisted banned list can be exercised pre-Start).
func newTestAgent(t *testing.T) *Agent {
	t.Helper()
	cfg := DefaultConfig()
	cfg.ID = "n1"
	cfg.Tree = "t1"
	cfg.DownloadPath = filepath.Join(t.TempDir(), "data")
	ag, err := NewAgent(cfg)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	t.Cleanup(ag.Stop)
	return ag
}

// TestDownloaderWaiterLeak verifies P1-3: waiters whose context expires are
// removed from the waiters map.
func TestDownloaderWaiterLeak(t *testing.T) {
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": []byte("x")}}
	fake.release = make(chan struct{})
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})

	// Two waiters with short deadlines; the peer hangs so neither is served.
	ctx1, c1 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer c1()
	ctx2, c2 := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer c2()
	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() { _, _ = d.WaitPiece(ctx1, 0); close(done1) }()
	go func() { _, _ = d.WaitPiece(ctx2, 0); close(done2) }()
	<-done1
	<-done2

	d.mu.Lock()
	n := len(d.waiters[0])
	d.mu.Unlock()
	if n != 0 {
		t.Fatalf("waiters after ctx expiry = %d, want 0 (leak)", n)
	}
}

// TestSubscribeGrantLeaseCap verifies P1-5: the granted lease is capped at the
// server TTL and matches what the lease manager actually stores.
func TestSubscribeGrantLeaseCap(t *testing.T) {
	ds := newTestDataServer(t) // ttl = 30s

	big, err := ds.Subscribe(context.Background(), &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}, ChildNodeId: "c1", LeaseSeconds: 120,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if big.GetGrantedLeaseSeconds() != 30 {
		t.Fatalf("granted = %d, want capped at server ttl 30", big.GetGrantedLeaseSeconds())
	}
	small, err := ds.Subscribe(context.Background(), &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}, ChildNodeId: "c2", LeaseSeconds: 10,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if small.GetGrantedLeaseSeconds() != 10 {
		t.Fatalf("granted = %d, want 10", small.GetGrantedLeaseSeconds())
	}
}

// TestDownloaderPersistentFailureBackoff verifies P1-2: after a persistent
// failure the piece cools down and the downloader does not hot-loop.
func TestDownloaderPersistentFailureBackoff(t *testing.T) {
	fake := &fakeDataServer{pieces: map[string][]byte{}}
	addr, stop := startFakeData(t, fake)
	defer stop()
	fake.setFailures(1000) // always fails

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	_, _ = d.WaitPiece(ctx, 0) // returns on ctx expiry

	after := fake.requestCount()
	if after > 5 {
		t.Fatalf("requests after backoff = %d, want <= 5 (no hot loop)", after)
	}
	// A further wait must not add requests (the piece is cooling).
	time.Sleep(300 * time.Millisecond)
	if got := fake.requestCount(); got > after+1 {
		t.Fatalf("requests grew during cooldown: %d -> %d", after, got)
	}
}

// TestDownloaderHungPeerTimeout verifies P1-1: a hung peer does not block a
// waiter indefinitely; the fetch is bounded and WaitPiece returns on its ctx.
func TestDownloaderHungPeerTimeout(t *testing.T) {
	old := pieceFetchTimeout
	pieceFetchTimeout = 100 * time.Millisecond
	defer func() { pieceFetchTimeout = old }()

	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": []byte("x")}}
	fake.release = make(chan struct{}) // hangs until the client cancels
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	start := time.Now()
	_, err := d.WaitPiece(ctx, 0)
	if err == nil {
		t.Fatal("WaitPiece with hung peer = nil error, want bounded failure")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("WaitPiece took %v, want bounded (< 3s)", elapsed)
	}
}

// TestDownloaderHashZeroRejected verifies P1-4: a peer piece with hash==0 is
// rejected and the retry succeeds.
func TestDownloaderHashZeroRejected(t *testing.T) {
	content := []byte("payload")
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": content}}
	fake.zeroHashFirst = 1
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, store := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	got, err := d.WaitPiece(context.Background(), 0)
	if err != nil {
		t.Fatalf("WaitPiece: %v", err)
	}
	if string(got) != "payload" {
		t.Fatalf("piece = %q, want payload", got)
	}
	if fake.requestCount() < 2 {
		t.Fatalf("requests = %d, want >= 2 (hash=0 rejected then retried)", fake.requestCount())
	}
	if !store.HasPiece("t1", "a.bin", 0) {
		t.Fatal("piece not stored after hash-0 retry")
	}
}

// TestBannedDiskCleanup verifies P2-6: banning a file removes its local
// pieces.
func TestBannedDiskCleanup(t *testing.T) {
	ag := newTestAgent(t)
	if err := ag.store.Put("t1", "a.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !ag.store.HasPiece("t1", "a.bin", 0) {
		t.Fatal("precondition: piece missing")
	}
	ag.applyBannedUpdate(&pppv1.BannedListUpdate{
		Generation: 1,
		Added:      []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}},
	})
	if ag.store.HasPiece("t1", "a.bin", 0) {
		t.Fatal("banned file pieces still on disk")
	}
	if !ag.banned.IsBanned("t1", "a.bin") {
		t.Fatal("banned list did not apply")
	}
}

// TestDownloaderReclaimed verifies P2-5: a completed downloader is removed
// from the manager while its data stays in the store.
func TestDownloaderReclaimed(t *testing.T) {
	content := []byte("0123456789")
	src := &fakeSource{data: content}
	dm, store := newTestManager(t, &fakeTopology{pullFromSource: true}, src)
	dm.SetTreeSource(&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}})
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := d.WaitPiece(ctx, 0); err != nil {
		t.Fatalf("WaitPiece: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for dm.Get("t1", "a.bin") != nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dm.Get("t1", "a.bin") != nil {
		t.Fatal("completed downloader not reclaimed from the manager")
	}
	if !store.HasPiece("t1", "a.bin", 0) {
		t.Fatal("reclaimed downloader dropped its stored data")
	}
}

// TestAgentEmptyTopologyApply verifies P2-7: an empty topology clears local
// upstreams so the node stops fetching.
func TestAgentEmptyTopologyApply(t *testing.T) {
	ag := newTestAgent(t)
	ag.applyTopology(&pppv1.Topology{
		TreeId: "t1",
		NodeUpstreams: map[string]*pppv1.NodeUpstream{
			"n1": {Addrs: []string{"10.0.0.1"}},
		},
	})
	if addrs := ag.UpstreamAddrs(); len(addrs) != 1 || addrs[0] != "10.0.0.1" {
		t.Fatalf("upstreams after apply = %v, want [10.0.0.1]", addrs)
	}
	ag.applyTopology(&pppv1.Topology{TreeId: "t1"})
	if addrs := ag.UpstreamAddrs(); len(addrs) != 0 {
		t.Fatalf("upstreams after empty topology = %v, want cleared", addrs)
	}
}

// TestBannedRestartRecovery verifies P2-10: a restarted node rejects from its
// locally persisted banned list before any ctl sync.
func TestBannedRestartRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")

	cfg1 := DefaultConfig()
	cfg1.ID = "n1"
	cfg1.Tree = "t1"
	cfg1.DownloadPath = path
	a1, err := NewAgent(cfg1)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	a1.banned.ApplyInitial(5, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})
	a1.persistBanned()
	a1.Stop() // closes the banned disk store

	cfg2 := DefaultConfig()
	cfg2.ID = "n1"
	cfg2.Tree = "t1"
	cfg2.DownloadPath = path
	a2, err := NewAgent(cfg2) // loads the local banned list
	if err != nil {
		t.Fatalf("NewAgent after restart: %v", err)
	}
	defer a2.Stop()
	if !a2.banned.IsBanned("t1", "a.bin") {
		t.Fatal("restarted node did not load the persisted banned list")
	}
	if a2.banned.Generation() != 5 {
		t.Fatalf("banned generation after restart = %d, want 5", a2.banned.Generation())
	}
}

// TestBannedDiskStoreRoundTrip verifies the local persistence store.
func TestBannedDiskStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	b, err := openBannedDiskStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	files := []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}, {TreeId: "t1", Filename: "b.bin"}}
	if err := b.Save(7, files); err != nil {
		t.Fatalf("Save: %v", err)
	}
	_ = b.Close()

	b2, err := openBannedDiskStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	gen, got, err := b2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if gen != 7 || len(got) != 2 {
		t.Fatalf("Load = (gen %d, %d files), want (7, 2)", gen, len(got))
	}
	if !bannedContains(got, "t1", "a.bin") || !bannedContains(got, "t1", "b.bin") {
		t.Fatalf("Load files = %v, want a.bin and b.bin", got)
	}
}

func bannedContains(files []*pppv1.BannedFile, treeID, filename string) bool {
	for _, f := range files {
		if f.GetTreeId() == treeID && f.GetFilename() == filename {
			return true
		}
	}
	return false
}

// TestDownloaderNeedLifecycle verifies the need model: add/release balances
// and need==0 reclaims the downloader.
func TestDownloaderNeedLifecycle(t *testing.T) {
	dm, _ := newTestManager(t, &fakeTopology{addrs: nil}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})
	if d.Need() != 0 {
		t.Fatalf("initial need = %d, want 0", d.Need())
	}
	d.addNeed() // e.g. a child subscription
	if d.Need() != 1 {
		t.Fatalf("need after add = %d, want 1", d.Need())
	}
	d.releaseNeed() // e.g. unsubscribe
	if d.Need() != 0 {
		t.Fatalf("need after release = %d, want 0", d.Need())
	}
	if dm.Get("t1", "a.bin") != nil {
		t.Fatal("need==0 downloader not reclaimed from the manager")
	}
}

// TestDownloaderStopsWhenLastNeedReleased verifies a pure-relay-style early
// stop: releasing the last need stops upstream fetching.
func TestDownloaderStopsWhenLastNeedReleased(t *testing.T) {
	content := []byte("0123456789")
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": content}}
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed() // subscriber/waiter holds the only need
	// Let a few fetches start.
	deadline := time.Now().Add(2 * time.Second)
	for fake.requestCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if fake.requestCount() == 0 {
		t.Fatal("no upstream request before release")
	}
	d.releaseNeed() // last subscriber leaves

	after := fake.requestCount()
	time.Sleep(300 * time.Millisecond)
	if got := fake.requestCount(); got > after+1 {
		t.Fatalf("requests kept growing after need release: %d -> %d", after, got)
	}
	if dm.Get("t1", "a.bin") != nil {
		t.Fatal("stopped downloader not reclaimed")
	}
}

// TestRegisterGenInit verifies P2-8: after Start the topology generation comes
// from the register response (no spurious first resync).
func TestRegisterGenInit(t *testing.T) {
	client, cleanup, ctlLis := startCtlGRPC(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.CreateTree(ctx, &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t1", RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	cfg := DefaultConfig()
	cfg.ID = "root"
	cfg.Addr = "127.0.0.1:0"
	cfg.CtlAddr = ctlLis.Addr().String()
	cfg.Tree = "t1"
	cfg.Role = pppv1.Node_ROOT
	cfg.DownloadPath = t.TempDir() + "/data"
	cfg.HeartbeatInterval = time.Hour // don't let heartbeats interfere
	ag, err := NewAgent(cfg)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	agentsCtx, agentsCancel := context.WithCancel(context.Background())
	defer agentsCancel()
	defer ag.Stop()
	if err := ag.Start(agentsCtx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	ag.mu.Lock()
	gen := ag.topologyGen
	ag.mu.Unlock()
	if gen != 1 {
		t.Fatalf("topologyGen after register = %d, want 1 (register response)", gen)
	}
}
