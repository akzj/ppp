package agent

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestSubscribeRenewalNeedBalance verifies P1-A: N idempotent renewals do not
// grow the downloader need counter beyond 1.
func TestSubscribeRenewalNeedBalance(t *testing.T) {
	ds := newTestDataServer(t) // ttl 30s, empty store (file not complete)
	key := &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}

	for i := 0; i < 5; i++ {
		sub, err := ds.Subscribe(context.Background(), &pppv1.SubscribeRequest{
			Key: key, JobId: "job:1", ChildNodeId: "c1", LeaseSeconds: 10,
		})
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		if !sub.GetAccepted() {
			t.Fatalf("Subscribe %d rejected", i)
		}
	}
	d := ds.dm.Get("t1", "f.bin")
	if d == nil {
		t.Fatal("no downloader created by subscription")
	}
	if n := d.Need(); n != 1 {
		t.Fatalf("need after 5 renewals = %d, want 1", n)
	}

	// Unsubscribe releases the single need and reclaims the downloader.
	if _, err := ds.Unsubscribe(context.Background(), &pppv1.UnsubscribeRequest{Key: key, JobId: "job:1", ChildNodeId: "c1"}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if n := d.Need(); n != 0 {
		t.Fatalf("need after unsubscribe = %d, want 0", n)
	}
	waitFor(t, 2*time.Second, "downloader reclaimed after unsubscribe", func() bool {
		return ds.dm.Get("t1", "f.bin") == nil
	})
}

// TestDownloaderRestartAfterSilentStop verifies P1-B: a downloader stopped
// mid-fetch (silent stop) can be restarted and still complete the file; stale
// in-flight markers must not block pieces forever.
func TestDownloaderRestartAfterSilentStop(t *testing.T) {
	content := make([]byte, PieceSize+100)
	for i := range content {
		content[i] = byte(i % 251)
	}
	fake := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:PieceSize],
		"t1\x00a.bin\x001": content[PieceSize:],
	}}
	fake.release = make(chan struct{}) // block every GetPiece until released
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, store := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed() // start fetching; both pieces block on the peer
	waitFor(t, 3*time.Second, "fetch in flight", func() bool { return fake.requestCount() >= 1 })

	d.releaseNeed() // silent stop: cancel ctx mid-fetch

	// Re-demand on the SAME held reference: the restart must drop stale
	// in-flight marks so the pieces can be re-dispatched.
	d.addNeed()
	close(fake.release) // let the restarted fetches through

	waitFor(t, 5*time.Second, "file completes after restart", func() bool {
		return store.IsComplete("a.bin")
	})
	if !store.HasPiece("a.bin", 0) || !store.HasPiece("a.bin", 1) {
		t.Fatal("not both pieces stored after restart")
	}
}

// TestDownloaderCooldownAntiFlood verifies P2-1: consecutive waiters cannot
// bypass a piece's failure cooldown.
func TestDownloaderCooldownAntiFlood(t *testing.T) {
	fake := &fakeDataServer{pieces: map[string][]byte{}} // always NOT_FOUND
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: 100})
	ctx1, c1 := context.WithTimeout(context.Background(), 500*time.Millisecond)
	_, _ = d.WaitPiece(ctx1, 0) // first demand: fails, piece cools down
	c1()

	base := fake.requestCount()
	// Dense demand: many waiters must not each trigger a fresh fetch cycle.
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		_, _ = d.WaitPiece(ctx, 0)
		cancel()
	}
	if after := fake.requestCount(); after > base+3 {
		t.Fatalf("dense demand bypassed cooldown: %d -> %d requests", base, after)
	}
}

// TestDownloaderReleasesUpstreamLease verifies the propagation mechanism: a
// terminal downloader explicitly unsubscribes from its upstream so the stop
// can move up the chain immediately.
func TestDownloaderReleasesUpstreamLease(t *testing.T) {
	content := []byte("payload")
	fake := &fakeDataServer{pieces: map[string][]byte{"t1\x00a.bin\x000": content}}
	addr, stop := startFakeData(t, fake)
	defer stop()

	dm, _ := newTestManager(t, &fakeTopology{addrs: []string{addr}}, nil)
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	if _, err := d.WaitPiece(context.Background(), 0); err != nil {
		t.Fatalf("WaitPiece: %v", err)
	}
	if fake.subscribeCount() == 0 {
		t.Fatal("downloader did not subscribe to its upstream")
	}
	// The downloader completed and was reclaimed; it must have unsubscribed
	// from the upstream so the stop propagates.
	waitFor(t, 3*time.Second, "upstream unsubscribe", func() bool {
		return fake.unsubscribeCount() >= 1
	})
	if dm.Get("t1", "a.bin") != nil {
		t.Fatal("completed downloader not reclaimed")
	}
}

// TestThreeNodeChainStopPropagation is the 3-node chain E2E: the root serves
// the file (job download), the parent fetches from the root (subscribing to
// it), and when the chain settles (child demand ends) both the parent's and
// the root's downloaders are stopped — the stop propagated upstream.
func TestThreeNodeChainStopPropagation(t *testing.T) {
	// --- HTTP source (2-piece file) ---
	content := make([]byte, PieceSize+100)
	for i := range content {
		content[i] = byte(i % 251)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, bytes.NewReader(content))
	}))
	defer httpSrv.Close()

	// --- in-process ctl + tree ---
	client, cleanup, ctlLis := startCtlGRPC(t)
	defer cleanup()
	ctx := context.Background()
	if _, err := client.CreateTree(ctx, &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t1", App: "app", Environment: "prod", Idc: "idc1",
			RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}}},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	// --- root + parent agents ---
	agentsCtx, agentsCancel := context.WithCancel(context.Background())
	defer agentsCancel()
	startAgent := func(id string, role pppv1.Node_Role) *Agent {
		cfg := DefaultConfig()
		cfg.ID = id
		cfg.Addr = "127.0.0.1:0"
		cfg.CtlAddr = ctlLis.Addr().String()
		cfg.Tree = "t1"
		cfg.Role = role
		cfg.DownloadPath = filepath.Join(t.TempDir(), "data-"+id)
		cfg.HeartbeatInterval = 200 * time.Millisecond
		cfg.LeaseTTL = 2 * time.Second
		ag, err := NewAgent(cfg)
		if err != nil {
			t.Fatalf("NewAgent(%s): %v", id, err)
		}
		if err := ag.Start(agentsCtx); err != nil {
			t.Fatalf("Start(%s): %v", id, err)
		}
		t.Cleanup(ag.Stop)
		return ag
	}
	root := startAgent("root", pppv1.Node_ROOT)
	parent := startAgent("parent", pppv1.Node_MEMBER)

	waitFor(t, 5*time.Second, "parent upstream = root", func() bool {
		as := parent.UpstreamAddrs()
		return len(as) == 1 && as[0] == root.Addr()
	})

	// --- the root pulls the file from the source on a job ---
	if _, err := client.CreateJob(ctx, &pppv1.CreateJobRequest{
		TreeId: "t1", Filename: "file.bin", Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	waitFor(t, 10*time.Second, "root has the file complete", func() bool {
		return root.store.IsComplete("file.bin")
	})
	// After completion the root's job-driven downloader is reclaimed.
	waitFor(t, 5*time.Second, "root downloader reclaimed after job", func() bool {
		return root.dm.Get("t1", "file.bin") == nil
	})

	// --- the child (test client) subscribes to the parent and pulls a piece:
	// the parent fetches from the root and subscribes to it upstream ---
	parentClient := pppv1.NewDataClient(mustDial(t, parent.Addr()))

	if _, err := parentClient.Subscribe(ctx, &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, JobId: "job:1",
		ChildNodeId: "child", LeaseSeconds: 1,
	}); err != nil {
		t.Fatalf("child Subscribe: %v", err)
	}
	if _, err := parentClient.GetPiece(ctx, &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		Index: 0, Size: int64(len(content)), JobId: "job:1",
		MetadataId: testMetaID(), // the parent has no artifact yet: back-to-source
	}); err != nil {
		t.Fatalf("child GetPiece: %v", err)
	}
	waitFor(t, 10*time.Second, "parent fetched the file from root", func() bool {
		return parent.store.HasPiece("file.bin", 1)
	})
	// While the parent fetched, it held a subscription on the root (the
	// propagation mechanism is also unit-covered by
	// TestDownloaderReleasesUpstreamLease).

	// --- the child's demand ends: unsubscribe ---
	if _, err := parentClient.Unsubscribe(ctx, &pppv1.UnsubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"}, JobId: "job:1", ChildNodeId: "child",
	}); err != nil {
		t.Fatalf("child Unsubscribe: %v", err)
	}

	// --- the whole chain must stop: parent reclaimed, and the root reclaimed
	// (the parent's terminal downloader released its upstream lease) ---
	waitFor(t, 5*time.Second, "parent + root downloaders reclaimed (chain stopped)", func() bool {
		return parent.dm.Get("t1", "file.bin") == nil && root.dm.Get("t1", "file.bin") == nil
	})
}

// TestBannedSaveCoalesce verifies the debounced Save keeps only the latest
// snapshot and Close flushes it (a burst of applies collapses to one write).
func TestBannedSaveCoalesce(t *testing.T) {
	dir := t.TempDir()
	b, err := openBannedDiskStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Burst of saves within the coalesce window.
	b.Save(1, []*pppv1.BannedFile{{TreeId: "t1", Filename: "a.bin"}})
	b.Save(2, []*pppv1.BannedFile{{TreeId: "t1", Filename: "b.bin"}})
	b.Save(3, []*pppv1.BannedFile{{TreeId: "t1", Filename: "c.bin"}})
	if err := b.Close(); err != nil { // flushes the latest synchronously
		t.Fatalf("Close: %v", err)
	}

	b2, err := openBannedDiskStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer b2.Close()
	gen, files, err := b2.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if gen != 3 || len(files) != 1 || files[0].GetFilename() != "c.bin" {
		t.Fatalf("Load after coalesce = (gen %d, %v), want (3, [c.bin])", gen, files)
	}
}
