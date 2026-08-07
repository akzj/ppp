package agent

import (
	"path/filepath"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestAgentStopClosesStore verifies Agent.Stop closes the piece store so mmap
// mappings/handles are released — the ENOMEM race-flake root cause and a real
// production leak on long-lived agents.
func TestAgentStopClosesStore(t *testing.T) {
	cfg := DefaultConfig()
	cfg.ID = "n1"
	cfg.Tree = "t1"
	cfg.DownloadPath = filepath.Join(t.TempDir(), "data")
	cfg.Store = "mmap"
	ag, err := NewAgent(cfg)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	ms, ok := ag.store.(*sparsePieceStore)
	if !ok {
		t.Fatalf("store = %T, want *sparsePieceStore", ag.store)
	}
	if err := ag.store.Put("f.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ms.mu.Lock()
	openBefore := len(ms.open)
	ms.mu.Unlock()
	if openBefore == 0 {
		t.Fatal("expected an open mmap file before Stop")
	}

	ag.Stop()

	ms.mu.Lock()
	openAfter := len(ms.open)
	ms.mu.Unlock()
	if openAfter != 0 {
		t.Fatalf("open mmap files after Stop = %d, want 0", openAfter)
	}
}

// TestSparseStoreEvictsIdleComplete verifies completed (read-only) files are
// evicted from the open cache after completeIdleTTL and re-opened on demand.
func TestSparseStoreEvictsIdleComplete(t *testing.T) {
	old := completeIdleTTL
	completeIdleTTL = 50 * time.Millisecond
	defer func() { completeIdleTTL = old }()

	dir := t.TempDir()
	st := newSparseTestStore(t, dir)
	if err := st.Put("a.bin", 0, []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := st.MarkComplete("a.bin", 1); err != nil {
		t.Fatalf("MarkComplete: %v", err)
	}
	ms := st.(*sparsePieceStore)
	ms.mu.Lock()
	if len(ms.open) != 1 {
		ms.mu.Unlock()
		t.Fatalf("open cache after complete = %d, want 1", len(ms.open))
	}
	ms.mu.Unlock()

	// Wait past the idle TTL, then open ANOTHER file (the lazy sweep runs on
	// open) and the completed file must be evicted.
	time.Sleep(80 * time.Millisecond)
	if err := st.Put("b.bin", 0, []byte("y")); err != nil {
		t.Fatalf("Put(b): %v", err)
	}
	ms.mu.Lock()
	_, stillOpen := ms.open["a.bin"]
	ms.mu.Unlock()
	if stillOpen {
		t.Fatal("idle completed file not evicted from the open cache")
	}

	// A later read re-opens it from disk.
	got, err := st.Get("a.bin", 0)
	if err != nil || string(got) != "x" {
		t.Fatalf("Get after eviction = %q, %v; want x", got, err)
	}
	ms.mu.Lock()
	_, reopened := ms.open["a.bin"]
	ms.mu.Unlock()
	if !reopened {
		t.Fatal("evicted file not re-opened on demand")
	}
}

// TestS3ClientPerRegion verifies the S3 source keeps one client per
// (region, endpoint): different regions get distinct, correctly configured
// clients; the same region reuses one.
func TestS3ClientPerRegion(t *testing.T) {
	s := newS3Source()

	srcA := &pppv1.Source{Type: pppv1.Source_S3, Urls: []string{"http://endpoint-a"}, Bucket: "b", Key: "k", Region: "cn-north-1"}
	srcB := &pppv1.Source{Type: pppv1.Source_S3, Urls: []string{"http://endpoint-a"}, Bucket: "b", Key: "k", Region: "us-east-1"}
	srcA2 := &pppv1.Source{Type: pppv1.Source_S3, Urls: []string{"http://endpoint-a"}, Bucket: "b", Key: "k", Region: "cn-north-1"}

	ca, err := s.clientFor(srcA)
	if err != nil {
		t.Fatalf("clientFor(A): %v", err)
	}
	cb, err := s.clientFor(srcB)
	if err != nil {
		t.Fatalf("clientFor(B): %v", err)
	}
	ca2, err := s.clientFor(srcA2)
	if err != nil {
		t.Fatalf("clientFor(A2): %v", err)
	}

	if ca == cb {
		t.Fatal("different regions shared one client")
	}
	if ca != ca2 {
		t.Fatal("same region did not reuse its client")
	}
	s.mu.Lock()
	n := len(s.clients)
	s.mu.Unlock()
	if n != 2 {
		t.Fatalf("client pool size = %d, want 2 (per region)", n)
	}
}
