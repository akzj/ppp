package agent

import (
	"context"
	"testing"
	"time"
)

func TestHandshakeDownloaderIdleReclaim(t *testing.T) {
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

	// The handshake creates a need=0 downloader.
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin"})
	if _, err := d.FileInfo(context.Background()); err != nil {
		t.Fatalf("FileInfo: %v", err)
	}
	if d.Need() != 0 {
		t.Fatalf("handshake downloader need = %d, want 0", d.Need())
	}
	if dm.Get("t1", "a.bin") == nil {
		t.Fatal("handshake downloader not registered")
	}

	// Age the downloader + trigger the sweep via a subsequent Ensure.
	dm.idleTTL = 50 * time.Millisecond
	d.mu.Lock()
	d.lastUse = time.Now().Add(-time.Hour)
	d.mu.Unlock()
	_ = dm.Ensure(FileNeed{TreeID: "t1", Filename: "other.bin"}) // triggers reclaimIdleLocked
	if dm.Get("t1", "a.bin") != nil {
		t.Fatal("idle handshake downloader was not reclaimed")
	}
}

// TestConsecutiveHandshakesNotReclaimed locks P2-A's no-false-positive: quick
// consecutive handshakes (within the TTL) reuse the same downloader and are
// not reclaimed between them.
func TestConsecutiveHandshakesNotReclaimed(t *testing.T) {
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
	dm.idleTTL = 50 * time.Millisecond

	d1 := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin"})
	if _, err := d1.FileInfo(context.Background()); err != nil {
		t.Fatalf("FileInfo: %v", err)
	}
	// A second handshake within the TTL reuses the SAME downloader.
	d2 := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin"})
	if d2 != d1 {
		t.Fatal("consecutive handshakes created a second downloader")
	}
	time.Sleep(30 * time.Millisecond) // still within the 50ms TTL
	_ = dm.Ensure(FileNeed{TreeID: "t1", Filename: "other.bin"})
	if dm.Get("t1", "a.bin") != d1 {
		t.Fatal("an active handshake sequence was reclaimed")
	}
}

// TestGetFileInfoClientCancel locks P2-B: GetFileInfo propagates the request
// ctx — a client cancellation aborts a slow upstream handshake quickly.
