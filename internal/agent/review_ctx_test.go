package agent

import (
	"context"
	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"testing"
	"time"
)

func TestGetFileInfoClientCancel(t *testing.T) {
	content := c3Content()
	fake := &fakeDataServer{pieces: map[string][]byte{
		"t1\x00a.bin\x000": content[:int(PieceSize)],
		"t1\x00a.bin\x001": content[int(PieceSize) : 2*int(PieceSize)],
		"t1\x00a.bin\x002": content[2*int(PieceSize):],
	}, blockFileInfo: make(chan struct{})}
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cancel() // the client is already gone
	start := time.Now()
	resp, err := ds.GetFileInfo(ctx, &pppv1.GetFileInfoRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"},
	})
	if err == nil && resp.GetInfo() != nil {
		t.Fatal("GetFileInfo succeeded despite a canceled client")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("GetFileInfo did not abort fast on client cancel (%s)", time.Since(start))
	}
}

func mustDialForTest(t *testing.T, addr string) pppv1.DataClient {
	t.Helper()
	return pppv1.NewDataClient(mustDial(t, addr))
}
