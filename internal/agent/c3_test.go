package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// c3Content returns a deterministic 3-piece file (2 full pieces + a short one).
func c3Content() []byte {
	content := make([]byte, 2*int(PieceSize)+10)
	for i := range content {
		content[i] = byte(i * 13)
	}
	return content
}

// TestDownloaderRootSourceSinglePass verifies the root single-pass path: a
// root downloader pulls from the source, the SHA-256 digests accumulate BY
// INDEX (not goroutine completion order), the metadata is deterministic, and
// Seal publishes the three-piece artifact.
func TestDownloaderRootSourceSinglePass(t *testing.T) {
	content := c3Content()
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topo := &fakeTopology{pullFromSource: true}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{data: content},
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)

	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsComplete("a.bin") {
		t.Fatal("root single-pass download did not seal")
	}
	sp := store.(*sparsePieceStore)
	metaData, _, err := sp.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	m, err := DecodeMetadata(metaData)
	if err != nil {
		t.Fatalf("DecodeMetadata: %v", err)
	}
	if m.Filename != "a.bin" || m.PieceCount != 3 || m.DigestAlgo != DigestAlgorithmSHA256 {
		t.Fatalf("metadata fields mismatch: %+v", m)
	}
	// Digests accumulate BY INDEX: piece i's digest matches the i-th slice of
	// the source content (the final file digest covers the whole file in order).
	fileHash := sha256.New()
	for i := 0; i < 3; i++ {
		start := i * int(PieceSize)
		end := start + int(PieceSize)
		if end > len(content) {
			end = len(content)
		}
		want := sha256.Sum256(content[start:end])
		got, err := m.PieceDigest(i)
		if err != nil || !bytes.Equal(got, want[:]) {
			t.Fatalf("piece digest %d = %x, %v; want %x", i, got, err, want)
		}
		fileHash.Write(content[start:end])
	}
	if !bytes.Equal(m.FileDigest, fileHash.Sum(nil)) {
		t.Fatal("file digest mismatch")
	}
	// Deterministic: re-downloading the same content yields the same metadata_id.
	m2, err := BuildMetadata("a.bin", int64(len(content)), PieceSize, m.PieceDigests, m.FileDigest)
	if err != nil {
		t.Fatalf("BuildMetadata: %v", err)
	}
	id1, _ := m.ID()
	id2, _ := m2.ID()
	if !bytes.Equal(id1, id2) {
		t.Fatal("metadata_id is not deterministic for the same content")
	}
}

// changingSource serves piece 0 from the original file and piece 1 with a
// WRONG length (as if the source changed mid-pull).
type changingSource struct {
	orig    []byte
	changed []byte
}

func (f *changingSource) FetchPiece(_ context.Context, _ *pppv1.Source, _, _ string, index, _, pieceSize int64) ([]byte, error) {
	if index == 0 {
		off := 0
		end := off + int(pieceSize)
		if end > len(f.orig) {
			end = len(f.orig)
		}
		return f.orig[off:end], nil
	}
	// The source "changed": the remainder has a different length.
	return f.changed[pieceSize:], nil
}

// TestDownloaderSourceChangeFails verifies a mid-pull source change (a piece
// with the wrong length) fails the build: the wrong piece is never stored and
// the artifact is never sealed.
func TestDownloaderSourceChangeFails(t *testing.T) {
	content := c3Content()
	changed := append(append([]byte{}, content...), bytes.Repeat([]byte("z"), 100)...) // longer file
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topo := &fakeTopology{pullFromSource: true}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &changingSource{orig: content, changed: changed},
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 2, 30*time.Second, nil)
	t.Cleanup(dm.Close)

	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()

	// The wrong-length piece can never be stored: the build must NOT publish.
	time.Sleep(1 * time.Second)
	if store.IsComplete("a.bin") {
		t.Fatal("inconsistent artifact was sealed")
	}
	sp := store.(*sparsePieceStore)
	if sp.HasPiece("a.bin", 1) {
		t.Fatal("wrong-length piece was stored")
	}
	if _, err := os.Stat(filepath.Join(sp.dir, "a.bin")); err == nil {
		t.Fatal("final file exists for an unsealed artifact")
	}
}

// TestDownloaderConcurrentSameFilename verifies build isolation (§4.1): two
// concurrent triggers for the same filename share one downloader and end with
// one consistent artifact (no double-write, no Seal race).
func TestDownloaderConcurrentSameFilename(t *testing.T) {
	content := c3Content()
	store, err := NewFilePieceStore(newTestStoreDir(t))
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	topo := &fakeTopology{pullFromSource: true}
	dm := NewDownloaderManager(store, NewBannedList(), topo, &fakeSource{data: content},
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 4, 30*time.Second, nil)
	t.Cleanup(dm.Close)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
			d.addNeed()
			defer d.releaseNeed()
			if _, err := d.WaitPiece(context.Background(), 0); err != nil {
				t.Errorf("WaitPiece(0): %v", err)
				return
			}
			if _, err := d.WaitPiece(context.Background(), 2); err != nil {
				t.Errorf("WaitPiece(2): %v", err)
				return
			}
		}()
	}
	wg.Wait()
	if !store.IsComplete("a.bin") {
		t.Fatal("concurrent same-filename build did not seal")
	}
	// The artifact is consistent: every stored piece matches the source.
	sp := store.(*sparsePieceStore)
	for i := 0; i < 3; i++ {
		start := i * int(PieceSize)
		end := start + int(PieceSize)
		if end > len(content) {
			end = len(content)
		}
		got, err := sp.Get("a.bin", int64(i))
		if err != nil || !bytes.Equal(got, content[start:end]) {
			t.Fatalf("piece %d corrupted by concurrent build: %v", i, err)
		}
	}
}

// blockingSource blocks piece 1 until released, so a build stays in the
// BUILDING state deterministically.
type blockingSource struct {
	data    []byte
	release chan struct{}
	once    sync.Once
}

func (f *blockingSource) FetchPiece(ctx context.Context, _ *pppv1.Source, _, _ string, index, _, pieceSize int64) ([]byte, error) {
	if index == 1 {
		select {
		case <-f.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	off := index * pieceSize
	end := off + pieceSize
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	return f.data[off:end], nil
}

func (f *blockingSource) unblock() { f.once.Do(func() { close(f.release) }) }

// TestDataServerBuildingGate verifies BUILDING invisibility (decision 1): a
// root's GetPiece for an actively building artifact returns NOT_READY and does
// not re-trigger the build; after Seal the same GetPiece succeeds.
func TestDataServerBuildingGate(t *testing.T) {
	content := c3Content()
	store, err := NewFilePieceStore(t.TempDir() + "/pieces")
	if err != nil {
		t.Fatalf("NewFilePieceStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	src := &blockingSource{data: content, release: make(chan struct{})}
	topo := &fakeTopology{pullFromSource: true}
	dm := NewDownloaderManager(store, NewBannedList(), topo, src,
		&pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{"http://fake"}}, "root", 2, 30*time.Second, nil)
	t.Cleanup(dm.Close)
	ds := newRootDataServer("root", "t1", t.TempDir()+"/download", store, NewBannedList(), dm, NewLeaseManager(30*time.Second), true)

	// Start the build: piece 0 is fetched, piece 1 blocks -> BUILDING.
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && (!dm.IsBuilding("t1", "a.bin") || !store.HasPiece("a.bin", 0)) {
		time.Sleep(20 * time.Millisecond)
	}
	if !dm.IsBuilding("t1", "a.bin") || !store.HasPiece("a.bin", 0) {
		t.Fatal("build did not reach BUILDING with a locally present piece")
	}

	req := &pppv1.GetPieceRequest{Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: int64(len(content)), JobId: "job:x", MetadataId: testMetaID()}
	resp, err := ds.GetPiece(context.Background(), req)
	if err != nil {
		t.Fatalf("GetPiece: %v", err)
	}
	if resp.GetError().GetCode() != pppv1.Error_NOT_READY {
		t.Fatalf("GetPiece during build = %v, want NOT_READY", resp.GetError().GetCode())
	}

	// Release the source; the build completes and the same GetPiece succeeds
	// (with the sealed artifact's real metadata_id — C4 serves only matching
	// ids).
	src.unblock()
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsComplete("a.bin") {
		t.Fatal("build did not seal after the source released")
	}
	meta, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	req.MetadataId = MetadataID(meta)
	resp, err = ds.GetPiece(context.Background(), req)
	if err != nil {
		t.Fatalf("GetPiece after seal: %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("GetPiece after seal errored: %v", resp.GetError())
	}
	if !bytes.Equal(resp.GetPiece().GetData(), content[:int(PieceSize)]) {
		t.Fatal("GetPiece after seal returned wrong data")
	}
}

// TestDataServerRootNoArtifactGate locks the C3.5 fix: a root's FIRST GetPiece
// (no sealed artifact AND no build in progress) returns NOT_READY with no
// piece data and must NOT trigger a build — a root's build is Job-driven
// (watchJobsLoop), never downstream-request-driven. After a Job-driven build
// seals the artifact, the same GetPiece succeeds.
func TestDataServerRootNoArtifactGate(t *testing.T) {
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

	req := &pppv1.GetPieceRequest{Key: &pppv1.TreeKey{TreeId: "t1", Filename: "a.bin"}, Index: 0, Size: int64(len(content)), JobId: "job:x", MetadataId: testMetaID()}

	// 1. No sealed artifact, no build -> NOT_READY, no piece data, no build.
	resp, err := ds.GetPiece(context.Background(), req)
	if err != nil {
		t.Fatalf("GetPiece: %v", err)
	}
	if resp.GetError().GetCode() != pppv1.Error_NOT_READY {
		t.Fatalf("first GetPiece = %v, want NOT_READY", resp.GetError().GetCode())
	}
	if resp.GetPiece() != nil {
		t.Fatal("first GetPiece returned piece data")
	}
	if dm.Get("t1", "a.bin") != nil || dm.IsBuilding("t1", "a.bin") {
		t.Fatal("first GetPiece triggered a root build")
	}

	// 2. The Job-driven build (watchJobsLoop path) seals the artifact.
	d := dm.Ensure(FileNeed{TreeID: "t1", Filename: "a.bin", Size: int64(len(content))})
	d.addNeed()
	defer d.releaseNeed()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && !store.IsComplete("a.bin") {
		time.Sleep(50 * time.Millisecond)
	}
	if !store.IsComplete("a.bin") {
		t.Fatal("job-driven build did not seal")
	}

	// 3. After seal, the same GetPiece succeeds (with the real metadata_id).
	meta, _, err := store.ReadMetadata("a.bin")
	if err != nil {
		t.Fatalf("ReadMetadata: %v", err)
	}
	req.MetadataId = MetadataID(meta)
	resp, err = ds.GetPiece(context.Background(), req)
	if err != nil {
		t.Fatalf("GetPiece after seal: %v", err)
	}
	if resp.GetError() != nil {
		t.Fatalf("GetPiece after seal errored: %v", resp.GetError())
	}
	if !bytes.Equal(resp.GetPiece().GetData(), content[:int(PieceSize)]) {
		t.Fatal("GetPiece after seal returned wrong data")
	}
}
