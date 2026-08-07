// Package e2e runs the full PPP system as real binaries over real gRPC:
// ppp-ctl-server + ppp-service subprocesses, a real HTTP piece source, and a
// real ctl client. Everything is end-to-end (no in-process shortcuts).
package e2e

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/ctl"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// e2ePGDSN is the PostgreSQL DSN for the e2e ctl (Docker PG; see
// docs/deployment.md). The tests skip when it is unreachable so the suite is
// not red on machines without PG.
const e2ePGDSN = "postgres://ppp:ppp@127.0.0.1:25433/ppp_test_e2e"

// truncateE2EPG clears the shared ctl tables before each scenario (the two
// e2e scenarios share one test database).
func truncateE2EPG(t *testing.T) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), e2ePGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not reachable at %s (%v); skipping", e2ePGDSN, err)
	}
	defer pool.Close()
	// The e2e database may be brand-new; ensure the schema before truncating.
	if err := ctl.MigrateSchema(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE trees, nodes, jobs, banned, progress, meta, ctl_leader RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// pieceSize must match the agent's PieceSize (4 MiB) so the real binaries
// slice/verify pieces identically.
const pieceSize = 4 << 20

const (
	treeID   = "t1"
	filename = "file.bin"
	// A 2-piece file exercises multi-piece download while keeping the test
	// binary light.
	contentLen = pieceSize + 100
)

var (
	ctlBin string
	svcBin string
)

// TestMain builds the real binaries once into a temp dir (the module cache is
// warm, so this is fast) and cleans them up afterwards.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ppp-e2e-bin-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: mkdir temp: %v\n", err)
		os.Exit(1)
	}
	ctlBin = filepath.Join(dir, "ppp-ctl-server")
	svcBin = filepath.Join(dir, "ppp-service")
	for pkg, out := range map[string]string{
		"github.com/akzj/ppp/cmd/ppp-ctl-server": ctlBin,
		"github.com/akzj/ppp/cmd/ppp-service":    svcBin,
	} {
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "e2e: build %s: %v\n", pkg, err)
			os.RemoveAll(dir)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// ---------- subprocess helpers ----------

// proc wraps a subprocess with kill+wait cleanup.
type proc struct {
	cmd     *exec.Cmd
	done    chan error
	mu      sync.Mutex
	stopped bool
}

func startProc(t *testing.T, logDir, logName, bin string, args ...string) *proc {
	t.Helper()
	logFile, err := os.Create(filepath.Join(logDir, logName+".log"))
	if err != nil {
		t.Fatalf("create %s log: %v", logName, err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", logName, err)
	}
	p := &proc{cmd: cmd, done: make(chan error, 1)}
	go func() { p.done <- cmd.Wait() }()
	t.Cleanup(p.stop)
	return p
}

func (p *proc) stop() {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return
	}
	p.stopped = true
	p.mu.Unlock()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
	}
}

// killAndWait terminates the process and blocks until it exits (for restart
// tests where the process must be gone before a replacement binds its port).
func (p *proc) killAndWait() error {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil
	}
	p.stopped = true
	p.mu.Unlock()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case err := <-p.done:
		return err
	case <-time.After(5 * time.Second):
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
		return fmt.Errorf("process %s did not exit in time", p.cmd.Path)
	}
}

// ---------- network/grpc helpers ----------

// freePort probes a free TCP port on 127.0.0.1 and returns it (the port is
// released; a small race exists but is acceptable for tests).
func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()
	return port
}

func waitPort(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("address %s not reachable within %s", addr, timeout)
}

func newControlClient(t *testing.T, addr string) pppv1.ControlClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial ctl %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pppv1.NewControlClient(conn)
}

func newDataClient(t *testing.T, addr string) pppv1.DataClient {
	t.Helper()
	// 4 MiB pieces + framing require a large max message size.
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64<<20),
			grpc.MaxCallSendMsgSize(64<<20),
		),
	)
	if err != nil {
		t.Fatalf("dial data %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pppv1.NewDataClient(conn)
}

// ---------- file/topology helpers ----------

// mmapFinalPath returns the final file path in the flat basename layout:
// <DownloadPath>/<basename> (the store is tree-agnostic; treeID is ignored).
func mmapFinalPath(dir, treeID, filename string) string {
	return filepath.Join(dir, filename)
}

// waitForFile polls until the file exists and its content matches want.
func waitForFile(t *testing.T, path string, want []byte, timeout time.Duration) {
	t.Helper()
	wantHash := fmt.Sprintf("%x", md5.Sum(want))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && fmt.Sprintf("%x", md5.Sum(data)) == wantHash {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("file %s not present with expected content within %s", path, timeout)
}

// waitTopologyUpstream waits until the ctl's topology gives nodeID the wanted
// upstream address (the explicit member→root convergence check).
func waitTopologyUpstream(t *testing.T, ctl pppv1.ControlClient, nodeID, wantUpstream string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stream, err := ctl.WatchTopology(ctx, &pppv1.WatchTopologyRequest{TreeId: treeID})
	if err != nil {
		t.Fatalf("WatchTopology: %v", err)
	}
	for {
		upd, err := stream.Recv()
		if err != nil {
			t.Fatalf("WatchTopology recv: %v", err)
		}
		topo := upd.GetTopology()
		if topo == nil {
			continue
		}
		up := topo.GetNodeUpstreams()[nodeID]
		t.Logf("topology gen=%d nodes=%d %s upstream=%v pull=%v",
			topo.GetGeneration(), len(topo.GetNodeUpstreams()), nodeID, up.GetAddrs(), up.GetPullFromSource())
		for _, a := range up.GetAddrs() {
			if a == wantUpstream {
				return
			}
		}
	}
}

// e2eFixedMetaID is used when no sealed artifact exists yet (the back-to-source
// path does not compare it; the BANNED/NOT_READY gates only need non-empty).
var e2eFixedMetaID = bytes.Repeat([]byte{0xCD}, 32)

// getPiece first asks the server for the sealed artifact's metadata_id (C4)
// and binds the request to it; before the artifact is sealed the fixed value
// is used (the back-to-source path binds the real id itself).
func getPiece(ctx context.Context, data pppv1.DataClient, treeID, filename string, index, size int64) (*pppv1.GetPieceResponse, error) {
	metaID := e2eFixedMetaID
	if resp, err := data.GetFileInfo(ctx, &pppv1.GetFileInfoRequest{
		Key: &pppv1.TreeKey{TreeId: treeID, Filename: filename},
	}); err == nil && resp.GetInfo() != nil {
		metaID = resp.GetInfo().GetMetadataId()
	}
	return data.GetPiece(ctx, &pppv1.GetPieceRequest{
		Key:        &pppv1.TreeKey{TreeId: treeID, Filename: filename},
		Index:      index,
		Size:       size,
		JobId:      "e2e:job",
		MetadataId: metaID,
	})
}

// waitPieceState polls GetPiece until it reaches the wanted error code (OK for
// served, BANNED for banned).
func waitPieceState(t *testing.T, data pppv1.DataClient, treeID, filename string, index, size int64, want pppv1.Error_ErrorCode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastCode pppv1.Error_ErrorCode
	var lastErr error
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		resp, err := getPiece(ctx, data, treeID, filename, index, size)
		cancel()
		if err == nil {
			code := resp.GetError().GetCode()
			if code == want {
				return
			}
			lastCode = code
		} else {
			lastErr = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("piece state not %v within %s (last code=%v err=%v)", want, timeout, lastCode, lastErr)
}

// startService starts a ppp-service subprocess and waits for its Data port.
func startService(t *testing.T, logDir, id, addr, ctlAddr, role, downloadPath string) *proc {
	t.Helper()
	p := startProc(t, logDir, "svc-"+id, svcBin,
		"-id", id,
		"-addr", addr,
		"-ctl-addr", ctlAddr,
		"-tree", treeID,
		"-role", role,
		"-download-path", downloadPath,
		"-heartbeat-interval", "200ms",
		"-lease-ttl", "2s",
	)
	waitPort(t, addr, 10*time.Second)
	return p
}

// ---------- Scenario A: single root + members ----------

// TestE2ESingleRootDistribution exercises the full happy path plus
// cancel/ban/unban and the restart-window local ban persistence.
func TestE2ESingleRootDistribution(t *testing.T) {
	truncateE2EPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Deterministic 2-piece content served by an in-test HTTP source.
	content := make([]byte, contentLen)
	for i := range content {
		content[i] = byte(i * 31)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, filename, time.Time{}, bytes.NewReader(content))
	}))
	defer httpSrv.Close()

	logDir := t.TempDir()

	// --- ctl ---
	ctlPort := freePort(t)
	ctlAddr := fmt.Sprintf("127.0.0.1:%d", ctlPort)
	startProc(t, logDir, "ctl", ctlBin, "-addr", ctlAddr, "-pg-dsn", e2ePGDSN)
	waitPort(t, ctlAddr, 10*time.Second)
	ctl := newControlClient(t, ctlAddr)

	// --- tree ---
	if _, err := ctl.CreateTree(ctx, &pppv1.CreateTreeRequest{Tree: &pppv1.Tree{
		Id: treeID, RootCount: 1, GroupMembers: 2, GroupChildren: 2,
		Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}},
	}}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	// --- services: root + 2 members ---
	rootPort := freePort(t)
	rootAddr := fmt.Sprintf("127.0.0.1:%d", rootPort)
	rootPath := filepath.Join(t.TempDir(), "root")
	startService(t, logDir, "root", rootAddr, ctlAddr, "root", rootPath)

	m1Port := freePort(t)
	m1Addr := fmt.Sprintf("127.0.0.1:%d", m1Port)
	m1Path := filepath.Join(t.TempDir(), "m1")
	startService(t, logDir, "m1", m1Addr, ctlAddr, "member", m1Path)

	m2Port := freePort(t)
	m2Addr := fmt.Sprintf("127.0.0.1:%d", m2Port)
	m2Path := filepath.Join(t.TempDir(), "m2")
	m2 := startService(t, logDir, "m2", m2Addr, ctlAddr, "member", m2Path)

	// --- topology convergence: members see the root as upstream ---
	waitTopologyUpstream(t, ctl, "m1", rootAddr, 30*time.Second)
	waitTopologyUpstream(t, ctl, "m2", rootAddr, 30*time.Second)

	// --- CreateJob: root pulls from the HTTP source, members P2P from root ---
	if _, err := ctl.CreateJob(ctx, &pppv1.CreateJobRequest{
		TreeId: treeID, Filename: filename, Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	m1Data := newDataClient(t, m1Addr)
	m2Data := newDataClient(t, m2Addr)

	// C5 metadata flow: before the root seals (and before any member
	// download), a member's GetFileInfo is NOT_FOUND (no artifact, no build —
	// the root's build is Job-driven, so a GetFileInfo never triggers one).
	beforeInfo, err := m1Data.GetFileInfo(ctx, &pppv1.GetFileInfoRequest{
		Key: &pppv1.TreeKey{TreeId: treeID, Filename: filename},
	})
	if err != nil {
		t.Fatalf("GetFileInfo before seal: %v", err)
	}
	if beforeInfo.GetError().GetCode() != pppv1.Error_NOT_FOUND {
		t.Fatalf("GetFileInfo before seal = %v, want NOT_FOUND", beforeInfo.GetError().GetCode())
	}

	// The job drives the root's download from the source. Members pull
	// on-demand (phase-3 design: watchJobsLoop is root-only): a GetPiece on a
	// member triggers its full-file subtask download from the root, after
	// which its download path has the final file.
	waitForFile(t, mmapFinalPath(rootPath, treeID, filename), content, 30*time.Second)

	// Trigger both members' downloads (index 0 waits for the whole file).
	waitPieceState(t, m1Data, treeID, filename, 0, int64(len(content)), pppv1.Error_ERROR_CODE_UNSPECIFIED, 30*time.Second)
	waitPieceState(t, m2Data, treeID, filename, 0, int64(len(content)), pppv1.Error_ERROR_CODE_UNSPECIFIED, 30*time.Second)

	// Assert every node's download path has the final file with correct content.
	waitForFile(t, mmapFinalPath(m1Path, treeID, filename), content, 15*time.Second)
	waitForFile(t, mmapFinalPath(m2Path, treeID, filename), content, 15*time.Second)

	// C5: after the members copied the artifact, GetFileInfo returns the
	// sealed metadata_id and the member's sealed metadata equals the root's
	// (the artifact is copied, never regenerated).
	afterInfo, err := m1Data.GetFileInfo(ctx, &pppv1.GetFileInfoRequest{
		Key: &pppv1.TreeKey{TreeId: treeID, Filename: filename},
	})
	if err != nil || afterInfo.GetInfo() == nil {
		t.Fatalf("GetFileInfo after copy = %v, %v; want Info", afterInfo.GetError(), err)
	}
	if len(afterInfo.GetInfo().GetMetadataId()) != 32 {
		t.Fatalf("member metadata_id len = %d, want 32", len(afterInfo.GetInfo().GetMetadataId()))
	}
	rootMeta, rerr := os.ReadFile(filepath.Join(rootPath, filename+".cds.metadata"))
	m1Meta, merr := os.ReadFile(filepath.Join(m1Path, filename+".cds.metadata"))
	if rerr != nil || merr != nil {
		t.Fatalf("read metadata sidecars: root=%v m1=%v", rerr, merr)
	}
	if !bytes.Equal(m1Meta, rootMeta) {
		t.Fatal("member metadata != root metadata (the artifact must be copied exactly)")
	}

	// --- CancelJob: banned converges, GetPiece returns BANNED ---
	if _, err := ctl.CancelJob(ctx, &pppv1.CancelJobRequest{TreeId: treeID, Filename: filename}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	waitPieceState(t, m1Data, treeID, filename, 0, int64(len(content)), pppv1.Error_BANNED, 30*time.Second)

	// --- restart m2 while the file is banned: the locally persisted banned
	// list must reject during the restart window and after reconnecting ---
	if err := m2.killAndWait(); err != nil {
		t.Logf("member2 exit: %v", err)
	}
	startService(t, logDir, "m2", m2Addr, ctlAddr, "member", m2Path)
	// The existing m2Data client reconnects to the restarted member. In the
	// restart window the local banned.db (persisted before the restart) is
	// loaded before the ctl re-sync, so the file is still rejected.
	waitPieceState(t, m2Data, treeID, filename, 0, int64(len(content)), pppv1.Error_BANNED, 30*time.Second)

	// --- Unban: GetPiece recovers (member re-downloads from the root) ---
	if _, err := ctl.Unban(ctx, &pppv1.UnbanRequest{TreeId: treeID, Filename: filename}); err != nil {
		t.Fatalf("Unban: %v", err)
	}
	// The cancel removed the local artifacts (P2-6). Under the BUILDING
	// semantics a root's rebuild is Job-driven (decision 1), so a fresh job
	// re-creates the artifact before members fetch again.
	if _, err := ctl.CreateJob(ctx, &pppv1.CreateJobRequest{TreeId: treeID, Filename: filename, Size: int64(len(content))}); err != nil {
		t.Fatalf("CreateJob after unban: %v", err)
	}
	waitForFile(t, mmapFinalPath(rootPath, treeID, filename), content, 30*time.Second)
	waitPieceState(t, m2Data, treeID, filename, 0, int64(len(content)), pppv1.Error_ERROR_CODE_UNSPECIFIED, 30*time.Second)
}

// ---------- Scenario B: multi-root fault tolerance ----------

// TestE2EMultiRootFaultTolerance verifies two roots back each other up: the
// non-primary root fetches from the primary, and after the primary is killed
// the surviving root still serves a member.
func TestE2EMultiRootFaultTolerance(t *testing.T) {
	truncateE2EPG(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	content := make([]byte, contentLen)
	for i := range content {
		content[i] = byte(i * 17)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, filename, time.Time{}, bytes.NewReader(content))
	}))
	defer httpSrv.Close()

	logDir := t.TempDir()

	ctlPort := freePort(t)
	ctlAddr := fmt.Sprintf("127.0.0.1:%d", ctlPort)
	startProc(t, logDir, "ctl", ctlBin, "-addr", ctlAddr, "-pg-dsn", e2ePGDSN)
	waitPort(t, ctlAddr, 10*time.Second)
	ctl := newControlClient(t, ctlAddr)

	// Two roots ("r1" < "r2": r1 is primary and pulls from the source).
	if _, err := ctl.CreateTree(ctx, &pppv1.CreateTreeRequest{Tree: &pppv1.Tree{
		Id: treeID, RootCount: 2, GroupMembers: 1, GroupChildren: 1,
		Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}},
	}}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	r1Path := filepath.Join(t.TempDir(), "r1")
	r2Path := filepath.Join(t.TempDir(), "r2")
	mPath := filepath.Join(t.TempDir(), "m")

	r1Port := freePort(t)
	r1Addr := fmt.Sprintf("127.0.0.1:%d", r1Port)
	r1 := startService(t, logDir, "r1", r1Addr, ctlAddr, "root", r1Path)

	r2Port := freePort(t)
	r2Addr := fmt.Sprintf("127.0.0.1:%d", r2Port)
	startService(t, logDir, "r2", r2Addr, ctlAddr, "root", r2Path)

	// r2 (non-primary) must converge with r1 (primary) as upstream.
	waitTopologyUpstream(t, ctl, "r2", r1Addr, 30*time.Second)

	// CreateJob: the primary pulls from the source; the non-primary fetches
	// from the primary (root mutual backup).
	if _, err := ctl.CreateJob(ctx, &pppv1.CreateJobRequest{
		TreeId: treeID, Filename: filename, Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	waitForFile(t, mmapFinalPath(r1Path, treeID, filename), content, 30*time.Second)
	waitForFile(t, mmapFinalPath(r2Path, treeID, filename), content, 30*time.Second)

	// A member fetches from a root (on-demand: GetPiece triggers the
	// full-file subtask download).
	mPort := freePort(t)
	mAddr := fmt.Sprintf("127.0.0.1:%d", mPort)
	startService(t, logDir, "m", mAddr, ctlAddr, "member", mPath)
	mData := newDataClient(t, mAddr)
	waitPieceState(t, mData, treeID, filename, 0, int64(len(content)), pppv1.Error_ERROR_CODE_UNSPECIFIED, 30*time.Second)
	waitForFile(t, mmapFinalPath(mPath, treeID, filename), content, 15*time.Second)

	// Kill the primary root; the surviving root still serves a member.
	if err := r1.killAndWait(); err != nil {
		t.Logf("primary root exit: %v", err)
	}
	// The member's pieces were cached, so its own GetPiece is a store hit;
	// the real fault-tolerance check is that the surviving root serves a
	// direct GetPiece after the primary died.
	r2Data := newDataClient(t, r2Addr)
	waitPieceState(t, r2Data, treeID, filename, 0, int64(len(content)), pppv1.Error_ERROR_CODE_UNSPECIFIED, 15*time.Second)
}
