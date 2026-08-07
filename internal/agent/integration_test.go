package agent

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/ctl"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ctlTestPGDSN is the PostgreSQL DSN for the in-process ctl used by the agent
// integration tests (Docker PG; tests skip when unreachable).
const ctlTestPGDSN = "postgres://ppp:ppp@127.0.0.1:25433/ppp_test_agent"

// truncateCtlPG clears the shared ctl test tables so the in-process ctl starts
// clean (skips when PostgreSQL is unreachable).
func truncateCtlPG(t *testing.T) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), ctlTestPGDSN)
	if err != nil {
		t.Skipf("PostgreSQL not reachable at %s (%v); skipping", ctlTestPGDSN, err)
	}
	defer pool.Close()
	// The agent test database may be brand-new; ensure the schema first.
	if err := ctl.MigrateSchema(context.Background(), pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(context.Background(),
		`TRUNCATE trees, nodes, jobs, banned, progress, meta, ctl_leader RESTART IDENTITY CASCADE`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// waitFor polls cond until it returns true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestIntegrationCtlTwoAgents walks the phase-2 happy path over real gRPC:
// in-process ctl + root + member agents; the root pulls a file from an HTTP
// source on a job, the member pulls a piece from the root, then a cancel bans
// the file (GetPiece -> BANNED) and an unban restores service.
func TestIntegrationCtlTwoAgents(t *testing.T) {
	// --- HTTP source ---
	content := make([]byte, PieceSize+100)
	for i := range content {
		content[i] = byte(i % 251)
	}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeContent(w, r, "file.bin", time.Time{}, strings.NewReader(string(content)))
	}))
	defer httpSrv.Close()

	// --- in-process ctl ---
	ctlCtx, ctlCancel := context.WithCancel(context.Background())
	defer ctlCancel()
	truncateCtlPG(t)
	ctlCfg := ctl.DefaultConfig()
	ctlCfg.PGDSN = ctlTestPGDSN
	ctlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ctl listen: %v", err)
	}
	_, ctlDone, err := ctl.ServeControl(ctlCtx, ctlCfg, ctlLis)
	if err != nil {
		t.Fatalf("ServeControl: %v", err)
	}
	defer func() { ctlCancel(); <-ctlDone }()

	conn, err := grpc.NewClient(ctlLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("ctl client: %v", err)
	}
	defer conn.Close()
	ctlClient := pppv1.NewControlClient(conn)

	// --- create the tree with the HTTP source ---
	if _, err := ctlClient.CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{
			Id: "t1", App: "app", Environment: "prod", Idc: "idc1",
			RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}},
		},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	// --- start two agents (root + member) ---
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
	member := startAgent("member", pppv1.Node_MEMBER)

	// The member's topology must include the root as its upstream.
	waitFor(t, 5*time.Second, "member upstream = root", func() bool {
		as := member.UpstreamAddrs()
		return len(as) == 1 && as[0] == root.Addr()
	})

	// --- create a job; the root pulls the file from the HTTP source ---
	if _, err := ctlClient.CreateJob(context.Background(), &pppv1.CreateJobRequest{
		TreeId: "t1", Filename: "file.bin", Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	waitFor(t, 10*time.Second, "root has the file complete", func() bool {
		return root.store.IsComplete("file.bin")
	})

	// --- member GetPiece pulls from the root ---
	memberClient := pppv1.NewDataClient(mustDial(t, member.Addr()))
	piece, err := memberClient.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		Index: 0, Size: int64(len(content)), JobId: "local:test",
	})
	if err != nil {
		t.Fatalf("member GetPiece: %v", err)
	}
	pieceData := getPieceData(t, piece)
	if len(pieceData) != int(PieceSize) || pieceData[0] != content[0] {
		t.Fatalf("member piece 0 len=%d first=%d, want %d/%d", len(pieceData), pieceData[0], PieceSize, content[0])
	}
	// The member cached the file (subtask full download).
	waitFor(t, 10*time.Second, "member cached the file", func() bool {
		return member.store.HasPiece("file.bin", 1)
	})
	// A second piece request is a local hit.
	piece2, err := memberClient.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		Index: 1, Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("member GetPiece(1): %v", err)
	}
	if data := getPieceData(t, piece2); int64(len(data)) != 100 {
		t.Fatalf("member piece 1 len=%d, want 100", len(data))
	}

	// --- cancel bans the file; the member must reject GetPiece ---
	if _, err := ctlClient.CancelJob(context.Background(), &pppv1.CancelJobRequest{
		TreeId: "t1", Filename: "file.bin", Reason: "test cancel",
	}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	waitFor(t, 5*time.Second, "member banned the file", func() bool {
		return member.banned.IsBanned("t1", "file.bin")
	})
	banned, err := memberClient.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		Index: 0, Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("GetPiece after ban: %v", err)
	}
	if errCode := getPieceError(t, banned); errCode != pppv1.Error_BANNED {
		t.Fatalf("GetPiece after ban code = %v, want BANNED", errCode)
	}

	// --- unban restores service ---
	if _, err := ctlClient.Unban(context.Background(), &pppv1.UnbanRequest{TreeId: "t1", Filename: "file.bin"}); err != nil {
		t.Fatalf("Unban: %v", err)
	}
	waitFor(t, 5*time.Second, "member unbanned the file", func() bool {
		return !member.banned.IsBanned("t1", "file.bin")
	})
	// The cancel removed the local artifacts (P2-6). Under the BUILDING
	// semantics a root's rebuild is Job-driven (decision 1): a fresh job
	// re-creates the artifact before the member can fetch it again.
	if _, err := ctlClient.CreateJob(context.Background(), &pppv1.CreateJobRequest{
		TreeId: "t1", Filename: "file.bin", Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("CreateJob after unban: %v", err)
	}
	waitFor(t, 15*time.Second, "root rebuilt the file", func() bool {
		return root.store.IsComplete("file.bin")
	})
	after, err := memberClient.GetPiece(context.Background(), &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: "t1", Filename: "file.bin"},
		Index: 0, Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("GetPiece after unban: %v", err)
	}
	if data := getPieceData(t, after); len(data) != int(PieceSize) {
		t.Fatalf("GetPiece after unban len=%d, want %d", len(data), PieceSize)
	}
}

// mustDial dials a Data server address with a message limit large enough for
// full-size pieces.
func mustDial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(maxGRPCMessageSize),
		),
	)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func getPieceData(t *testing.T, resp *pppv1.GetPieceResponse) []byte {
	t.Helper()
	switch r := resp.GetResult().(type) {
	case *pppv1.GetPieceResponse_Piece:
		return r.Piece.GetData()
	default:
		t.Fatalf("response = %v, want a piece", resp)
		return nil
	}
}

func getPieceError(t *testing.T, resp *pppv1.GetPieceResponse) pppv1.Error_ErrorCode {
	t.Helper()
	switch r := resp.GetResult().(type) {
	case *pppv1.GetPieceResponse_Error:
		return r.Error.GetCode()
	default:
		t.Fatalf("response = %v, want an error", resp)
		return pppv1.Error_ERROR_CODE_UNSPECIFIED
	}
}

// TestIntegrationDataServerSubscribeLease verifies subscribe/unsubscribe and
// lease expiry on a real agent Data server.
func TestIntegrationDataServerSubscribeLease(t *testing.T) {
	content := []byte("0123456789")
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer httpSrv.Close()

	ctlCtx, ctlCancel := context.WithCancel(context.Background())
	defer ctlCancel()
	truncateCtlPG(t)
	ctlCfg := ctl.DefaultConfig()
	ctlCfg.PGDSN = ctlTestPGDSN
	ctlLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ctl listen: %v", err)
	}
	_, ctlDone, err := ctl.ServeControl(ctlCtx, ctlCfg, ctlLis)
	if err != nil {
		t.Fatalf("ServeControl: %v", err)
	}
	defer func() { ctlCancel(); <-ctlDone }()

	conn, err := grpc.NewClient(ctlLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("ctl client: %v", err)
	}
	defer conn.Close()
	if _, err := pppv1.NewControlClient(conn).CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t1", RootCount: 1, GroupMembers: 2, GroupChildren: 2,
			Source: &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{httpSrv.URL}}},
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
	cfg.LeaseTTL = 100 * time.Millisecond
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

	dataClient := pppv1.NewDataClient(mustDial(t, ag.Addr()))
	sub, err := dataClient.Subscribe(context.Background(), &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}, JobId: "job:1", ChildNodeId: "c1", LeaseSeconds: 1,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if !sub.GetAccepted() || sub.GetBanned() {
		t.Fatalf("Subscribe = %v, want accepted", sub)
	}
	// Renew is idempotent.
	if _, err := dataClient.Subscribe(context.Background(), &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}, JobId: "job:1", ChildNodeId: "c1", LeaseSeconds: 1,
	}); err != nil {
		t.Fatalf("Subscribe renew: %v", err)
	}
	if ag.leases.Count() != 1 {
		t.Fatalf("lease count = %d, want 1", ag.leases.Count())
	}
	unsub, err := dataClient.Unsubscribe(context.Background(), &pppv1.UnsubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}, JobId: "job:1", ChildNodeId: "c1",
	})
	if err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if !unsub.GetOk() || ag.leases.Count() != 0 {
		t.Fatalf("Unsubscribe ok=%v count=%d, want ok + 0", unsub.GetOk(), ag.leases.Count())
	}

	// Lease expiry by the background scan.
	if _, err := dataClient.Subscribe(context.Background(), &pppv1.SubscribeRequest{
		Key: &pppv1.TreeKey{TreeId: "t1", Filename: "f.bin"}, JobId: "job:1", ChildNodeId: "c1",
	}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	waitFor(t, 5*time.Second, "lease expiry scan", func() bool {
		return ag.leases.Count() == 0
	})
}

// TestAgentRegisterRetry verifies the B-2 fix: if the ctl is briefly
// unreachable (or a follower during the election window) at startup, the
// initial RegisterNode retries with backoff instead of killing the process.
func TestAgentRegisterRetry(t *testing.T) {
	truncateCtlPG(t)
	// Reserve a loopback port and free it so the ctl is initially DOWN.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probe listen: %v", err)
	}
	ctlAddr := probe.Addr().String()
	_ = probe.Close()

	cfg := DefaultConfig()
	cfg.ID = "root"
	cfg.Addr = "127.0.0.1:0"
	cfg.CtlAddr = ctlAddr
	cfg.Tree = "t1"
	cfg.Role = pppv1.Node_ROOT
	cfg.DownloadPath = filepath.Join(t.TempDir(), "data")
	cfg.HeartbeatInterval = 200 * time.Millisecond
	ag, err := NewAgent(cfg)
	if err != nil {
		t.Fatalf("NewAgent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	startErr := make(chan error, 1)
	go func() { startErr <- ag.Start(ctx) }()

	// Give the first register attempts time to fail (the ctl is down).
	time.Sleep(300 * time.Millisecond)

	// Bring the ctl up on the same address; the retry must succeed.
	ctlLis, err := net.Listen("tcp", ctlAddr)
	if err != nil {
		t.Fatalf("ctl re-listen: %v", err)
	}
	ctlCfg := ctl.DefaultConfig()
	ctlCfg.PGDSN = ctlTestPGDSN
	ctlCfg.HTTPAddr = "127.0.0.1:0"
	ctlCtx, ctlCancel := context.WithCancel(context.Background())
	defer ctlCancel()
	_, ctlDone, err := ctl.ServeControl(ctlCtx, ctlCfg, ctlLis)
	if err != nil {
		t.Fatalf("ServeControl: %v", err)
	}
	defer func() { ctlCancel(); <-ctlDone }()

	// The tree must exist before the ctl accepts the node registration.
	ctlConn, err := grpc.NewClient(ctlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial ctl: %v", err)
	}
	defer ctlConn.Close()
	if _, err := pppv1.NewControlClient(ctlConn).CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t1", RootCount: 1, GroupMembers: 2, GroupChildren: 2},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("agent Start after ctl recovery: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("agent did not register after the ctl recovered")
	}
	if ag.Addr() == "" {
		t.Fatal("agent did not bind a Data address")
	}
}
