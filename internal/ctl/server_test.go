package ctl

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// testNow is the shared fake clock. Tests that mutate it must not run in
// parallel.
var testNow = time.Unix(1_000_000, 0)

// newTestServer builds a server over a temp bbolt store with a fixed clock.
func newTestServer(t *testing.T) *Server {
	t.Helper()
	st, err := OpenStore(filepath.Join(t.TempDir(), "ctl.db"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	srv := NewServer(st, DefaultConfig())
	srv.now = func() time.Time { return testNow }
	return srv
}

// createTree is a shorthand that asserts success.
func createTree(t *testing.T, srv *Server, id string, rootCount int32) {
	t.Helper()
	_, err := srv.CreateTree(context.Background(), &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: id, App: "app", Environment: "prod", Idc: "idc1", RootCount: rootCount, GroupMembers: 2, GroupChildren: 2},
	})
	if err != nil {
		t.Fatalf("CreateTree(%s): %v", id, err)
	}
}

func registerNode(t *testing.T, srv *Server, id, treeID, addr string, role pppv1.Node_Role) *pppv1.RegisterNodeResponse {
	t.Helper()
	resp, err := srv.RegisterNode(context.Background(), &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: id, Addr: addr, TreeId: treeID, Role: role},
	})
	if err != nil {
		t.Fatalf("RegisterNode(%s): %v", id, err)
	}
	return resp
}

// TestCreateTreeValidationAndDefaults covers validation and default group
// sizes applied on create.
func TestCreateTreeValidationAndDefaults(t *testing.T) {
	srv := newTestServer(t)
	ctx := context.Background()

	if _, err := srv.CreateTree(ctx, &pppv1.CreateTreeRequest{Tree: nil}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateTree(nil) code = %v, want InvalidArgument", status.Code(err))
	}
	if _, err := srv.CreateTree(ctx, &pppv1.CreateTreeRequest{Tree: &pppv1.Tree{}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("CreateTree(empty id) code = %v, want InvalidArgument", status.Code(err))
	}

	tr := &pppv1.Tree{Id: "t1", GroupMembers: 0, GroupChildren: 0}
	resp, err := srv.CreateTree(ctx, &pppv1.CreateTreeRequest{Tree: tr})
	if err != nil {
		t.Fatalf("CreateTree: %v", err)
	}
	if resp.GetTree().GetGroupMembers() != int32(DefaultConfig().DefaultGroupMembers) ||
		resp.GetTree().GetGroupChildren() != int32(DefaultConfig().DefaultGroupChildren) {
		t.Fatalf("defaults not applied: %v", resp.GetTree())
	}

	if _, err := srv.CreateTree(ctx, &pppv1.CreateTreeRequest{Tree: tr}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate CreateTree code = %v, want AlreadyExists", status.Code(err))
	}
}

// TestRegisterNodeTopology verifies topology recomputation, generation bumps
// on node-set changes and idempotent re-registration.
func TestRegisterNodeTopology(t *testing.T) {
	srv := newTestServer(t)
	createTree(t, srv, "t1", 3)

	r1 := registerNode(t, srv, "r1", "t1", "10.0.0.1", pppv1.Node_ROOT)
	if up := r1.GetTopology().GetNodeUpstreams()["r1"]; !up.GetPullFromSource() || len(up.GetAddrs()) != 0 {
		t.Fatalf("r1 upstream = %v, want PullFromSource", up)
	}
	if r1.GetTopology().GetGeneration() != 1 {
		t.Fatalf("generation after first root = %d, want 1", r1.GetTopology().GetGeneration())
	}

	m1 := registerNode(t, srv, "m01", "t1", "10.0.0.11", pppv1.Node_MEMBER)
	if up := m1.GetTopology().GetNodeUpstreams()["m01"]; len(up.GetAddrs()) != 1 || up.GetAddrs()[0] != "10.0.0.1" {
		t.Fatalf("m01 upstream = %v, want [10.0.0.1]", up.GetAddrs())
	}
	if m1.GetTopology().GetGeneration() != 2 {
		t.Fatalf("generation after member = %d, want 2", m1.GetTopology().GetGeneration())
	}

	// Re-registering the same node with identical data must not bump the
	// generation or change the topology.
	again := registerNode(t, srv, "m01", "t1", "10.0.0.11", pppv1.Node_MEMBER)
	if again.GetTopology().GetGeneration() != 2 {
		t.Fatalf("generation after re-register = %d, want 2 (no change)", again.GetTopology().GetGeneration())
	}

	// Changing the address bumps the generation (topology changed).
	moved := registerNode(t, srv, "m01", "t1", "10.0.0.99", pppv1.Node_MEMBER)
	if moved.GetTopology().GetGeneration() != 3 {
		t.Fatalf("generation after addr change = %d, want 3", moved.GetTopology().GetGeneration())
	}
}

// TestRegisterNodeMissingTree verifies registration requires an existing tree.
func TestRegisterNodeMissingTree(t *testing.T) {
	srv := newTestServer(t)
	_, err := srv.RegisterNode(context.Background(), &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: "r1", Addr: "10.0.0.1", TreeId: "nope", Role: pppv1.Node_ROOT},
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("RegisterNode on missing tree code = %v, want NotFound", status.Code(err))
	}
}

// TestRegisterNodeRootCountReject verifies root_count enforcement.
func TestRegisterNodeRootCountReject(t *testing.T) {
	srv := newTestServer(t)
	createTree(t, srv, "t1", 1) // root_count = 1

	registerNode(t, srv, "r1", "t1", "10.0.0.1", pppv1.Node_ROOT)
	_, err := srv.RegisterNode(context.Background(), &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: "r2", Addr: "10.0.0.2", TreeId: "t1", Role: pppv1.Node_ROOT},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second root code = %v, want FailedPrecondition", status.Code(err))
	}

	// Members are unlimited even when the root quota is full.
	memberResp := registerNode(t, srv, "m01", "t1", "10.0.0.11", pppv1.Node_MEMBER)
	if memberResp.GetTopology() == nil {
		t.Fatal("member registration failed unexpectedly")
	}
}

// TestHeartbeat verifies heartbeat updates last_seen and returns generations;
// unknown nodes are rejected.
func TestHeartbeat(t *testing.T) {
	srv := newTestServer(t)
	createTree(t, srv, "t1", 3)
	registerNode(t, srv, "r1", "t1", "10.0.0.1", pppv1.Node_ROOT)

	resp, err := srv.Heartbeat(context.Background(), &pppv1.HeartbeatRequest{
		Node: &pppv1.Node{Id: "r1"},
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if resp.GetTopologyGeneration() != 1 || resp.GetBannedGeneration() != 0 {
		t.Fatalf("Heartbeat gens = (%d,%d), want (1,0)", resp.GetTopologyGeneration(), resp.GetBannedGeneration())
	}

	// Unknown node.
	if _, err := srv.Heartbeat(context.Background(), &pppv1.HeartbeatRequest{
		Node: &pppv1.Node{Id: "ghost"},
	}); status.Code(err) != codes.NotFound {
		t.Fatalf("Heartbeat(ghost) code = %v, want NotFound", status.Code(err))
	}
}

// TestHeartbeatTimeoutPrune verifies that a node silent beyond the timeout is
// removed, its topology entry is dropped and the generation is bumped.
func TestHeartbeatTimeoutPrune(t *testing.T) {
	srv := newTestServer(t)
	srv.cfg.HeartbeatTimeout = 30 * time.Second
	createTree(t, srv, "t1", 3)
	registerNode(t, srv, "r1", "t1", "10.0.0.1", pppv1.Node_ROOT)
	registerNode(t, srv, "m01", "t1", "10.0.0.11", pppv1.Node_MEMBER)
	if got := len(srv.regNodesByTree("t1")); got != 2 {
		t.Fatalf("registered nodes before prune = %d, want 2", got)
	}

	// Keep the root alive with a heartbeat; the member stays stale.
	testNow = testNow.Add(10 * time.Second)
	if _, err := srv.Heartbeat(context.Background(), &pppv1.HeartbeatRequest{
		Node: &pppv1.Node{Id: "r1"},
	}); err != nil {
		t.Fatalf("Heartbeat r1: %v", err)
	}

	// No pruning while every heartbeat is fresh.
	srv.pruneExpired()
	if got := len(srv.regNodesByTree("t1")); got != 2 {
		t.Fatalf("nodes after fresh prune = %d, want 2", got)
	}

	// Past the timeout the stale member is pruned; the heartbeated root stays.
	// The tree still has a root so the topology is recomputed and the
	// generation bumps.
	testNow = testNow.Add(25 * time.Second) // member age 35s > 30s, root age 25s < 30s
	srv.pruneExpired()
	if srv.regGet("m01") != nil {
		t.Fatal("m01 still registered after timeout prune")
	}
	if srv.regGet("r1") == nil {
		t.Fatal("r1 pruned although its heartbeat was fresh")
	}
	if got := len(srv.regNodesByTree("t1")); got != 1 {
		t.Fatalf("registered nodes after prune = %d, want 1", got)
	}
	topo, gen := srv.currentTopologyLocked(&pppv1.Tree{Id: "t1", GroupMembers: 2, GroupChildren: 2})
	if gen != 3 {
		t.Fatalf("generation after prune = %d, want 3", gen)
	}
	if topo == nil || topo.GetNodeUpstreams()["m01"] != nil {
		t.Fatal("pruned member still present in topology")
	}
}

// TestCreateJobRejectsBanned verifies that a banned file cannot get a new job.
func TestCreateJobRejectsBanned(t *testing.T) {
	srv := newTestServer(t)
	createTree(t, srv, "t1", 3)
	if _, _, err := srv.store.AddBanned(&pppv1.BannedFile{TreeId: "t1", Filename: "a.bin"}); err != nil {
		t.Fatalf("AddBanned: %v", err)
	}

	_, err := srv.CreateJob(context.Background(), &pppv1.CreateJobRequest{TreeId: "t1", Filename: "a.bin"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateJob(banned) code = %v, want FailedPrecondition", status.Code(err))
	}
}

// TestCancelJobAndUnban verifies the cancel/unban state transitions via direct
// server calls.
func TestCancelJobAndUnban(t *testing.T) {
	srv := newTestServer(t)
	createTree(t, srv, "t1", 3)
	registerNode(t, srv, "r1", "t1", "10.0.0.1", pppv1.Node_ROOT)

	jobResp, err := srv.CreateJob(context.Background(), &pppv1.CreateJobRequest{TreeId: "t1", Filename: "a.bin", Size: 100})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job := jobResp.GetJob()
	if !strings.HasPrefix(job.GetId(), "job:") || job.GetState() != pppv1.Job_DISTRIBUTING {
		t.Fatalf("job = id %q state %v, want job: prefix + DISTRIBUTING", job.GetId(), job.GetState())
	}

	// Cancel by job_id only.
	cancelResp, err := srv.CancelJob(context.Background(), &pppv1.CancelJobRequest{JobId: job.GetId(), Reason: "mistake"})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if !cancelResp.GetCanceled() || cancelResp.GetAlreadyBanned() || cancelResp.GetAffectedJobs() != 1 {
		t.Fatalf("CancelJob response = %v, want canceled=true affected=1", cancelResp)
	}
	got, _ := srv.store.GetBanned("t1", "a.bin")
	if got == nil || got.GetReason() != "mistake" || got.GetJobId() != job.GetId() {
		t.Fatalf("banned record = %v, want reason=mistake job=%s", got, job.GetId())
	}
	q, _ := srv.store.GetJob(job.GetId())
	if q.GetState() != pppv1.Job_CANCELED {
		t.Fatalf("job state after cancel = %v, want CANCELED", q.GetState())
	}

	// Canceling again is idempotent: already_banned, no extra affected jobs.
	again, err := srv.CancelJob(context.Background(), &pppv1.CancelJobRequest{JobId: job.GetId()})
	if err != nil {
		t.Fatalf("CancelJob again: %v", err)
	}
	if !again.GetAlreadyBanned() || again.GetAffectedJobs() != 0 {
		t.Fatalf("re-cancel response = %v, want already_banned affected=0", again)
	}

	// Unban removes the record.
	unban, err := srv.Unban(context.Background(), &pppv1.UnbanRequest{TreeId: "t1", Filename: "a.bin"})
	if err != nil {
		t.Fatalf("Unban: %v", err)
	}
	if !unban.GetUnbanned() {
		t.Fatal("Unban response unbanned=false, want true")
	}
	if _, err := srv.store.GetBanned("t1", "a.bin"); err != ErrNotFound {
		t.Fatalf("banned record after unban = %v, want ErrNotFound", err)
	}
}

// TestListJobsPagination verifies filtering and offset-token pagination.
func TestListJobsPagination(t *testing.T) {
	srv := newTestServer(t)
	createTree(t, srv, "t1", 3)
	for i := 0; i < 5; i++ {
		if _, err := srv.CreateJob(context.Background(), &pppv1.CreateJobRequest{
			TreeId: "t1", Filename: "f" + string(rune('a'+i)) + ".bin",
		}); err != nil {
			t.Fatalf("CreateJob %d: %v", i, err)
		}
	}

	// Page 1 of 2.
	page1, err := srv.ListJobs(context.Background(), &pppv1.ListJobsRequest{TreeId: "t1", PageSize: 2})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(page1.GetJobs()) != 2 || page1.GetNextPageToken() == "" {
		t.Fatalf("page1 = %d jobs token %q, want 2 jobs + token", len(page1.GetJobs()), page1.GetNextPageToken())
	}
	// Page 2.
	page2, err := srv.ListJobs(context.Background(), &pppv1.ListJobsRequest{TreeId: "t1", PageSize: 2, PageToken: page1.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ListJobs page2: %v", err)
	}
	if len(page2.GetJobs()) != 2 {
		t.Fatalf("page2 jobs = %d, want 2", len(page2.GetJobs()))
	}
	// Page 3 (last).
	page3, err := srv.ListJobs(context.Background(), &pppv1.ListJobsRequest{TreeId: "t1", PageSize: 2, PageToken: page2.GetNextPageToken()})
	if err != nil {
		t.Fatalf("ListJobs page3: %v", err)
	}
	if len(page3.GetJobs()) != 1 || page3.GetNextPageToken() != "" {
		t.Fatalf("page3 = %d jobs token %q, want 1 job no token", len(page3.GetJobs()), page3.GetNextPageToken())
	}

	// Filter by state after canceling one job.
	if _, err := srv.CancelJob(context.Background(), &pppv1.CancelJobRequest{TreeId: "t1", Filename: "fa.bin"}); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	canceled, err := srv.ListJobs(context.Background(), &pppv1.ListJobsRequest{TreeId: "t1", State: pppv1.Job_CANCELED})
	if err != nil {
		t.Fatalf("ListJobs(canceled): %v", err)
	}
	if len(canceled.GetJobs()) != 1 {
		t.Fatalf("canceled jobs = %d, want 1", len(canceled.GetJobs()))
	}
}

// TestUUIDFormat checks the job id prefix and UUID shape.
func TestUUIDFormat(t *testing.T) {
	id, err := newUUID()
	if err != nil {
		t.Fatalf("newUUID: %v", err)
	}
	// 8-4-4-4-12 hex.
	parts := strings.Split(id, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		t.Fatalf("uuid %q does not match 8-4-4-4-12", id)
	}
	if parts[2][0] != '4' {
		t.Fatalf("uuid %q version != 4", id)
	}
}

// ============ Real gRPC integration ============

// startTestGRPC serves a real Control server on a loopback port and returns a
// connected client plus a cleanup func.
func startTestGRPC(t *testing.T) (pppv1.ControlClient, func()) {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DBPath = filepath.Join(t.TempDir(), "ctl.db")
	ctx, cancel := context.WithCancel(context.Background())
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs, err := ServeControl(ctx, cfg, lis)
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
		conn.Close()
		gs.Stop()
		cancel()
	}
	return pppv1.NewControlClient(conn), cleanup
}

// recvBanned reads one update from a banned watch stream with a timeout.
func recvBanned(t *testing.T, stream pppv1.Control_WatchBannedListClient) *pppv1.BannedListUpdate {
	t.Helper()
	type res struct {
		up  *pppv1.BannedListUpdate
		err error
	}
	ch := make(chan res, 1)
	go func() { up, err := stream.Recv(); ch <- res{up, err} }()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv banned: %v", r.err)
		}
		return r.up
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for banned update")
		return nil
	}
}

func recvJob(t *testing.T, stream pppv1.Control_WatchJobsClient) *pppv1.JobUpdate {
	t.Helper()
	type res struct {
		up  *pppv1.JobUpdate
		err error
	}
	ch := make(chan res, 1)
	go func() { up, err := stream.Recv(); ch <- res{up, err} }()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv job: %v", r.err)
		}
		return r.up
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for job update")
		return nil
	}
}

// TestGRPCCancelFlow walks the whole cancel lifecycle over real gRPC: create
// tree -> register nodes -> watch streams -> create job -> cancel -> unban.
func TestGRPCCancelFlow(t *testing.T) {
	client, cleanup := startTestGRPC(t)
	defer cleanup()
	ctx := context.Background()

	// 1. Create tree.
	treeResp, err := client.CreateTree(ctx, &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t1", App: "app", Environment: "prod", Idc: "idc1", RootCount: 3, GroupMembers: 2, GroupChildren: 2},
	})
	if err != nil {
		t.Fatalf("CreateTree: %v", err)
	}
	if treeResp.GetTree().GetId() != "t1" {
		t.Fatalf("tree id = %q, want t1", treeResp.GetTree().GetId())
	}

	// 2. Register two nodes; the second registration must include the first
	// node's address in its topology.
	r1, err := client.RegisterNode(ctx, &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: "r1", Addr: "10.0.0.1", TreeId: "t1", Role: pppv1.Node_ROOT},
	})
	if err != nil {
		t.Fatalf("RegisterNode r1: %v", err)
	}
	if !r1.GetTopology().GetNodeUpstreams()["r1"].GetPullFromSource() {
		t.Fatal("r1 should pull from source")
	}
	m1, err := client.RegisterNode(ctx, &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: "m01", Addr: "10.0.0.11", TreeId: "t1", Role: pppv1.Node_MEMBER},
	})
	if err != nil {
		t.Fatalf("RegisterNode m01: %v", err)
	}
	if up := m1.GetTopology().GetNodeUpstreams()["m01"]; len(up.GetAddrs()) != 1 || up.GetAddrs()[0] != "10.0.0.1" {
		t.Fatalf("m01 upstream = %v, want [10.0.0.1]", up.GetAddrs())
	}

	// 3. Watch the banned list: the first message is the authoritative full
	// snapshot.
	bannedStream, err := client.WatchBannedList(ctx, &pppv1.WatchBannedListRequest{TreeId: "t1"})
	if err != nil {
		t.Fatalf("WatchBannedList: %v", err)
	}
	snap := recvBanned(t, bannedStream)
	if !snap.GetFullSnapshot() || len(snap.GetSnapshot()) != 0 {
		t.Fatalf("initial banned update = %v, want empty full snapshot", snap)
	}

	// 4. Watch jobs and create a job.
	jobStream, err := client.WatchJobs(ctx, &pppv1.WatchJobsRequest{TreeId: "t1"})
	if err != nil {
		t.Fatalf("WatchJobs: %v", err)
	}
	jobResp, err := client.CreateJob(ctx, &pppv1.CreateJobRequest{TreeId: "t1", Filename: "a.bin", Size: 1024})
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}
	job := jobResp.GetJob()
	if !strings.HasPrefix(job.GetId(), "job:") || job.GetState() != pppv1.Job_DISTRIBUTING {
		t.Fatalf("job = %v, want job: prefix + DISTRIBUTING", job)
	}
	jobPush := recvJob(t, jobStream)
	if jobPush.GetRemoved() || jobPush.GetJob().GetId() != job.GetId() {
		t.Fatalf("job push = %v, want new job %s", jobPush, job.GetId())
	}

	// 5. Cancel by job_id: banned added + job removed pushed, job canceled.
	cancelResp, err := client.CancelJob(ctx, &pppv1.CancelJobRequest{JobId: job.GetId(), Reason: "mistake"})
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
	if !cancelResp.GetCanceled() || cancelResp.GetAffectedJobs() != 1 {
		t.Fatalf("CancelJob response = %v, want canceled affected=1", cancelResp)
	}
	bannedUp := recvBanned(t, bannedStream)
	if len(bannedUp.GetAdded()) != 1 || bannedUp.GetAdded()[0].GetFilename() != "a.bin" {
		t.Fatalf("banned push = %v, want added a.bin", bannedUp)
	}
	removedUp := recvJob(t, jobStream)
	if !removedUp.GetRemoved() || removedUp.GetJob().GetId() != job.GetId() {
		t.Fatalf("removed push = %v, want removed %s", removedUp, job.GetId())
	}
	q, err := client.QueryJob(ctx, &pppv1.QueryJobRequest{JobId: job.GetId()})
	if err != nil {
		t.Fatalf("QueryJob: %v", err)
	}
	if q.GetJob().GetState() != pppv1.Job_CANCELED {
		t.Fatalf("job state = %v, want CANCELED", q.GetJob().GetState())
	}

	// 6. Unban: removed push + sync reflects the empty list at a higher gen.
	unbanResp, err := client.Unban(ctx, &pppv1.UnbanRequest{TreeId: "t1", Filename: "a.bin"})
	if err != nil {
		t.Fatalf("Unban: %v", err)
	}
	if !unbanResp.GetUnbanned() {
		t.Fatal("unban returned false")
	}
	removedBanned := recvBanned(t, bannedStream)
	if len(removedBanned.GetRemoved()) != 1 || removedBanned.GetRemoved()[0].GetFilename() != "a.bin" {
		t.Fatalf("unban push = %v, want removed a.bin", removedBanned)
	}
	syncResp, err := client.SyncBannedList(ctx, &pppv1.SyncBannedListRequest{TreeId: "t1"})
	if err != nil {
		t.Fatalf("SyncBannedList: %v", err)
	}
	if len(syncResp.GetBanned()) != 0 {
		t.Fatalf("banned list after unban = %v, want empty", syncResp.GetBanned())
	}
	if syncResp.GetGeneration() != 2 { // one add + one remove
		t.Fatalf("banned generation = %d, want 2", syncResp.GetGeneration())
	}

	// 7. Progress stream is accepted.
	progress, err := client.SyncProgress(ctx)
	if err != nil {
		t.Fatalf("SyncProgress open: %v", err)
	}
	if err := progress.Send(&pppv1.ProgressRecord{
		State:  &pppv1.ProgressState{TreeId: "t1", Filename: "a.bin", Progress: 50},
		NodeId: "m01",
	}); err != nil {
		t.Fatalf("SyncProgress send: %v", err)
	}
	if err := progress.CloseSend(); err != nil {
		t.Fatalf("SyncProgress CloseSend: %v", err)
	}
	pr, err := progress.CloseAndRecv()
	if err != nil {
		t.Fatalf("SyncProgress close: %v", err)
	}
	if !pr.GetOk() {
		t.Fatal("SyncProgress response ok=false, want true")
	}
}

// TestGRPCWatchTopology verifies the topology watch delivers a full topology
// update when the node set changes.
func TestGRPCWatchTopology(t *testing.T) {
	client, cleanup := startTestGRPC(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.CreateTree(ctx, &pppv1.CreateTreeRequest{
		Tree: &pppv1.Tree{Id: "t1", App: "app", Environment: "prod", Idc: "idc1", RootCount: 3, GroupMembers: 2, GroupChildren: 2},
	}); err != nil {
		t.Fatalf("CreateTree: %v", err)
	}

	stream, err := client.WatchTopology(ctx, &pppv1.WatchTopologyRequest{TreeId: "t1"})
	if err != nil {
		t.Fatalf("WatchTopology: %v", err)
	}

	// Registering a root must push a full topology update (generation >= 1).
	if _, err := client.RegisterNode(ctx, &pppv1.RegisterNodeRequest{
		Node: &pppv1.Node{Id: "r1", Addr: "10.0.0.1", TreeId: "t1", Role: pppv1.Node_ROOT},
	}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	type res struct {
		up  *pppv1.TopologyUpdate
		err error
	}
	ch := make(chan res, 1)
	go func() { up, err := stream.Recv(); ch <- res{up, err} }()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("recv topology: %v", r.err)
		}
		if r.up.GetGeneration() < 1 || r.up.GetTopology().GetNodeUpstreams()["r1"] == nil {
			t.Fatalf("topology push = %v, want r1 entry with gen >= 1", r.up)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for topology update")
	}
}
