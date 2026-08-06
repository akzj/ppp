package ctl

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/pkg/topology"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the Control gRPC service. Mutable state (the node
// registry and multi-step store updates) is guarded by mu; the fanout and the
// store carry their own synchronization. The store is the durable source of
// truth; the registry is the in-memory mirror of the registered node set.
type Server struct {
	pppv1.UnimplementedControlServer

	store  Store
	fanout *Fanout
	cfg    *Config

	mu sync.Mutex
	// nodes is the in-memory node registry (node_id -> node), loaded from the
	// store at startup and kept in sync on every change.
	nodes map[string]*pppv1.Node

	// now is injectable for tests; defaults to time.Now.
	now func() time.Time
}

// NewServer creates a control-plane server over the given store.
func NewServer(st Store, cfg *Config) *Server {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	return &Server{
		store:  st,
		fanout: newFanout(),
		cfg:    cfg,
		nodes:  make(map[string]*pppv1.Node),
		now:    time.Now,
	}
}

// Start loads the persisted node set into the registry and launches the
// background loops. It returns when ctx is done.
func (s *Server) Start(ctx context.Context) error {
	nodes, err := s.store.ListNodes("")
	if err != nil {
		return fmt.Errorf("ctl: load nodes: %w", err)
	}
	s.mu.Lock()
	for _, n := range nodes {
		s.nodes[n.GetId()] = n
	}
	s.mu.Unlock()
	go s.heartbeatLoop(ctx)
	return nil
}

// Run opens the store, starts the server and serves gRPC on cfg.Addr until
// ctx is done or the listener fails. Intended for main.
func Run(ctx context.Context, cfg *Config) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("ctl: listen %s: %w", cfg.Addr, err)
	}
	_, err = ServeControl(ctx, cfg, lis)
	return err
}

// ServeControl opens the store from cfg.DBPath, starts a Control server and
// serves gRPC on lis until ctx is done, then gracefully stops and closes the
// store. It returns the grpc.Server so tests can stop it explicitly. Callers
// must cancel ctx (or stop the server) to release resources.
func ServeControl(ctx context.Context, cfg *Config, lis net.Listener) (*grpc.Server, error) {
	st, err := OpenStore(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	srv := NewServer(st, cfg)
	if err := srv.Start(ctx); err != nil {
		st.Close()
		return nil, err
	}
	gs := grpc.NewServer()
	pppv1.RegisterControlServer(gs, srv)
	go func() {
		<-ctx.Done()
		gs.GracefulStop()
		st.Close()
	}()
	go func() {
		_ = gs.Serve(lis)
	}()
	return gs, nil
}

// ============ Topology helpers (require s.mu) ============

// currentTopologyLocked computes the tree's current topology and generation
// without changing the generation. It returns nil when the tree has no
// computable topology yet (e.g. no root registered).
func (s *Server) currentTopologyLocked(tree *pppv1.Tree) (*pppv1.Topology, int64) {
	nodes := s.regNodesByTree(tree.GetId())
	topo, err := topology.Build(topology.Options{Tree: tree, Nodes: nodes})
	if err != nil {
		return nil, 0
	}
	gen, err := s.store.TopologyGeneration(tree.GetId())
	if err != nil {
		return nil, 0
	}
	topo.Generation = gen
	return topo, gen
}

// refreshTopologyLocked recomputes the tree's topology after a node-set
// change, bumps the topology generation and returns the new topology.
func (s *Server) refreshTopologyLocked(tree *pppv1.Tree) (*pppv1.Topology, int64, error) {
	nodes := s.regNodesByTree(tree.GetId())
	topo, err := topology.Build(topology.Options{Tree: tree, Nodes: nodes})
	if err != nil {
		return nil, 0, err
	}
	gen, err := s.store.BumpTopologyGeneration(tree.GetId())
	if err != nil {
		return nil, 0, err
	}
	topo.Generation = gen
	return topo, gen, nil
}

// ============ Tree lifecycle ============

func (s *Server) CreateTree(_ context.Context, req *pppv1.CreateTreeRequest) (*pppv1.CreateTreeResponse, error) {
	t := req.GetTree()
	if t == nil || t.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree id is required")
	}
	// Apply the configured default group sizes when the tree does not set them.
	if t.GetGroupMembers() <= 0 {
		t.GroupMembers = int32(s.cfg.DefaultGroupMembers)
	}
	if t.GetGroupChildren() <= 0 {
		t.GroupChildren = int32(s.cfg.DefaultGroupChildren)
	}
	if err := s.store.CreateTree(t); err != nil {
		if errors.Is(err, ErrExists) {
			return nil, status.Errorf(codes.AlreadyExists, "tree %q already exists", t.GetId())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pppv1.CreateTreeResponse{Tree: t}, nil
}

func (s *Server) GetTree(_ context.Context, req *pppv1.GetTreeRequest) (*pppv1.GetTreeResponse, error) {
	if req.GetTreeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id is required")
	}
	tree, err := s.store.GetTree(req.GetTreeId())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tree %q not found", req.GetTreeId())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pppv1.GetTreeResponse{Tree: tree}, nil
}

func (s *Server) ListTrees(context.Context, *pppv1.ListTreesRequest) (*pppv1.ListTreesResponse, error) {
	trees, err := s.store.ListTrees()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pppv1.ListTreesResponse{Trees: trees}, nil
}

func (s *Server) DeleteTree(_ context.Context, req *pppv1.DeleteTreeRequest) (*pppv1.DeleteTreeResponse, error) {
	if req.GetTreeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.store.DeleteTree(req.GetTreeId()); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tree %q not found", req.GetTreeId())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Drop the tree's registered nodes and subscribers too: a stale registry
	// would otherwise poison root_count enforcement and topology if the tree
	// is re-created. (Jobs and banned records are cleaned up in a later phase.)
	for _, n := range s.regNodesByTree(req.GetTreeId()) {
		s.regDelete(n.GetId())
		_ = s.store.DeleteNode(req.GetTreeId(), n.GetId())
	}
	s.fanout.clearTree(req.GetTreeId())
	return &pppv1.DeleteTreeResponse{Deleted: true}, nil
}

// ============ Node onboarding ============

func (s *Server) RegisterNode(_ context.Context, req *pppv1.RegisterNodeRequest) (*pppv1.RegisterNodeResponse, error) {
	node := req.GetNode()
	if node == nil || node.GetId() == "" || node.GetAddr() == "" {
		return nil, status.Error(codes.InvalidArgument, "node id and addr are required")
	}
	if node.GetTreeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node tree_id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tree, err := s.store.GetTree(node.GetTreeId())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tree %q not found; create it before registering nodes", node.GetTreeId())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Root-count enforcement: reject a new ROOT once the tree has its full
	// quota of registered roots (MEMBER registration is unlimited).
	if node.GetRole() == pppv1.Node_ROOT {
		if rc := int(tree.GetRootCount()); rc > 0 && s.regCountRoots(node.GetTreeId(), node.GetId()) >= rc {
			return nil, status.Errorf(codes.FailedPrecondition, "tree %q already has %d roots (root_count=%d)", node.GetTreeId(), rc, rc)
		}
	}

	existing := s.regGet(node.GetId())
	changed := existing == nil ||
		existing.GetAddr() != node.GetAddr() ||
		existing.GetRole() != node.GetRole() ||
		existing.GetTreeId() != node.GetTreeId()

	node.LastHeartbeat = s.now().Unix()
	s.regPut(node)
	if err := s.store.PutNode(node); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Recompute the topology when the node set changed and push it. If the
	// topology is not computable yet (no root registered), register anyway and
	// return an empty topology; the next node change will produce one.
	topo := &pppv1.Topology{TreeId: tree.GetId()}
	pushed := false
	if changed {
		if t, _, err := s.refreshTopologyLocked(tree); err == nil {
			topo = t
			pushed = true
		} else {
			topo.Generation, _ = s.store.TopologyGeneration(tree.GetId())
		}
	} else {
		if t, _ := s.currentTopologyLocked(tree); t != nil {
			topo = t
		} else {
			topo.Generation, _ = s.store.TopologyGeneration(tree.GetId())
		}
	}

	banned, err := s.store.ListBanned(tree.GetId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if pushed {
		s.fanout.publishTopology(tree.GetId(), &pppv1.TopologyUpdate{Generation: topo.GetGeneration(), Topology: topo})
	}
	return &pppv1.RegisterNodeResponse{Tree: tree, Topology: topo, Banned: banned}, nil
}

func (s *Server) Heartbeat(_ context.Context, req *pppv1.HeartbeatRequest) (*pppv1.HeartbeatResponse, error) {
	node := req.GetNode()
	if node == nil || node.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	existing := s.regGet(node.GetId())
	if existing == nil {
		return nil, status.Errorf(codes.NotFound, "node %q not registered; call RegisterNode first", node.GetId())
	}
	existing.LastHeartbeat = s.now().Unix()
	s.regPut(existing)
	if err := s.store.PutNode(existing); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	tg, err := s.store.TopologyGeneration(existing.GetTreeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	bg, err := s.store.BannedGeneration(existing.GetTreeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pppv1.HeartbeatResponse{TopologyGeneration: tg, BannedGeneration: bg}, nil
}

func (s *Server) WatchTopology(req *pppv1.WatchTopologyRequest, stream pppv1.Control_WatchTopologyServer) error {
	if req.GetTreeId() == "" {
		return status.Error(codes.InvalidArgument, "tree_id is required")
	}
	ch := s.fanout.subscribeTopology(req.GetTreeId())
	defer s.fanout.unsubscribeTopology(req.GetTreeId(), ch)

	// Send the current full topology immediately so the stream is
	// self-sufficient; changes afterwards arrive as full snapshots too.
	tree, err := s.store.GetTree(req.GetTreeId())
	var initial *pppv1.TopologyUpdate
	if err == nil {
		s.mu.Lock()
		if topo, gen := s.currentTopologyLocked(tree); topo != nil {
			initial = &pppv1.TopologyUpdate{Generation: gen, Topology: topo}
		}
		s.mu.Unlock()
	}
	if initial != nil {
		if err := stream.Send(initial); err != nil {
			return err
		}
	}

	for {
		select {
		case up := <-ch:
			if err := stream.Send(up); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Server) WatchBannedList(req *pppv1.WatchBannedListRequest, stream pppv1.Control_WatchBannedListServer) error {
	if req.GetTreeId() == "" {
		return status.Error(codes.InvalidArgument, "tree_id is required")
	}
	ch := s.fanout.subscribeBanned(req.GetTreeId())
	defer s.fanout.unsubscribeBanned(req.GetTreeId(), ch)

	// The full snapshot is authoritative and is sent before any increment, so
	// a new subscriber converges even after missed updates.
	banned, err := s.store.ListBanned(req.GetTreeId())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	gen, err := s.store.BannedGeneration(req.GetTreeId())
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if err := stream.Send(&pppv1.BannedListUpdate{Generation: gen, FullSnapshot: true, Snapshot: banned}); err != nil {
		return err
	}

	for {
		select {
		case up := <-ch:
			if err := stream.Send(up); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Server) SyncBannedList(_ context.Context, req *pppv1.SyncBannedListRequest) (*pppv1.SyncBannedListResponse, error) {
	if req.GetTreeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id is required")
	}
	banned, err := s.store.ListBanned(req.GetTreeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	gen, err := s.store.BannedGeneration(req.GetTreeId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pppv1.SyncBannedListResponse{Generation: gen, Banned: banned}, nil
}

// ============ Job orchestration ============

func (s *Server) CreateJob(_ context.Context, req *pppv1.CreateJobRequest) (*pppv1.CreateJobResponse, error) {
	if req.GetTreeId() == "" || req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id and filename are required")
	}

	tree, err := s.store.GetTree(req.GetTreeId())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tree %q not found", req.GetTreeId())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	id, err := newUUID()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	job := &pppv1.Job{
		Id:            "job:" + id,
		TreeId:        req.GetTreeId(),
		Filename:      req.GetFilename(),
		Size:          req.GetSize(),
		Md5:           req.GetMd5(),
		Source:        req.GetSource(),
		TargetNodeIds: req.GetTargetNodeIds(),
		State:         pppv1.Job_CREATED,
		CreatedAt:     s.now().Unix(),
		UpdatedAt:     s.now().Unix(),
	}
	// Fall back to the tree default source when the request does not override it.
	if job.GetSource() == nil {
		job.Source = tree.GetSource()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// A banned file must not be distributed again.
	if _, err := s.store.GetBanned(req.GetTreeId(), req.GetFilename()); err == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "file %q in tree %q is banned; unban it before creating a job", req.GetFilename(), req.GetTreeId())
	}
	if err := s.store.CreateJob(job); err != nil {
		if errors.Is(err, ErrExists) {
			return nil, status.Error(codes.Internal, "job id collision")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Transition CREATED -> DISTRIBUTING: roots are now instructed to fetch.
	job.State = pppv1.Job_DISTRIBUTING
	job.UpdatedAt = s.now().Unix()
	if err := s.store.UpdateJob(job); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.fanout.publishJobs(req.GetTreeId(), &pppv1.JobUpdate{Job: job, Removed: false})
	return &pppv1.CreateJobResponse{Job: job}, nil
}

func (s *Server) QueryJob(_ context.Context, req *pppv1.QueryJobRequest) (*pppv1.QueryJobResponse, error) {
	if req.GetJobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id is required")
	}
	job, err := s.store.GetJob(req.GetJobId())
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "job %q not found", req.GetJobId())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &pppv1.QueryJobResponse{Job: job}, nil
}

func (s *Server) CancelJob(_ context.Context, req *pppv1.CancelJobRequest) (*pppv1.CancelJobResponse, error) {
	// Resolve (tree_id, filename): a center job_id identifies them; otherwise
	// the caller must supply both.
	var treeID, filename string
	if req.GetJobId() != "" {
		job, err := s.store.GetJob(req.GetJobId())
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, status.Errorf(codes.NotFound, "job %q not found", req.GetJobId())
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		treeID, filename = job.GetTreeId(), job.GetFilename()
	} else {
		treeID, filename = req.GetTreeId(), req.GetFilename()
	}
	if treeID == "" || filename == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id and filename are required when job_id is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The core of cancellation: persist (tree_id, filename) in the banned list.
	gen, already, err := s.store.AddBanned(&pppv1.BannedFile{
		TreeId:   treeID,
		Filename: filename,
		Reason:   req.GetReason(),
		JobId:    req.GetJobId(),
		BannedAt: s.now().Unix(),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var bannedPush *pppv1.BannedListUpdate
	if !already {
		bannedPush = &pppv1.BannedListUpdate{
			Generation: gen,
			Added:      []*pppv1.BannedFile{{TreeId: treeID, Filename: filename, Reason: req.GetReason(), JobId: req.GetJobId(), BannedAt: s.now().Unix()}},
		}
	}

	// Cancel every non-terminal center job for the file.
	jobs, err := s.store.JobsByFile(treeID, filename)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	affected := 0
	var removedPushes []*pppv1.JobUpdate
	for _, j := range jobs {
		if j.GetState() == pppv1.Job_CANCELED || j.GetState() == pppv1.Job_SUCCESS || j.GetState() == pppv1.Job_FAILED {
			continue
		}
		j.State = pppv1.Job_CANCELED
		j.UpdatedAt = s.now().Unix()
		if err := s.store.UpdateJob(j); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		affected++
		removedPushes = append(removedPushes, &pppv1.JobUpdate{Job: j, Removed: true})
	}

	if bannedPush != nil {
		s.fanout.publishBanned(treeID, bannedPush)
	}
	for _, up := range removedPushes {
		s.fanout.publishJobs(treeID, up)
	}
	return &pppv1.CancelJobResponse{Canceled: true, AlreadyBanned: already, AffectedJobs: int32(affected)}, nil
}

func (s *Server) Unban(_ context.Context, req *pppv1.UnbanRequest) (*pppv1.UnbanResponse, error) {
	if req.GetTreeId() == "" || req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id and filename are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	gen, removed, err := s.store.RemoveBanned(req.GetTreeId(), req.GetFilename())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if removed {
		s.fanout.publishBanned(req.GetTreeId(), &pppv1.BannedListUpdate{
			Generation: gen,
			Removed:    []*pppv1.TreeKey{{TreeId: req.GetTreeId(), Filename: req.GetFilename()}},
		})
	}
	return &pppv1.UnbanResponse{Unbanned: removed}, nil
}

func (s *Server) ListJobs(_ context.Context, req *pppv1.ListJobsRequest) (*pppv1.ListJobsResponse, error) {
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = 100
	}

	jobs, err := s.store.ListJobs()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var filtered []*pppv1.Job
	for _, j := range jobs {
		if req.GetTreeId() != "" && j.GetTreeId() != req.GetTreeId() {
			continue
		}
		if req.GetState() != pppv1.Job_JOB_STATE_UNSPECIFIED && j.GetState() != req.GetState() {
			continue
		}
		filtered = append(filtered, j)
	}
	// Stable order (created_at, then id) for deterministic pagination.
	sort.Slice(filtered, func(i, j int) bool {
		if filtered[i].GetCreatedAt() != filtered[j].GetCreatedAt() {
			return filtered[i].GetCreatedAt() < filtered[j].GetCreatedAt()
		}
		return filtered[i].GetId() < filtered[j].GetId()
	})

	// page_token is the offset of the first job of the page.
	offset := 0
	if tok := req.GetPageToken(); tok != "" {
		n, err := strconv.Atoi(tok)
		if err != nil || n < 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid page_token")
		}
		offset = n
	}
	if offset > len(filtered) {
		offset = len(filtered)
	}
	end := offset + pageSize
	if end > len(filtered) {
		end = len(filtered)
	}
	next := ""
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return &pppv1.ListJobsResponse{Jobs: filtered[offset:end], NextPageToken: next}, nil
}

func (s *Server) WatchJobs(req *pppv1.WatchJobsRequest, stream pppv1.Control_WatchJobsServer) error {
	if req.GetTreeId() == "" {
		return status.Error(codes.InvalidArgument, "tree_id is required")
	}
	ch := s.fanout.subscribeJobs(req.GetTreeId())
	defer s.fanout.unsubscribeJobs(req.GetTreeId(), ch)

	// Replay every active (CREATED/DISTRIBUTING) job on subscribe so a root
	// that connects late does not miss an assignment. No wall-clock cursor is
	// used, avoiding client/server clock skew.
	jobs, err := s.store.ListJobs()
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	for _, j := range jobs {
		if j.GetTreeId() != req.GetTreeId() {
			continue
		}
		if j.GetState() != pppv1.Job_CREATED && j.GetState() != pppv1.Job_DISTRIBUTING {
			continue
		}
		if err := stream.Send(&pppv1.JobUpdate{Job: j, Removed: false}); err != nil {
			return err
		}
	}

	for {
		select {
		case up := <-ch:
			if err := stream.Send(up); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Server) SyncProgress(stream pppv1.Control_SyncProgressServer) error {
	for {
		rec, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&pppv1.SyncProgressResponse{Ok: true})
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				return nil
			}
			return err
		}
		st := rec.GetState()
		if st == nil || st.GetTreeId() == "" || st.GetFilename() == "" || rec.GetNodeId() == "" {
			return status.Error(codes.InvalidArgument, "progress state (tree_id, filename) and node_id are required")
		}
		if err := s.store.UpsertProgress(st, rec.GetNodeId()); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
}

// ============ helpers ============

// newUUID returns a random RFC 4122 version 4 UUID string.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("ctl: generate uuid: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
