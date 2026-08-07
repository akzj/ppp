package ctl

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/tlsutil"
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

	// leader reports whether this instance currently holds the PG lease. Nil
	// means the server is always the leader (tests / single-instance).
	leader func() bool

	// now is injectable for tests; defaults to time.Now.
	now func() time.Time
}

// SetLeaderProvider installs the PG lease leader probe. Without it the server
// behaves as the leader (single-instance tests).
func (s *Server) SetLeaderProvider(f func() bool) {
	s.leader = f
}

// IsLeader reports whether this instance may serve leader duties.
func (s *Server) IsLeader() bool {
	return s.leader == nil || s.leader()
}

// requireLeader returns Unavailable when a follower receives a leader-only
// call (an LB should route only to the leader).
func (s *Server) requireLeader() error {
	if !s.IsLeader() {
		return status.Error(codes.Unavailable, "ctl: not the leader")
	}
	return nil
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

// gracefulStopTimeout bounds GracefulStop during shutdown so active watch
// streams cannot hang the process forever; the fallback is a forced Stop.
const gracefulStopTimeout = 5 * time.Second

// Run opens the store, starts the server and serves gRPC on cfg.Addr until
// ctx is done. It blocks until the server has shut down and the store is
// closed, so the process stays alive while serving. Intended for main.
func Run(ctx context.Context, cfg *Config) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("ctl: listen %s: %w", cfg.Addr, err)
	}
	_, done, err := ServeControl(ctx, cfg, lis)
	if err != nil {
		return err
	}
	<-ctx.Done()
	<-done // wait for gRPC shutdown + store close before the process exits
	return nil
}

// ServeControl opens the PostgreSQL store, starts a Control server with PG
// lease leader election and serves gRPC on lis plus the /leader health check
// on cfg.HTTPAddr. On ctx cancellation it shuts the server down (bounded
// graceful, forced Stop if active streams hang), stops the elector and closes
// the store; the returned done channel closes when cleanup finishes. Tests
// stop the returned grpc.Server explicitly and may cancel ctx to release
// resources.
func ServeControl(ctx context.Context, cfg *Config, lis net.Listener) (gs *grpc.Server, done <-chan struct{}, err error) {
	st, err := OpenPGStore(ctx, cfg.PGDSN)
	if err != nil {
		return nil, nil, err
	}
	srv := NewServer(st, cfg)
	leader := NewLeaderElector(st.Pool(), cfg.InstanceID, cfg.LeaderLease, cfg.LeaderRenew)
	go leader.Run(ctx)
	srv.SetLeaderProvider(leader.IsLeader)
	// Wait (bounded) for the initial election so callers/tests do not race the
	// first mutation against the elector's first acquisition. A follower (the
	// lease is held by another instance) simply times out here.
	for i := 0; i < 100 && !leader.IsLeader() && ctx.Err() == nil; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if err := srv.Start(ctx); err != nil {
		leader.Stop()
		st.Close()
		return nil, nil, err
	}
	// mTLS when configured (all TLS flags empty = plaintext). The /leader HTTP
	// health endpoint stays plaintext (it only returns 200/503 for an LB/VIP).
	var serverOpts []grpc.ServerOption
	creds, err := tlsutil.LoadServerCredentials(cfg.TLSCA, cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		leader.Stop()
		st.Close()
		return nil, nil, err
	}
	if creds != nil {
		serverOpts = append(serverOpts, grpc.Creds(creds))
	}
	gs = grpc.NewServer(serverOpts...)
	pppv1.RegisterControlServer(gs, srv)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		<-ctx.Done()
		shutdownGRPC(gs)
		if leader != nil {
			leader.Stop()
		}
		st.Close()
	}()
	go func() {
		_ = gs.Serve(lis)
	}()
	go serveLeaderHTTP(ctx, cfg.HTTPAddr, srv.IsLeader)
	return gs, doneCh, nil
}

// serveLeaderHTTP exposes the /leader health endpoint (200 when this instance
// is the leader, 503 otherwise) for an LB/VIP to route only to the leader.
func serveLeaderHTTP(ctx context.Context, addr string, isLeader func() bool) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/leader", func(w http.ResponseWriter, _ *http.Request) {
		if isLeader() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("leader\n"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not leader\n"))
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("ctl: /leader http: %v", err)
	}
}

// shutdownGRPC stops the server. GracefulStop is attempted first so in-flight
// unary calls can finish, but it is bounded: active watch streams would block
// it forever, so a forced Stop is used as the fallback.
func shutdownGRPC(gs *grpc.Server) {
	done := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracefulStopTimeout):
		gs.Stop()
	}
}

// ============ Topology helpers (require s.mu) ============

// buildTreeTopology computes the tree's topology from its registered nodes.
// A missing root is a legitimate transient state (e.g. the last root was
// pruned): it yields an empty topology in which every registered node gets an
// empty upstream, so subscribers can detect the broken link. Any other Build
// error (duplicate node id, empty address, ...) is a data-integrity problem
// and is returned instead of being silently turned into an empty topology.
func buildTreeTopology(tree *pppv1.Tree, nodes []*pppv1.Node) (*pppv1.Topology, error) {
	topo, err := topology.Build(topology.Options{Tree: tree, Nodes: nodes})
	if err != nil {
		if errors.Is(err, topology.ErrNoRoot) {
			return &pppv1.Topology{TreeId: tree.GetId(), NodeUpstreams: emptyUpstreams(nodes)}, nil
		}
		return nil, err
	}
	return topo, nil
}

// emptyUpstreams maps every node to an empty upstream list.
func emptyUpstreams(nodes []*pppv1.Node) map[string]*pppv1.NodeUpstream {
	m := make(map[string]*pppv1.NodeUpstream, len(nodes))
	for _, n := range nodes {
		m[n.GetId()] = &pppv1.NodeUpstream{}
	}
	return m
}

// currentTopologyLocked computes the tree's current topology and generation
// without changing the generation. Store and topology-data errors are
// returned, not swallowed.
func (s *Server) currentTopologyLocked(tree *pppv1.Tree) (*pppv1.Topology, int64, error) {
	gen, err := s.store.TopologyGeneration(tree.GetId())
	if err != nil {
		return nil, 0, err
	}
	topo, err := buildTreeTopology(tree, s.regNodesByTree(tree.GetId()))
	if err != nil {
		return nil, 0, err
	}
	topo.Generation = gen
	return topo, gen, nil
}

// refreshTopologyLocked recomputes the tree's topology after a node-set
// change and bumps the topology generation. Even when the tree has no
// computable topology (e.g. its last root was removed) the generation is
// bumped and an empty topology is produced, so subscribers are notified that
// upstreams are gone instead of silently keeping a stale topology.
func (s *Server) refreshTopologyLocked(tree *pppv1.Tree) (*pppv1.Topology, int64, error) {
	gen, err := s.store.BumpTopologyGeneration(tree.GetId())
	if err != nil {
		return nil, 0, err
	}
	topo, err := buildTreeTopology(tree, s.regNodesByTree(tree.GetId()))
	if err != nil {
		return nil, 0, err
	}
	topo.Generation = gen
	return topo, gen, nil
}

// ============ Tree lifecycle ============

func (s *Server) CreateTree(_ context.Context, req *pppv1.CreateTreeRequest) (*pppv1.CreateTreeResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

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
	// Cascade-clean everything belonging to the tree (nodes, jobs, banned,
	// progress) so a re-created tree cannot inherit stale state, then drop the
	// in-memory registry entries and end active watch streams.
	if err := s.store.DeleteTreeData(req.GetTreeId()); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for _, n := range s.regNodesByTree(req.GetTreeId()) {
		s.regDelete(n.GetId())
	}
	s.fanout.closeTree(req.GetTreeId())
	return &pppv1.DeleteTreeResponse{Deleted: true}, nil
}

// ============ Node onboarding ============

func (s *Server) RegisterNode(_ context.Context, req *pppv1.RegisterNodeRequest) (*pppv1.RegisterNodeResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

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
	// Persist first: only update the in-memory registry once the store write
	// succeeded, so a store failure cannot leave the two out of sync.
	if err := s.store.PutNode(node); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	s.regPut(node)

	// Recompute the topology when the node set changed and push it. When the
	// topology is not computable (no root registered), an empty topology is
	// produced and pushed so subscribers see there is no usable upstream.
	topo := &pppv1.Topology{TreeId: tree.GetId()}
	pushed := false
	if changed {
		if topo, _, err = s.refreshTopologyLocked(tree); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		pushed = true
	} else {
		if topo, _, err = s.currentTopologyLocked(tree); err != nil {
			return nil, status.Error(codes.Internal, err.Error())
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

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
	if err := s.requireLeader(); err != nil {
		return err
	}

	if req.GetTreeId() == "" {
		return status.Error(codes.InvalidArgument, "tree_id is required")
	}
	ch := s.fanout.subscribeTopology(req.GetTreeId())
	defer s.fanout.unsubscribeTopology(req.GetTreeId(), ch)

	// Send the current full topology immediately so the stream is
	// self-sufficient; changes afterwards arrive as full snapshots too. A
	// tree without a computable topology yields an empty topology rather than
	// nothing.
	tree, err := s.store.GetTree(req.GetTreeId())
	var initial *pppv1.TopologyUpdate
	if err == nil {
		s.mu.Lock()
		topo, gen, err := s.currentTopologyLocked(tree)
		if err != nil {
			s.mu.Unlock()
			return status.Error(codes.Internal, err.Error())
		}
		initial = &pppv1.TopologyUpdate{Generation: gen, Topology: topo}
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
			if up == nil {
				return nil // tree deleted; fanout closed the stream
			}
			if err := stream.Send(up); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Server) WatchBannedList(req *pppv1.WatchBannedListRequest, stream pppv1.Control_WatchBannedListServer) error {
	if err := s.requireLeader(); err != nil {
		return err
	}

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
			if up == nil {
				return nil // tree deleted; fanout closed the stream
			}
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

// validFilename is the control plane's basename check for job filenames
// (mirrors the agent's validBasename; the packages are independent). The
// ".cds." marker is reserved for the agent's sparse store internals and is
// rejected so a job cannot target an internal path.
func validFilename(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.ContainsAny(name, `/\`) {
		return false
	}
	return !strings.Contains(name, ".cds.")
}

func (s *Server) CreateJob(_ context.Context, req *pppv1.CreateJobRequest) (*pppv1.CreateJobResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	if req.GetTreeId() == "" || req.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "tree_id and filename are required")
	}
	if !validFilename(req.GetFilename()) {
		return nil, status.Error(codes.InvalidArgument, "filename must be a basename (no path separators)")
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

	// A banned file must not be distributed again. Any store error other than
	// "not found" is propagated, not treated as "not banned".
	if _, err := s.store.GetBanned(req.GetTreeId(), req.GetFilename()); err == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "file %q in tree %q is banned; unban it before creating a job", req.GetFilename(), req.GetTreeId())
	} else if !errors.Is(err, ErrNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

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

	// The tree must exist; a banned record for a nonexistent tree would be
	// unreachable for any future operation.
	if _, err := s.store.GetTree(treeID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "tree %q not found", treeID)
		}
		return nil, status.Error(codes.Internal, err.Error())
	}

	// The core of cancellation: persist (tree_id, filename) in the banned list.
	// The same record (single BannedAt timestamp) is what gets pushed.
	now := s.now().Unix()
	banned := &pppv1.BannedFile{
		TreeId:   treeID,
		Filename: filename,
		Reason:   req.GetReason(),
		JobId:    req.GetJobId(),
		BannedAt: now,
	}
	gen, already, err := s.store.AddBanned(banned)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	var bannedPush *pppv1.BannedListUpdate
	if !already {
		bannedPush = &pppv1.BannedListUpdate{Generation: gen, Added: []*pppv1.BannedFile{banned}}
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
	if err := s.requireLeader(); err != nil {
		return nil, err
	}

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
	if err := s.requireLeader(); err != nil {
		return err
	}

	if req.GetTreeId() == "" {
		return status.Error(codes.InvalidArgument, "tree_id is required")
	}
	ch := s.fanout.subscribeJobs(req.GetTreeId())
	defer s.fanout.unsubscribeJobs(req.GetTreeId(), ch)

	// delivered tracks job ids already pushed as active (Removed=false), so a
	// job created concurrently between subscribe and the replay is delivered
	// exactly once (the live push and the replay can both carry it). Removed
	// (cancel) updates always pass and clear the mark.
	delivered := make(map[string]struct{})
	send := func(up *pppv1.JobUpdate) error {
		if up == nil || up.GetJob() == nil {
			return nil
		}
		if !up.GetRemoved() {
			if _, ok := delivered[up.GetJob().GetId()]; ok {
				return nil // duplicate active push (replay + live)
			}
			delivered[up.GetJob().GetId()] = struct{}{}
		} else {
			delete(delivered, up.GetJob().GetId())
		}
		return stream.Send(up)
	}

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
		if err := send(&pppv1.JobUpdate{Job: j, Removed: false}); err != nil {
			return err
		}
	}

	for {
		select {
		case up := <-ch:
			if up == nil {
				return nil // tree deleted; fanout closed the stream
			}
			if err := send(up); err != nil {
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
