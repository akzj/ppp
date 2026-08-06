package agent

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
)

// watchRetryDelay is how long a watch loop waits before reconnecting.
const watchRetryDelay = time.Second

// Agent is the ppp edge data node. It registers with the control plane, keeps
// topology and banned state in sync, serves the Data gRPC service and drives
// per-file downloaders.
type Agent struct {
	cfg    *Config
	ctl    *ctlClient
	store  PieceStore
	banned *BannedList
	leases *LeaseManager
	dm     *DownloaderManager
	source Source

	nodeID string
	addr   string
	grpc   *grpc.Server

	mu             sync.Mutex
	upstreamAddrs  []string
	pullFromSource bool
	topologyGen    int64
	topoCancel     context.CancelFunc
	bannedCancel   context.CancelFunc

	closeOnce sync.Once
}

// NewAgent builds the agent from the (already finalized) config.
func NewAgent(cfg *Config) (*Agent, error) {
	store, err := NewFilePieceStore(cfg.DownloadPath)
	if err != nil {
		return nil, err
	}
	a := &Agent{
		cfg:    cfg,
		store:  store,
		banned: NewBannedList(),
		leases: NewLeaseManager(cfg.LeaseTTL),
		source: &dispatchSource{http: &httpSource{client: newHTTPClient()}},
		nodeID: cfg.ID,
	}
	a.dm = NewDownloaderManager(store, a.banned, a, a.source, nil, a.nodeID, cfg.DownloadConcurrency)
	return a, nil
}

// NodeID returns the registered node id.
func (a *Agent) NodeID() string { return a.nodeID }

// Addr returns the advertised Data gRPC address (set after Start).
func (a *Agent) Addr() string { return a.addr }

// Start dials the ctl, registers, starts the Data gRPC service and launches
// the background loops.
func (a *Agent) Start(ctx context.Context) error {
	cc, err := dialCtl(a.cfg.CtlAddr)
	if err != nil {
		return err
	}
	a.ctl = cc

	lis, err := net.Listen("tcp", a.cfg.Addr)
	if err != nil {
		_ = cc.Close()
		return err
	}
	a.addr = lis.Addr().String()
	a.grpc = grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageSize),
		grpc.MaxSendMsgSize(maxGRPCMessageSize),
	)
	pppv1.RegisterDataServer(a.grpc, NewDataServer(a.nodeID, a.store, a.banned, a.dm, a.leases))
	go func() { _ = a.grpc.Serve(lis) }()

	node := &pppv1.Node{Id: a.nodeID, Addr: a.addr, TreeId: a.cfg.Tree, Role: a.cfg.Role}
	reg, err := a.ctl.RegisterNode(ctx, node)
	if err != nil {
		a.grpc.Stop()
		_ = cc.Close()
		return err
	}
	a.applyTopology(reg.GetTopology())
	a.banned.ApplyInitial(0, reg.GetBanned())
	a.setTreeSource(reg.GetTree().GetSource())

	go a.watchTopologyLoop(ctx)
	go a.watchBannedLoop(ctx)
	if a.cfg.Role == pppv1.Node_ROOT {
		go a.watchJobsLoop(ctx)
	}
	go a.heartbeatLoop(ctx)
	go a.progressLoop(ctx)
	go a.leaseScanLoop(ctx)
	return nil
}

// Stop shuts down the Data service, cancels downloaders and closes the ctl
// connection. Idempotent.
func (a *Agent) Stop() {
	a.closeOnce.Do(func() {
		a.dm.CancelAll()
		if a.grpc != nil {
			a.grpc.Stop()
		}
		if a.ctl != nil {
			_ = a.ctl.Close()
		}
		a.dm.Close()
	})
}

// ============ topologyProvider ============

func (a *Agent) UpstreamAddrs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.upstreamAddrs...)
}

func (a *Agent) PullFromSource() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pullFromSource
}

// applyTopology stores the node's upstream state from a full topology update.
// An empty topology (e.g. zero roots) clears upstreams and source-pull so
// downloaders stop fetching until a usable topology returns.
func (a *Agent) applyTopology(topo *pppv1.Topology) {
	if topo == nil {
		return
	}
	a.mu.Lock()
	if up := topo.GetNodeUpstreams()[a.nodeID]; up != nil {
		a.upstreamAddrs = append([]string(nil), up.GetAddrs()...)
		a.pullFromSource = up.GetPullFromSource()
	} else {
		a.upstreamAddrs = nil
		a.pullFromSource = false
	}
	a.mu.Unlock()
	a.dm.WakeAll()
}

// setTreeSource records the tree default source (from the register response).
func (a *Agent) setTreeSource(src *pppv1.Source) {
	a.dm.SetTreeSource(src)
}

// ============ watch loops ============

// watchTopologyLoop applies full topology snapshots. Each stream iteration
// uses a cancelable context so the heartbeat safety net can force a reconnect.
func (a *Agent) watchTopologyLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		streamCtx, streamCancel := context.WithCancel(ctx)
		a.mu.Lock()
		a.topoCancel = streamCancel
		a.mu.Unlock()
		stream, err := a.ctl.WatchTopology(streamCtx, a.cfg.Tree)
		if err != nil {
			a.mu.Lock()
			a.topoCancel = nil
			a.mu.Unlock()
			streamCancel()
			if !sleepCtx(ctx, watchRetryDelay) {
				return
			}
			continue
		}
		for {
			up, err := stream.Recv()
			if err != nil {
				break
			}
			a.applyTopology(up.GetTopology())
			a.mu.Lock()
			a.topologyGen = up.GetGeneration()
			a.mu.Unlock()
		}
		streamCancel()
	}
}

// watchBannedLoop applies banned-list updates (full snapshot authoritative,
// added/removed incremental) and cancels downloaders of newly banned files.
func (a *Agent) watchBannedLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		streamCtx, streamCancel := context.WithCancel(ctx)
		a.mu.Lock()
		a.bannedCancel = streamCancel
		a.mu.Unlock()
		stream, err := a.ctl.WatchBannedList(streamCtx, a.cfg.Tree)
		if err != nil {
			a.mu.Lock()
			a.bannedCancel = nil
			a.mu.Unlock()
			streamCancel()
			if !sleepCtx(ctx, watchRetryDelay) {
				return
			}
			continue
		}
		for {
			up, err := stream.Recv()
			if err != nil {
				break
			}
			a.banned.ApplyUpdate(up)
			a.cancelBannedDownloaders(up)
		}
		streamCancel()
	}
}

// cancelBannedDownloaders stops downloaders for files banned by an update.
func (a *Agent) cancelBannedDownloaders(up *pppv1.BannedListUpdate) {
	var files []*pppv1.BannedFile
	if up.GetFullSnapshot() {
		files = up.GetSnapshot()
	} else {
		files = up.GetAdded()
	}
	for _, f := range files {
		a.dm.CancelFile(f.GetTreeId(), f.GetFilename())
	}
}

// watchJobsLoop (root only) starts downloaders for active jobs and cancels
// them on job removal.
func (a *Agent) watchJobsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		stream, err := a.ctl.WatchJobs(ctx, a.cfg.Tree)
		if err != nil {
			if !sleepCtx(ctx, watchRetryDelay) {
				return
			}
			continue
		}
		for {
			up, err := stream.Recv()
			if err != nil {
				break
			}
			job := up.GetJob()
			if job == nil {
				continue
			}
			if up.GetRemoved() {
				a.dm.CancelFile(job.GetTreeId(), job.GetFilename())
				continue
			}
			if job.GetState() == pppv1.Job_CREATED || job.GetState() == pppv1.Job_DISTRIBUTING {
				a.dm.Ensure(FileNeed{
					TreeID:   job.GetTreeId(),
					Filename: job.GetFilename(),
					Size:     job.GetSize(),
					JobID:    job.GetId(),
					Source:   job.GetSource(),
				})
			}
		}
	}
}

// ============ heartbeat safety net ============

func (a *Agent) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(a.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		resp, err := a.ctl.Heartbeat(ctx, &pppv1.Node{Id: a.nodeID})
		if err != nil {
			log.Printf("agent %s: heartbeat: %v", a.nodeID, err)
			continue
		}
		a.mu.Lock()
		topoGen := a.topologyGen
		a.mu.Unlock()
		bannedGen := a.banned.Generation()
		if resp.GetTopologyGeneration() != topoGen {
			log.Printf("agent %s: topology generation mismatch (%d != %d), reconnecting watch", a.nodeID, resp.GetTopologyGeneration(), topoGen)
			a.forceTopologyResync()
		}
		if resp.GetBannedGeneration() != bannedGen {
			log.Printf("agent %s: banned generation mismatch (%d != %d), re-syncing", a.nodeID, resp.GetBannedGeneration(), bannedGen)
			a.resyncBanned()
		}
	}
}

// forceTopologyResync cancels the current topology watch stream so its loop
// reconnects and receives a fresh full snapshot.
func (a *Agent) forceTopologyResync() {
	a.mu.Lock()
	cancel := a.topoCancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// forceBannedResync cancels the current banned watch stream so its loop
// reconnects and receives a fresh full snapshot.
func (a *Agent) forceBannedResync() {
	a.mu.Lock()
	cancel := a.bannedCancel
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// resyncBanned pulls the authoritative banned list directly (the watch loop
// reconnects and delivers a full snapshot as well).
func (a *Agent) resyncBanned() {
	resp, err := a.ctl.SyncBannedList(context.Background(), a.cfg.Tree)
	if err != nil {
		log.Printf("agent %s: sync banned: %v", a.nodeID, err)
		return
	}
	a.banned.ApplyInitial(resp.GetGeneration(), resp.GetBanned())
	a.cancelBannedDownloaders(&pppv1.BannedListUpdate{FullSnapshot: true, Snapshot: resp.GetBanned()})
	a.forceBannedResync()
}

// ============ progress + leases ============

// progressLoop reports downloader progress to the ctl about once per second,
// best effort, reconnecting the stream on failure.
func (a *Agent) progressLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var stream pppv1.Control_SyncProgressClient
	var cancel context.CancelFunc
	open := func() {
		if stream != nil {
			return
		}
		sctx, c := context.WithCancel(ctx)
		s, err := a.ctl.SyncProgress(sctx)
		if err != nil {
			c()
			return
		}
		stream, cancel = s, c
	}
	for {
		select {
		case <-ctx.Done():
			if cancel != nil {
				cancel()
			}
			return
		case <-ticker.C:
		}
		open()
		if stream == nil {
			continue
		}
		for _, d := range a.dm.Snapshot() {
			downloaded, size, complete, err := d.Progress()
			state := pppv1.ProgressState_DOWNLOADING
			switch {
			case complete:
				state = pppv1.ProgressState_SUCCESS
			case errors.Is(err, errFileBanned):
				state = pppv1.ProgressState_BANNED
			case err != nil:
				state = pppv1.ProgressState_FAILED
			}
			rec := &pppv1.ProgressRecord{
				State: &pppv1.ProgressState{
					JobId: d.jobID, TreeId: d.treeID, Filename: d.filename, Size: size,
					DownloadedBytes: downloaded, Progress: progressPercent(downloaded, size), State: state,
				},
				NodeId: a.nodeID,
			}
			if err := stream.Send(rec); err != nil {
				if cancel != nil {
					cancel()
				}
				stream, cancel = nil, nil
				break
			}
		}
	}
}

// leaseScanLoop prunes expired session leases.
func (a *Agent) leaseScanLoop(ctx context.Context) {
	interval := a.cfg.LeaseTTL / 2
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.leases.Expire(time.Now())
		}
	}
}

// sleepCtx sleeps d or returns false when ctx is done first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
