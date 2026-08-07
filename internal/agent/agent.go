package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"path/filepath"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/akzj/ppp/internal/tlsutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
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

	// mTLS credentials (nil = plaintext). serverCreds protects the Data
	// service; clientCreds is used for the ctl and peer connections.
	serverCreds credentials.TransportCredentials
	clientCreds credentials.TransportCredentials

	mu             sync.Mutex
	upstreamAddrs  []string
	pullFromSource bool
	topologyGen    int64
	topoCancel     context.CancelFunc
	bannedCancel   context.CancelFunc
	bannedDisk     *bannedDiskStore

	closeOnce sync.Once
}

// NewAgent builds the agent from the (already finalized) config. The locally
// persisted banned list is loaded immediately so a restarted node keeps
// rejecting banned files during the restart window, before the ctl sync.
func NewAgent(cfg *Config) (*Agent, error) {
	store, err := newPieceStore(cfg)
	if err != nil {
		return nil, err
	}
	bannedDisk, err := openBannedDiskStore(cfg.DownloadPath)
	if err != nil {
		return nil, err
	}
	// mTLS when configured; all TLS flags empty -> nil (plaintext).
	serverCreds, err := tlsutil.LoadServerCredentials(cfg.TLSCA, cfg.TLSCert, cfg.TLSKey)
	if err != nil {
		return nil, err
	}
	clientCreds, err := tlsutil.LoadClientCredentials(cfg.TLSCA, cfg.TLSCert, cfg.TLSKey, cfg.TLSServerName)
	if err != nil {
		return nil, err
	}
	a := &Agent{
		cfg:         cfg,
		store:       store,
		banned:      NewBannedList(),
		leases:      NewLeaseManager(cfg.LeaseTTL),
		source:      &dispatchSource{http: &httpSource{client: newHTTPClient()}, s3: newS3Source()},
		bannedDisk:  bannedDisk,
		nodeID:      cfg.ID,
		serverCreds: serverCreds,
		clientCreds: clientCreds,
	}
	if gen, files, err := bannedDisk.Load(); err == nil {
		a.banned.ApplyInitial(gen, files)
	}
	a.dm = NewDownloaderManager(store, a.banned, a, a.source, nil, a.nodeID, cfg.DownloadConcurrency, cfg.LeaseTTL, clientCreds)
	return a, nil
}

// newPieceStore selects the piece store implementation from the config.
func newPieceStore(cfg *Config) (PieceStore, error) {
	return NewPieceStore(cfg.DownloadPath)
}

// NodeID returns the registered node id.
func (a *Agent) NodeID() string { return a.nodeID }

// Addr returns the advertised Data gRPC address (set after Start).
func (a *Agent) Addr() string { return a.addr }

// Start dials the ctl, registers, starts the Data gRPC service and launches
// the background loops.
func (a *Agent) Start(ctx context.Context) error {
	cc, err := dialCtl(a.cfg.CtlAddr, a.clientCreds)
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
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxGRPCMessageSize),
		grpc.MaxSendMsgSize(maxGRPCMessageSize),
	}
	if a.serverCreds != nil {
		serverOpts = append(serverOpts, grpc.Creds(a.serverCreds))
	}
	a.grpc = grpc.NewServer(serverOpts...)
	pppv1.RegisterDataServer(a.grpc, NewDataServer(a.nodeID, a.cfg.Tree, a.cfg.DownloadPath, a.store, a.banned, a.dm, a.leases))
	go func() { _ = a.grpc.Serve(lis) }()

	node := &pppv1.Node{Id: a.nodeID, Addr: a.addr, TreeId: a.cfg.Tree, Role: a.cfg.Role}
	// Register with the same backoff the watch loops use: the ctl may be
	// briefly unreachable or a follower during a leader-election window, and a
	// node starting then must retry instead of exiting the process.
	var reg *pppv1.RegisterNodeResponse
	for {
		reg, err = a.ctl.RegisterNode(ctx, node)
		if err == nil {
			break
		}
		if !sleepCtx(ctx, watchRetryDelay) {
			a.grpc.Stop()
			_ = cc.Close()
			return fmt.Errorf("agent %s: register with ctl: %w", a.nodeID, err)
		}
	}
	a.applyTopology(reg.GetTopology())
	a.mu.Lock()
	// P2-8: initialize the topology generation from the register response so
	// the first heartbeat does not trigger a spurious resync.
	a.topologyGen = reg.GetTopology().GetGeneration()
	a.mu.Unlock()
	// The banned list has no generation in the register response; the
	// authoritative gen + list come from the sync right after registration.
	a.applyBannedSnapshot(0, reg.GetBanned())
	if resp, err := a.ctl.SyncBannedList(ctx, a.cfg.Tree); err == nil {
		a.applyBannedSnapshot(resp.GetGeneration(), resp.GetBanned())
	}
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
// connection and the local banned store. Idempotent.
func (a *Agent) Stop() {
	a.closeOnce.Do(func() {
		a.dm.CancelAll()
		if a.grpc != nil {
			a.grpc.Stop()
		}
		if a.ctl != nil {
			_ = a.ctl.Close()
		}
		if a.bannedDisk != nil {
			_ = a.bannedDisk.Close()
		}
		// Close the piece store: the mmap implementation must release its
		// mappings/handles or long-lived agents (and repeated test runs) leak
		// address space until ENOMEM.
		if a.store != nil {
			if err := a.store.Close(); err != nil {
				log.Printf("agent %s: close piece store: %v", a.nodeID, err)
			}
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
				// P2-7: when the tree is gone (clean close / not found), clear
				// local upstreams so we stop fetching; the banned list stays.
				if errors.Is(err, io.EOF) || status.Code(err) == codes.NotFound {
					a.applyTopology(&pppv1.Topology{TreeId: a.cfg.Tree})
					a.mu.Lock()
					a.topologyGen = 0
					a.mu.Unlock()
				}
				break
			}
			a.applyTopology(up.GetTopology())
			a.mu.Lock()
			a.topologyGen = up.GetGeneration()
			a.mu.Unlock()
		}
		streamCancel()
		// P2-7: avoid a hot reconnect loop on persistent Recv errors.
		if !sleepCtx(ctx, watchRetryDelay) {
			return
		}
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
			a.applyBannedUpdate(up)
		}
		streamCancel()
		// P2-7: avoid a hot reconnect loop on persistent Recv errors.
		if !sleepCtx(ctx, watchRetryDelay) {
			return
		}
	}
}

// applyBannedSnapshot replaces the banned list (authoritative), persists it
// locally and reacts (stop downloaders, remove local pieces).
func (a *Agent) applyBannedSnapshot(gen int64, banned []*pppv1.BannedFile) {
	a.banned.ApplyInitial(gen, banned)
	a.persistBanned()
	a.cancelBannedDownloaders(&pppv1.BannedListUpdate{FullSnapshot: true, Snapshot: banned})
}

// applyBannedUpdate applies an incremental update, persists it locally and
// reacts.
func (a *Agent) applyBannedUpdate(up *pppv1.BannedListUpdate) {
	a.banned.ApplyUpdate(up)
	a.persistBanned()
	a.cancelBannedDownloaders(up)
}

// persistBanned schedules a coalesced write of the current banned list to the
// local store (Save never fails; Close flushes synchronously).
func (a *Agent) persistBanned() {
	gen, files := a.banned.Snapshot()
	a.bannedDisk.Save(gen, files)
}

// cancelBannedDownloaders stops downloaders for files banned by an update and
// removes their local pieces.
func (a *Agent) cancelBannedDownloaders(up *pppv1.BannedListUpdate) {
	var files []*pppv1.BannedFile
	if up.GetFullSnapshot() {
		files = up.GetSnapshot()
	} else {
		files = up.GetAdded()
	}
	for _, f := range files {
		a.dm.CancelFile(f.GetTreeId(), f.GetFilename())
		// P2-6: a banned file's local pieces are removed. Retention for files
		// still needed by other jobs is deferred to phase 4.
		_ = a.store.Delete(f.GetFilename())
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
				// The job drives the download until the file completes, at
				// which point the downloader releases this need itself.
				a.dm.Ensure(FileNeed{
					TreeID:   job.GetTreeId(),
					Filename: job.GetFilename(),
					Size:     job.GetSize(),
					JobID:    job.GetId(),
					Source:   job.GetSource(),
				}).markJobNeed()
			}
		}
		// P2-7: avoid a hot reconnect loop on persistent Recv errors.
		if !sleepCtx(ctx, watchRetryDelay) {
			return
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
	a.applyBannedSnapshot(resp.GetGeneration(), resp.GetBanned())
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
					LocalPath: filepath.Join(a.cfg.DownloadPath, d.filename),
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
			now := time.Now()
			for _, k := range a.leases.Expire(now) {
				// An expired child lease releases its downloader need; when the
				// last one goes, the downloader stops and the stop propagates
				// toward the source via natural upstream lease expiry.
				if d := a.dm.Get(k.treeID, k.filename); d != nil {
					d.releaseNeed()
				}
			}
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
