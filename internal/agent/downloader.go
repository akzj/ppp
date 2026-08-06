package agent

import (
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Fetch tuning for the per-file downloader.
const (
	// fetchAttempts is how many times a single piece fetch is retried before
	// the piece is left missing (a later request re-triggers it).
	fetchAttempts = 3
	// fetchRetryDelay is the base delay between piece fetch attempts.
	fetchRetryDelay = 100 * time.Millisecond
	// noUpstreamRetry is how long a downloader waits before re-checking for
	// upstreams when it has none (topology may have changed).
	noUpstreamRetry = 250 * time.Millisecond
)

// Sentinel errors used by the downloader and its callers.
var (
	// errFileBanned marks a terminal file-level failure caused by the banned
	// list (either locally or reported by an upstream).
	errFileBanned = errors.New("agent: file banned")
	// errNoUpstream means the node has no upstream addresses and is not the
	// primary root, so it cannot fetch right now.
	errNoUpstream = errors.New("agent: no upstream")
	// errPieceFailed is an internal sentinel when a piece could not be fetched
	// after all attempts (non-terminal; waiters time out on their own ctx).
	errPieceFailed = errors.New("agent: piece fetch failed")
)

// crcTable is the ECMA crc64 table used for piece hashes.
var crcTable = crc64.MakeTable(crc64.ECMA)

// topologyProvider exposes the agent's current upstream state to downloaders.
// Implemented by Agent.
type topologyProvider interface {
	UpstreamAddrs() []string
	PullFromSource() bool
}

// FileNeed describes why a (tree, file) download should start.
type FileNeed struct {
	TreeID   string
	Filename string
	Size     int64
	JobID    string
	// From is the hop chain of the request that triggered this download; the
	// downloader appends its own hop when fetching upstream.
	From []*pppv1.Hop
	// Source optionally overrides the tree default source (root fetch).
	Source *pppv1.Source
}

// Downloader fetches all pieces of one (tree, file) in subtask mode: on
// demand it downloads the whole file from upstreams or the source, caches it
// locally, and serves downstream. Waiters block on specific pieces until the
// piece is stored, the file fails (e.g. banned), or their context ends.
type Downloader struct {
	treeID   string
	filename string
	jobID    string
	baseFrom []*pppv1.Hop

	store       PieceStore
	banned      *BannedList
	topo        topologyProvider
	source      Source
	treeSource  *pppv1.Source
	peers       *peerPool
	nodeID      string
	concurrency int

	mu        sync.Mutex
	ctx       context.Context
	cancel    context.CancelFunc
	size      int64
	numPieces int64
	inflight  map[int64]bool
	waiters   map[int64][]*pieceWaiter
	fileErr   error
	complete  bool
	started   bool
	wakeCh    chan struct{}
}

type pieceWaiter struct {
	ch chan error // buffered: nil on piece arrival, else file error
}

func newDownloader(need FileNeed, store PieceStore, banned *BannedList, topo topologyProvider, source Source, treeSource *pppv1.Source, peers *peerPool, nodeID string, concurrency int) *Downloader {
	ctx, cancel := context.WithCancel(context.Background())
	return &Downloader{
		treeID:      need.TreeID,
		filename:    need.Filename,
		jobID:       need.JobID,
		baseFrom:    append([]*pppv1.Hop(nil), need.From...),
		store:       store,
		banned:      banned,
		topo:        topo,
		source:      source,
		treeSource:  treeSource,
		peers:       peers,
		nodeID:      nodeID,
		concurrency: concurrency,
		ctx:         ctx,
		cancel:      cancel,
		inflight:    make(map[int64]bool),
		waiters:     make(map[int64][]*pieceWaiter),
		wakeCh:      make(chan struct{}, 1),
	}
}

// ensureSize records the file size (first one wins) and starts the fetch
// loop. Call with d.mu held.
func (d *Downloader) ensureSizeLocked(size int64) {
	if d.size == 0 && size > 0 {
		d.size = size
		d.numPieces = (size + PieceSize - 1) / PieceSize
	}
	if !d.started && d.size > 0 {
		d.started = true
		go d.run()
	}
}

// Ensure records the file size and starts fetching if needed. Idempotent.
func (d *Downloader) Ensure(size int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ensureSizeLocked(size)
}

// WaitPiece returns the piece bytes, downloading the file if necessary.
func (d *Downloader) WaitPiece(ctx context.Context, index int64) ([]byte, error) {
	if data, err := d.store.Get(d.treeID, d.filename, index); err == nil {
		return data, nil
	}
	if index < 0 || index >= d.numPieces {
		return nil, fmt.Errorf("%w: piece %d out of range", errPieceFailed, index)
	}

	w := &pieceWaiter{ch: make(chan error, 1)}
	d.mu.Lock()
	if d.fileErr != nil {
		err := d.fileErr
		d.mu.Unlock()
		return nil, err
	}
	d.waiters[index] = append(d.waiters[index], w)
	d.ensureSizeLocked(d.size) // size may be 0 if only WaitPiece was called; nothing to fetch yet
	d.mu.Unlock()

	select {
	case err := <-w.ch:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return d.store.Get(d.treeID, d.filename, index)
}

// Progress returns the approximate downloaded bytes, total size and whether
// the file is complete.
func (d *Downloader) Progress() (downloaded int64, size int64, complete bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := int64(d.store.PieceCount(d.treeID, d.filename)) * PieceSize
	if n > d.size {
		n = d.size
	}
	return n, d.size, d.complete
}

// stop cancels the downloader and fails all waiters. Used on file ban or
// agent shutdown.
func (d *Downloader) stop(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fileErr == nil {
		d.fileErr = err
	}
	d.cancel()
	d.failWaitersLocked(err)
}

// fail marks the file failed and notifies every waiter.
func (d *Downloader) fail(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fileErr == nil {
		d.fileErr = err
	}
	d.cancel()
	d.failWaitersLocked(err)
}

// failWaitersLocked delivers err to every waiter and clears the map.
func (d *Downloader) failWaitersLocked(err error) {
	for _, ws := range d.waiters {
		for _, w := range ws {
			w.ch <- err
		}
	}
	d.waiters = make(map[int64][]*pieceWaiter)
}

// wake unblocks a downloader waiting for upstreams (topology changed).
func (d *Downloader) wake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

// run is the fetch loop: it dispatches every missing piece with bounded
// concurrency until the file is complete, canceled, or failed.
func (d *Downloader) run() {
	sem := make(chan struct{}, d.concurrency)
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		d.mu.Lock()
		d.checkCompleteLocked()
		d.mu.Unlock()
	}()
	for {
		d.mu.Lock()
		if d.fileErr != nil || d.ctx.Err() != nil {
			d.mu.Unlock()
			break
		}
		index := d.nextMissingLocked()
		if index < 0 {
			d.mu.Unlock()
			break
		}
		d.inflight[index] = true
		d.mu.Unlock()

		select {
		case sem <- struct{}{}:
		case <-d.ctx.Done():
			d.mu.Lock()
			delete(d.inflight, index)
			d.mu.Unlock()
			break
		}
		if d.ctx.Err() != nil {
			d.mu.Lock()
			delete(d.inflight, index)
			d.mu.Unlock()
			break
		}
		wg.Add(1)
		go func(idx int64) {
			defer wg.Done()
			defer func() { <-sem }()
			d.fetchPiece(idx)
		}(index)
	}
}

// nextMissingLocked returns the first piece not stored and not in flight, or
// -1 when nothing is missing.
func (d *Downloader) nextMissingLocked() int64 {
	for i := int64(0); i < d.numPieces; i++ {
		if d.inflight[i] {
			continue
		}
		if !d.store.HasPiece(d.treeID, d.filename, i) {
			return i
		}
	}
	return -1
}

// checkCompleteLocked marks the file complete when every piece is stored.
func (d *Downloader) checkCompleteLocked() {
	if d.complete || d.fileErr != nil {
		return
	}
	for i := int64(0); i < d.numPieces; i++ {
		if !d.store.HasPiece(d.treeID, d.filename, i) {
			return
		}
	}
	d.complete = true
	_ = d.store.MarkComplete(d.treeID, d.filename, d.size)
}

// fetchPiece fetches one piece with retries, stores it and notifies waiters.
func (d *Downloader) fetchPiece(index int64) {
	if d.banned.IsBanned(d.treeID, d.filename) {
		d.fail(errFileBanned)
		return
	}
	var data []byte
	var err error
	for attempt := 0; attempt < fetchAttempts; attempt++ {
		if d.ctx.Err() != nil {
			return
		}
		if d.banned.IsBanned(d.treeID, d.filename) {
			d.fail(errFileBanned)
			return
		}
		data, err = d.fetchOnce(index)
		if err == nil {
			break
		}
		if errors.Is(err, errFileBanned) {
			d.fail(errFileBanned)
			return
		}
		if errors.Is(err, errNoUpstream) {
			// No upstream right now: wait briefly, or until the topology
			// changes (WakeAll) or the file is canceled.
			select {
			case <-time.After(noUpstreamRetry):
			case <-d.wakeCh:
			case <-d.ctx.Done():
				return
			}
			continue
		}
		select {
		case <-time.After(fetchRetryDelay):
		case <-d.ctx.Done():
			return
		}
	}
	if err != nil {
		// Persistent failure: leave the piece missing. Waiters are bounded by
		// their own context; a later Ensure re-triggers the fetch.
		d.mu.Lock()
		delete(d.inflight, index)
		d.mu.Unlock()
		return
	}
	if err := d.store.Put(d.treeID, d.filename, index, data); err != nil {
		d.mu.Lock()
		delete(d.inflight, index)
		d.mu.Unlock()
		return
	}
	d.pieceStored(index)
}

// pieceStored records the piece, notifies its waiters and checks completion.
func (d *Downloader) pieceStored(index int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inflight, index)
	if ws := d.waiters[index]; len(ws) > 0 {
		delete(d.waiters, index)
		for _, w := range ws {
			w.ch <- nil
		}
	}
	if d.fileErr == nil && !d.complete {
		d.checkCompleteLocked()
	}
}

// fetchOnce fetches one piece from the source (primary root) or from the
// upstream parents.
func (d *Downloader) fetchOnce(index int64) ([]byte, error) {
	if d.topo.PullFromSource() {
		if d.treeSource == nil {
			return nil, errors.New("agent: pull-from-source but no source configured")
		}
		return d.source.FetchPiece(d.ctx, d.treeSource, d.treeID, d.filename, index, d.size, PieceSize)
	}
	upstreams := d.topo.UpstreamAddrs()
	if len(upstreams) == 0 {
		return nil, errNoUpstream
	}
	var lastErr error
	for _, addr := range upstreams {
		data, err := d.fetchFromPeer(addr, index)
		if err == nil {
			return data, nil
		}
		lastErr = err
		if errors.Is(err, errFileBanned) {
			return nil, err
		}
	}
	return nil, lastErr
}

// fetchFromPeer requests one piece from an upstream Data node.
func (d *Downloader) fetchFromPeer(addr string, index int64) ([]byte, error) {
	client, err := d.peers.client(addr)
	if err != nil {
		return nil, err
	}
	from := append(append([]*pppv1.Hop(nil), d.baseFrom...), &pppv1.Hop{NodeId: d.nodeID, JobId: d.jobID})
	resp, err := client.GetPiece(d.ctx, &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: d.treeID, Filename: d.filename},
		Index: index,
		Size:  d.size,
		JobId: d.jobID,
		From:  from,
	})
	if err != nil {
		return nil, err
	}
	switch r := resp.GetResult().(type) {
	case *pppv1.GetPieceResponse_Piece:
		p := r.Piece
		if p == nil {
			return nil, errors.New("agent: empty piece from peer")
		}
		data := p.GetData()
		if h := p.GetInfo().GetHash(); h != 0 && crc64.Checksum(data, crcTable) != h {
			return nil, errors.New("agent: piece crc mismatch from peer")
		}
		return data, nil
	case *pppv1.GetPieceResponse_Error:
		return nil, mapPeerError(r.Error)
	}
	return nil, errors.New("agent: empty piece response from peer")
}

// mapPeerError converts a data-plane Error message into a Go error.
func mapPeerError(e *pppv1.Error) error {
	if e.GetCode() == pppv1.Error_BANNED {
		return errFileBanned
	}
	return fmt.Errorf("agent: peer error %s: %s", e.GetCode(), e.GetMessage())
}

// peerPool caches Data gRPC client connections per address.
type peerPool struct {
	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pppv1.DataClient
}

func newPeerPool() *peerPool {
	return &peerPool{
		conns:   make(map[string]*grpc.ClientConn),
		clients: make(map[string]pppv1.DataClient),
	}
}

func (p *peerPool) client(addr string) (pppv1.DataClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.clients[addr]; ok {
		return c, nil
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("agent: dial peer %s: %w", addr, err)
	}
	p.conns[addr] = conn
	c := pppv1.NewDataClient(conn)
	p.clients[addr] = c
	return c, nil
}

func (p *peerPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
	p.conns = make(map[string]*grpc.ClientConn)
	p.clients = make(map[string]pppv1.DataClient)
}

// DownloaderManager owns the per-(tree, file) downloaders.
type DownloaderManager struct {
	store       PieceStore
	banned      *BannedList
	topo        topologyProvider
	source      Source
	treeSource  *pppv1.Source
	peers       *peerPool
	nodeID      string
	concurrency int

	mu    sync.Mutex
	files map[string]*Downloader
}

// NewDownloaderManager creates the manager. treeSource is the tree default
// source (nil until the register response arrives).
func NewDownloaderManager(store PieceStore, banned *BannedList, topo topologyProvider, source Source, treeSource *pppv1.Source, nodeID string, concurrency int) *DownloaderManager {
	return &DownloaderManager{
		store:       store,
		banned:      banned,
		topo:        topo,
		source:      source,
		treeSource:  treeSource,
		peers:       newPeerPool(),
		nodeID:      nodeID,
		concurrency: concurrency,
		files:       make(map[string]*Downloader),
	}
}

// SetTreeSource updates the tree default source (after registration).
func (m *DownloaderManager) SetTreeSource(src *pppv1.Source) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.treeSource = src
}

// Ensure returns (creating if needed) the downloader for the file and starts
// fetching. A nil Source means the tree default is used.
func (m *DownloaderManager) Ensure(need FileNeed) *Downloader {
	key := need.TreeID + "\x00" + need.Filename
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.files[key]
	if !ok {
		src := m.treeSource
		if need.Source != nil {
			src = need.Source
		}
		d = newDownloader(need, m.store, m.banned, m.topo, m.source, src, m.peers, m.nodeID, m.concurrency)
		m.files[key] = d
	}
	d.Ensure(need.Size)
	return d
}

// CancelFile stops the downloader for a file (banned arrival / job removal).
func (m *DownloaderManager) CancelFile(treeID, filename string) {
	key := treeID + "\x00" + filename
	m.mu.Lock()
	d, ok := m.files[key]
	if ok {
		delete(m.files, key)
	}
	m.mu.Unlock()
	if ok && d != nil {
		d.stop(errFileBanned)
	}
}

// WakeAll nudges every downloader that may be waiting for upstreams.
func (m *DownloaderManager) WakeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, d := range m.files {
		d.Ensure(0)
	}
}

// Snapshot returns the active downloaders (for progress reporting).
func (m *DownloaderManager) Snapshot() []*Downloader {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*Downloader, 0, len(m.files))
	for _, d := range m.files {
		out = append(out, d)
	}
	return out
}

// Close releases peer connections.
func (m *DownloaderManager) Close() {
	m.peers.close()
}
