package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
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
	// pieceCooldown is how long a piece is not re-dispatched after a
	// persistent fetch failure, preventing a hot retry loop against a dead or
	// misbehaving peer. Active demand (a waiting GetPiece) or a topology
	// change clears it early.
	pieceCooldown = 5 * time.Second
)

// pieceFetchTimeout bounds each upstream GetPiece call so a hung peer cannot
// starve a file forever; the parent still gets plenty of time to be
// downloading the piece itself. A var so tests can lower it.
var pieceFetchTimeout = 60 * time.Second

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
	// errPieceDigestMismatch marks a piece whose SHA-256 does not match the
	// bound metadata's expected digest (C4): the piece is discarded and the
	// upstream is considered faulty.
	errPieceDigestMismatch = errors.New("agent: piece digest mismatch")
	// errContentConflict marks an upstream whose metadata_id differs from the
	// bound metadata: it must never be mixed with the current content (§5.3).
	errContentConflict = errors.New("agent: content conflict")
	// errMetadataCorrupt marks copied metadata whose bytes failed validation
	// (metadata_id mismatch or decode error): the upstream is faulty.
	errMetadataCorrupt = errors.New("agent: metadata corrupt")
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
	mgr         *DownloaderManager

	mu              sync.Mutex
	metaBindMu      sync.Mutex // serializes the one authoritative metadata bind
	ctx             context.Context
	cancel          context.CancelFunc
	size            int64
	numPieces       int64
	inflight        map[int64]bool
	cooldown        map[int64]time.Time // pieces not re-dispatched before this time
	cooldownCleared map[int64]bool      // demand already cleared a cooldown once (anti-flood)
	waiters         map[int64][]*pieceWaiter
	// pieceHashes[i] is the SHA-256 of stored piece i (C2): accumulated while
	// the piece is fetched, read back at Seal time for resumed pieces. It is
	// the basis of the self-built FileMetadataV1. C4 replaces the self-build
	// with the upstream-copied metadata (same content, same metadata_id).
	pieceHashes [][]byte
	// meta is the bound authoritative metadata (C4): non-root downloaders
	// copy it from an upstream before fetching pieces; the root self-builds
	// at Seal time. metaBytes is the exact canonical bytes used for Seal and
	// metaID = SHA-256(metaBytes).
	meta      *FileMetadataV1
	metaBytes []byte
	metaID    []byte
	// bindAttempted marks a root that decided to self-build (no upstream
	// sealed): the run loop must not re-bind it every iteration.
	bindAttempted bool
	fileErr       error
	complete      bool
	running       bool
	wakeCh        chan struct{}
	// need counts active holders of this file (child subscribers + active
	// piece waiters); jobNeed marks a root job-driven download. When need
	// reaches zero and the file is not complete the downloader stops and is
	// reclaimed, so upstream leases expire and the stop propagates.
	need    int
	jobNeed bool

	// Upstream leases keep the parents' downloaders alive while this node
	// fetches from them (design §3.3). They are renewed while fetching and
	// released on stop, so a stopped node propagates its stop upstream.
	leaseTTL          time.Duration
	upstreamLastRenew map[string]time.Time // upstream addr -> last renewal
	upstreamLeases    map[string]struct{}  // upstream addrs currently subscribed
}

type pieceWaiter struct {
	ch chan error // buffered: nil on piece arrival, else file error
}

func newDownloader(need FileNeed, store PieceStore, banned *BannedList, topo topologyProvider, source Source, treeSource *pppv1.Source, peers *peerPool, nodeID string, concurrency int, mgr *DownloaderManager, leaseTTL time.Duration) *Downloader {
	ctx, cancel := context.WithCancel(context.Background())
	return &Downloader{
		treeID:            need.TreeID,
		filename:          need.Filename,
		jobID:             need.JobID,
		baseFrom:          append([]*pppv1.Hop(nil), need.From...),
		store:             store,
		banned:            banned,
		topo:              topo,
		source:            source,
		treeSource:        treeSource,
		peers:             peers,
		nodeID:            nodeID,
		concurrency:       concurrency,
		mgr:               mgr,
		leaseTTL:          leaseTTL,
		ctx:               ctx,
		cancel:            cancel,
		inflight:          make(map[int64]bool),
		cooldown:          make(map[int64]time.Time),
		cooldownCleared:   make(map[int64]bool),
		waiters:           make(map[int64][]*pieceWaiter),
		wakeCh:            make(chan struct{}, 1),
		upstreamLastRenew: make(map[string]time.Time),
		upstreamLeases:    make(map[string]struct{}),
	}
}

// ensureSize records the file size (first one wins) and starts the fetch
// loop. Call with d.mu held.
func (d *Downloader) ensureSizeLocked(size int64) {
	if d.size == 0 && size > 0 {
		d.size = size
		d.numPieces = (size + PieceSize - 1) / PieceSize
		d.pieceHashes = make([][]byte, d.numPieces)
	}
}

// startLocked starts the fetch loop if a size is known and it is not already
// running. A downloader that was stopped silently (no need left) is
// restartable: a fresh context is created and any stale in-flight markers are
// dropped, so pieces that were being fetched when the previous run was
// canceled can be re-dispatched. Cooldowns are deliberately NOT cleared here:
// a restart must not let dense demand bypass the failure backoff (P2-1).
// Banned/failed downloaders never restart. Call with d.mu held.
func (d *Downloader) startLocked() {
	if d.running || d.size <= 0 || d.fileErr != nil {
		return
	}
	if d.ctx.Err() != nil {
		d.ctx, d.cancel = context.WithCancel(context.Background())
		d.inflight = make(map[int64]bool)
	}
	d.running = true
	go d.run()
}

// Need returns the current need count (tests/debugging).
func (d *Downloader) Need() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.need
}

// MetadataID returns a copy of the authoritative metadata identity currently
// bound to this downloader. It is empty for a primary root that is still
// constructing the first metadata.
func (d *Downloader) MetadataID() []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]byte(nil), d.metaID...)
}

// MetadataBytes returns a copy of the verified, upstream-copied metadata bound
// to this downloader. It is safe to relay before the local data is complete:
// the bytes have already passed the metadata_id and canonical-format checks
// and are immutable after binding.
func (d *Downloader) MetadataBytes() ([]byte, []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]byte(nil), d.metaBytes...), append([]byte(nil), d.metaID...)
}

// Ensure records the file size. Fetching starts only once a need exists
// (a waiter, a subscriber, or a root job).
func (d *Downloader) Ensure(size int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ensureSizeLocked(size)
}

// ============ need model ============

// addNeed registers one holder of the file (child subscriber or active piece
// waiter) and starts fetching if possible.
func (d *Downloader) addNeed() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.need++
	d.startLocked()
}

// markJobNeed registers the driving need of a root job. It is released by the
// downloader itself once the file completes.
func (d *Downloader) markJobNeed() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.jobNeed {
		d.jobNeed = true
		d.need++
	}
	d.startLocked()
}

// releaseNeed drops one holder. When the last need goes away and the file is
// not complete, the downloader stops fetching and is reclaimed, so upstream
// leases expire naturally and the stop propagates toward the source.
func (d *Downloader) releaseNeed() {
	d.mu.Lock()
	if d.need > 0 {
		d.need--
	}
	stop := d.need == 0 && !d.complete && d.fileErr == nil
	d.mu.Unlock()
	if stop {
		d.stopSilent()
	}
}

// stopSilent stops a downloader that is no longer needed: in-flight fetches
// are canceled and the manager drops it (the file data stays in the store).
func (d *Downloader) stopSilent() {
	d.mu.Lock()
	if d.fileErr == nil && !d.complete {
		d.cancel()
	}
	d.mu.Unlock()
	d.releaseUpstreamLeases()
	d.mgr.removeTerminal(d)
}

// WaitPiece returns the piece bytes, downloading the file if necessary.
func (d *Downloader) WaitPiece(ctx context.Context, index int64) ([]byte, error) {
	if data, err := d.store.Get(d.filename, index); err == nil {
		return data, nil
	}

	w := &pieceWaiter{ch: make(chan error, 1)}
	d.mu.Lock()
	if index < 0 || index >= d.numPieces {
		// A second request for the same file with a LARGER size is the common
		// cause: ensureSizeLocked only records the first size, so the extra
		// pieces are unknown. Hint at the mismatch so the caller can fix it.
		// (numPieces/size are read under the lock — the C4 metadata bind can
		// resize the downloader concurrently.)
		size := d.size
		d.mu.Unlock()
		return nil, fmt.Errorf("%w: piece %d out of range (file size %d; a larger second size is a size mismatch)", errPieceFailed, index, size)
	}
	if d.fileErr != nil {
		err := d.fileErr
		d.mu.Unlock()
		return nil, err
	}
	// Active demand may clear a piece's failure cooldown at most once per
	// cooldown period so a waiting request is retried promptly, but dense or
	// malicious requests cannot bypass the backoff (P2-1).
	if cd, ok := d.cooldown[index]; ok && time.Now().Before(cd) && !d.cooldownCleared[index] {
		d.cooldownCleared[index] = true
		delete(d.cooldown, index)
	}
	d.waiters[index] = append(d.waiters[index], w)
	d.need++ // this waiter holds one unit of local need until it exits
	d.ensureSizeLocked(d.size)
	d.startLocked()
	d.mu.Unlock()
	defer d.releaseNeed()

	select {
	case err := <-w.ch:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		// Remove this waiter so disconnected callers cannot accumulate in the
		// waiters map indefinitely (P1-3).
		d.mu.Lock()
		if ws := d.waiters[index]; len(ws) > 0 {
			for i, w2 := range ws {
				if w2 == w {
					d.waiters[index] = append(ws[:i], ws[i+1:]...)
					break
				}
			}
			if len(d.waiters[index]) == 0 {
				delete(d.waiters, index)
			}
		}
		d.mu.Unlock()
		return nil, ctx.Err()
	}
	return d.store.Get(d.filename, index)
}

// Progress returns the approximate downloaded bytes, total size, completion
// state and any terminal file error (e.g. banned).
func (d *Downloader) Progress() (downloaded int64, size int64, complete bool, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := int64(d.store.PieceCount(d.filename)) * PieceSize
	if n > d.size {
		n = d.size
	}
	return n, d.size, d.complete, d.fileErr
}

// stop cancels the downloader and fails all waiters. Used on file ban or
// agent shutdown.
func (d *Downloader) stop(err error) {
	d.mu.Lock()
	if d.fileErr == nil {
		d.fileErr = err
	}
	d.cancel()
	d.failWaitersLocked(err)
	d.mu.Unlock()
	d.releaseUpstreamLeases()
}

// fail marks the file failed and notifies every waiter.
func (d *Downloader) fail(err error) {
	d.mu.Lock()
	if d.fileErr == nil {
		d.fileErr = err
	}
	d.cancel()
	d.failWaitersLocked(err)
	d.mu.Unlock()
	d.releaseUpstreamLeases()
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

// wake unblocks a downloader waiting for upstreams and clears every piece
// cooldown (a topology change is a fresh opportunity to fetch); the one-shot
// demand-clear markers reset with them.
func (d *Downloader) wake() {
	d.mu.Lock()
	d.cooldown = make(map[int64]time.Time)
	d.cooldownCleared = make(map[int64]bool)
	d.mu.Unlock()
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
		complete := d.complete
		failed := d.fileErr != nil
		if complete && d.jobNeed {
			// The driving root job is satisfied; release its need.
			d.jobNeed = false
			if d.need > 0 {
				d.need--
			}
		}
		d.running = false
		d.mu.Unlock()
		// Terminal downloaders release their upstream leases (propagating the
		// stop), are reclaimed so the manager cannot grow unbounded, and the
		// file data stays in the PieceStore.
		if complete || failed {
			d.releaseUpstreamLeases()
			d.mgr.removeTerminal(d)
		}
	}()
	for {
		d.mu.Lock()
		if d.fileErr != nil || d.ctx.Err() != nil {
			d.mu.Unlock()
			break
		}
		// C4 (§5.2a/d): bind the authoritative metadata before the first
		// dispatch. A non-root (or a root with root upstreams — the failover
		// sealed-check) copies it from an upstream; a root that self-builds
		// (no sealed sibling) marks bindAttempted and proceeds (C2/C3).
		if d.meta == nil && !d.bindAttempted {
			d.mu.Unlock()
			if err := d.bindMetadata(d.ctx); err != nil {
				if errors.Is(err, errFileBanned) {
					d.fail(errFileBanned)
					return
				}
				// No usable upstream: wait for the topology to change or a
				// caller to retry.
				select {
				case <-time.After(noUpstreamRetry):
				case <-d.wakeCh:
				case <-d.ctx.Done():
					return
				}
				continue
			}
			continue
		}
		index := d.nextMissingLocked()
		if index < 0 {
			d.mu.Unlock()
			break
		}
		if d.cooldownCleared[index] {
			// This is the one allowed demand retry; re-apply the cooldown
			// immediately so a concurrent/dense run cannot dispatch the piece
			// again while it is being fetched (P2-1).
			d.cooldown[index] = time.Now().Add(pieceCooldown)
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

// nextMissingLocked returns the first piece not stored, not in flight and not
// cooling down, or -1 when nothing is dispatchable.
func (d *Downloader) nextMissingLocked() int64 {
	now := time.Now()
	for i := int64(0); i < d.numPieces; i++ {
		if d.inflight[i] {
			continue
		}
		if now.Before(d.cooldown[i]) {
			continue // persistent failure recently; back off (P1-2)
		}
		// A piece demand-cleared once must wait out its cooldown before being
		// dispatched again, even if the cooldown was cleared (P2-1 anti-flood).
		if d.cooldownCleared[i] && !d.store.HasPiece(d.filename, i) && now.Before(d.cooldown[i]) {
			continue
		}
		if !d.store.HasPiece(d.filename, i) {
			return i
		}
	}
	return -1
}

// bindMetadata acquires the authoritative metadata (§5.2a/d). A non-root
// downloader (and a root with root upstreams — the failover sealed-check,
// §4.4) copies it from an upstream BEFORE fetching pieces: GetFileInfo ->
// GetMetadata stream -> verify metadata_id == SHA-256(bytes) -> bind. The
// primary root (PullFromSource with no upstreams) self-builds at Seal time
// (C2/C3) and returns nil without binding.
func (d *Downloader) bindMetadata(ctx context.Context) error {
	d.metaBindMu.Lock()
	defer d.metaBindMu.Unlock()
	d.mu.Lock()
	if d.meta != nil {
		d.mu.Unlock()
		return nil
	}
	pullFromSource := d.topo.PullFromSource()
	upstreams := d.topo.UpstreamAddrs()
	d.mu.Unlock()
	if len(upstreams) == 0 {
		if pullFromSource {
			d.mu.Lock()
			d.bindAttempted = true
			d.mu.Unlock()
			return nil // primary root: self-build at Seal
		}
		return errNoUpstream
	}
	var lastErr error = errNoUpstream
	var selectedMeta *FileMetadataV1
	var selectedBytes []byte
	var selectedSize int64
	for _, addr := range upstreams {
		meta, metaBytes, size, err := d.fetchMetadataFrom(ctx, addr)
		if err != nil {
			if errors.Is(err, errFileBanned) || errors.Is(err, errContentConflict) {
				return err
			}
			lastErr = err
			continue // try the next upstream
		}
		if selectedMeta == nil {
			selectedMeta, selectedBytes, selectedSize = meta, metaBytes, size
			continue
		}
		if !bytes.Equal(MetadataID(selectedBytes), MetadataID(metaBytes)) {
			return fmt.Errorf("%w: upstreams returned different metadata_id values", errContentConflict)
		}
	}
	if selectedMeta != nil {
		d.mu.Lock()
		d.meta = selectedMeta
		d.metaBytes = selectedBytes
		d.metaID = MetadataID(selectedBytes)
		d.size = selectedSize
		d.numPieces = (selectedSize + PieceSize - 1) / PieceSize
		d.pieceHashes = make([][]byte, d.numPieces)
		boundID := append([]byte(nil), d.metaID...)
		d.mu.Unlock()
		log.Printf("agent %s: bound metadata for %s/%s metadata_id=%x", d.nodeID, d.treeID, d.filename, boundID[:min(len(boundID), 8)])
		return nil
	}
	// No upstream has a sealed artifact. A root (the failover case) then
	// builds from the source (self-build); a non-root waits for an upstream.
	if pullFromSource {
		d.mu.Lock()
		d.bindAttempted = true
		d.mu.Unlock()
		return nil
	}
	return lastErr
}

// FileInfo binds metadata without starting piece downloads and returns the
// resulting artifact description. This closes the first-download handshake:
// a caller asks its chosen node for FileInfo, that node resolves immutable
// metadata from its upstream, and only then does the caller issue GetPiece
// with the returned metadata_id.
func (d *Downloader) FileInfo(ctx context.Context) (*pppv1.FileInfo, error) {
	if err := d.bindMetadata(ctx); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.meta == nil || len(d.metaBytes) == 0 || len(d.metaID) != MetadataDigestSize {
		return nil, errNoUpstream
	}
	return &pppv1.FileInfo{
		Key:             &pppv1.TreeKey{TreeId: d.treeID, Filename: d.filename},
		FileSize:        d.meta.FileSize,
		PieceSize:       d.meta.PieceSize,
		PieceCount:      int64(d.meta.PieceCount),
		MetadataId:      append([]byte(nil), d.metaID...),
		MetadataSize:    int64(len(d.metaBytes)),
		DigestAlgorithm: d.meta.DigestAlgo,
	}, nil
}

// fetchMetadataFrom copies the sealed metadata from one upstream and verifies
// it (§5.2a/d): GetFileInfo -> GetMetadata stream -> metadata_id ==
// SHA-256(bytes) -> decode + cross-check the file size.
func (d *Downloader) fetchMetadataFrom(ctx context.Context, addr string) (*FileMetadataV1, []byte, int64, error) {
	client, err := d.peers.client(addr)
	if err != nil {
		return nil, nil, 0, err
	}
	callCtx, cancel := context.WithTimeout(ctx, pieceFetchTimeout)
	defer cancel()
	key := &pppv1.TreeKey{TreeId: d.treeID, Filename: d.filename}
	infoResp, err := client.GetFileInfo(callCtx, &pppv1.GetFileInfoRequest{Key: key})
	if err != nil {
		return nil, nil, 0, err
	}
	info, err := fileInfoFromResponse(infoResp)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(info.GetMetadataId()) != MetadataDigestSize {
		return nil, nil, 0, fmt.Errorf("%w: upstream returned a bad metadata_id", errMetadataCorrupt)
	}
	stream, err := client.GetMetadata(callCtx, &pppv1.GetMetadataRequest{Key: key, MetadataId: info.GetMetadataId()})
	if err != nil {
		// Banned consistency: the server returns PermissionDenied for a
		// banned file (the MetadataChunk message has no error field); map it
		// to errFileBanned so the downloader fails the file like the
		// GetFileInfo BANNED path.
		if status.Code(err) == codes.PermissionDenied {
			return nil, nil, 0, errFileBanned
		}
		return nil, nil, 0, err
	}
	// P1-2: bound the accumulation by the advertised metadata size (capped) so
	// a buggy/malicious upstream cannot stream arbitrary bytes and OOM the
	// receiver before the hash check; over-limit is METADATA_CORRUPT.
	limit := info.GetMetadataSize()
	if limit <= 0 || limit > maxMetadataSize {
		return nil, nil, 0, fmt.Errorf("%w: invalid advertised metadata size %d", errMetadataCorrupt, limit)
	}
	buf := make([]byte, 0, limit)
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Banned consistency (C5): the server sends PermissionDenied for a
			// banned file (the MetadataChunk message has no error field).
			if status.Code(err) == codes.PermissionDenied {
				log.Printf("agent %s: metadata banned %s/%s (GetMetadata PermissionDenied)", d.nodeID, d.treeID, d.filename)
				return nil, nil, 0, errFileBanned
			}
			return nil, nil, 0, err
		}
		if !bytes.Equal(chunk.GetMetadataId(), info.GetMetadataId()) {
			return nil, nil, 0, fmt.Errorf("%w: metadata chunk identity mismatch", errMetadataCorrupt)
		}
		if chunk.GetOffset() != int64(len(buf)) {
			return nil, nil, 0, fmt.Errorf("%w: metadata chunk offset %d != expected %d", errMetadataCorrupt, chunk.GetOffset(), len(buf))
		}
		if len(chunk.GetData()) == 0 {
			return nil, nil, 0, fmt.Errorf("%w: empty metadata chunk", errMetadataCorrupt)
		}
		if int64(len(buf))+int64(len(chunk.GetData())) > limit {
			return nil, nil, 0, fmt.Errorf("%w: metadata stream exceeds the advertised size %d", errMetadataCorrupt, limit)
		}
		buf = append(buf, chunk.GetData()...)
	}
	if int64(len(buf)) != info.GetMetadataSize() {
		return nil, nil, 0, fmt.Errorf("%w: metadata stream length %d != advertised %d", errMetadataCorrupt, len(buf), info.GetMetadataSize())
	}
	if !bytes.Equal(MetadataID(buf), info.GetMetadataId()) {
		return nil, nil, 0, fmt.Errorf("%w: metadata_id mismatch after copy", errMetadataCorrupt)
	}
	meta, err := DecodeMetadata(buf)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("%w: %v", errMetadataCorrupt, err)
	}
	// P1-1: the metadata must describe the file we asked for — never bind or
	// seal one artifact's metadata under another filename.
	if meta.Filename != d.filename {
		return nil, nil, 0, fmt.Errorf("%w: metadata filename %q != requested %q", errMetadataCorrupt, meta.Filename, d.filename)
	}
	if meta.FileSize != info.GetFileSize() {
		return nil, nil, 0, fmt.Errorf("%w: file size %d != info %d", errMetadataCorrupt, meta.FileSize, info.GetFileSize())
	}
	if meta.PieceSize != info.GetPieceSize() || int64(meta.PieceCount) != info.GetPieceCount() || meta.DigestAlgo != info.GetDigestAlgorithm() {
		return nil, nil, 0, fmt.Errorf("%w: metadata parameters do not match FileInfo", errMetadataCorrupt)
	}
	return meta, buf, info.GetFileSize(), nil
}

// fileInfoFromResponse extracts the FileInfo from a GetFileInfoResponse,
// mapping the error codes (BANNED -> errFileBanned, CONTENT_CONFLICT ->
// errContentConflict, etc).
func fileInfoFromResponse(resp *pppv1.GetFileInfoResponse) (*pppv1.FileInfo, error) {
	switch r := resp.GetResult().(type) {
	case *pppv1.GetFileInfoResponse_Info:
		if r.Info == nil {
			return nil, errors.New("agent: empty file info")
		}
		return r.Info, nil
	case *pppv1.GetFileInfoResponse_Error:
		return nil, mapPeerError(r.Error)
	}
	return nil, errors.New("agent: empty file info response")
}

// verifyPieceDigest checks a fetched piece against the bound metadata's
// expected SHA-256 (§5.2e / §5.1): a mismatch is a PIECE_DIGEST_MISMATCH —
// the piece is discarded and the upstream is treated as faulty (the fetch
// retries against another upstream). CRC64 remains the transport fast-check;
// the metadata digest is the content identity. A root with no bound metadata
// (self-build) has no expected digests yet and returns nil.
func (d *Downloader) verifyPieceDigest(index int64, data []byte) error {
	d.mu.Lock()
	meta := d.meta
	d.mu.Unlock()
	if meta == nil {
		return nil
	}
	want, err := meta.PieceDigest(int(index))
	if err != nil {
		return err
	}
	got := sha256.Sum256(data)
	if !bytes.Equal(got[:], want) {
		log.Printf("agent %s: piece digest mismatch %s/%s piece=%d metadata_id=%x", d.nodeID, d.treeID, d.filename, index, d.metaID[:min(len(d.metaID), 8)])
		return fmt.Errorf("%w: piece %d", errPieceDigestMismatch, index)
	}
	return nil
}

// checkCompleteLocked marks the file complete when every piece is stored and
// atomically publishes it: the per-piece SHA-256 digests (accumulated while
// fetching; read back for resumed pieces) are assembled into a FileMetadataV1
// and the artifact is sealed (data + metadata + commit marker).
//
// TRANSITION (C2 -> C4): in C2 the node SELF-BUILDS the metadata from its
// verified pieces — deterministic, so identical content yields the identical
// metadata_id. C4 changes the downloader to COPY the upstream metadata before
// fetching pieces and publish that exact bytes instead; since both describe
// the same content they carry the same metadata_id, so the transition is
// conflict-free.
func (d *Downloader) checkCompleteLocked() {
	if d.complete || d.fileErr != nil {
		return
	}
	for i := int64(0); i < d.numPieces; i++ {
		if !d.store.HasPiece(d.filename, i) {
			return
		}
	}
	d.complete = true
	if err := d.sealAndPublishLocked(); err != nil {
		d.complete = false
		d.fileErr = err
	}
}

// sealAndPublishLocked builds the FileMetadataV1 from the stored pieces and
// atomically publishes the artifact. Call with d.mu held.
func (d *Downloader) sealAndPublishLocked() error {
	// C4 (§5.2f): a downloader with bound (upstream-copied) metadata
	// publishes those exact bytes — the artifact identity is the copied
	// metadata, never a locally regenerated one. The root (no bound
	// metadata) self-builds below (C2/C3).
	if d.metaBytes != nil {
		return d.sealChecked(d.metaBytes)
	}
	pieceCount := int(d.numPieces)
	digests := make([][]byte, pieceCount)
	fileHash := sha256.New()
	for i := 0; i < pieceCount; i++ {
		if d.pieceHashes != nil && d.pieceHashes[i] != nil {
			digests[i] = d.pieceHashes[i]
		}
		data, err := d.store.Get(d.filename, int64(i))
		if err != nil {
			return fmt.Errorf("agent: seal read piece %d: %w", i, err)
		}
		if digests[i] == nil {
			// Resumed piece: digest the stored bytes now.
			h := sha256.Sum256(data)
			digests[i] = h[:]
		}
		fileHash.Write(data)
	}
	m, err := BuildMetadata(d.filename, d.size, PieceSize, digests, fileHash.Sum(nil))
	if err != nil {
		return err
	}
	metaBytes, err := m.Encode()
	if err != nil {
		return err
	}
	return d.sealChecked(metaBytes)
}

// sealChecked publishes the artifact bytes only when the local sealed
// artifact (if any) has the SAME metadata_id: a sequential rebuild that
// would overwrite a different sealed artifact is CONTENT_CONFLICT (C5/C6) —
// the existing artifact is never overwritten. Identical content (same id)
// re-seals idempotently.
func (d *Downloader) sealChecked(metaBytes []byte) error {
	if existing, ok, _ := d.store.ReadMetadata(d.filename); ok {
		if !bytes.Equal(MetadataID(existing), MetadataID(metaBytes)) {
			log.Printf("agent %s: content conflict %s/%s existing=%x new=%x", d.nodeID, d.treeID, d.filename, MetadataID(existing)[:8], MetadataID(metaBytes)[:8])
			return fmt.Errorf("%w: existing sealed artifact has a different metadata_id", errContentConflict)
		}
	}
	if err := d.store.Seal(d.filename, d.size, metaBytes); err != nil {
		return err
	}
	log.Printf("agent %s: sealed %s/%s metadata_id=%x", d.nodeID, d.treeID, d.filename, MetadataID(metaBytes)[:8])
	return nil
}

// fetchPiece fetches one piece with retries, stores it and notifies waiters.
func (d *Downloader) fetchPiece(index int64) {
	// Every exit path (including ctx cancellation) clears the in-flight mark,
	// so a silently stopped downloader restarted later can re-dispatch the
	// piece (P1-B). A demand-cleared retry that did not succeed re-enters the
	// cooldown so dense waiters cannot keep re-dispatching it (P2-1).
	defer func() {
		d.mu.Lock()
		delete(d.inflight, index)
		if d.cooldownCleared[index] && !d.store.HasPiece(d.filename, index) && d.fileErr == nil {
			d.cooldown[index] = time.Now().Add(pieceCooldown)
		}
		d.mu.Unlock()
	}()

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
			// Single-pass boundary validation (§4.2): every piece except the
			// last is exactly PieceSize; the last is the remainder. A source
			// that changed mid-pull (or a misbehaving peer) yields a
			// wrong-length piece, which fails the build instead of silently
			// mixing content versions.
			want := d.size - index*PieceSize
			if want > PieceSize {
				want = PieceSize
			}
			if int64(len(data)) != want {
				err = fmt.Errorf("agent: piece %d length %d != expected %d (source changed?)", index, len(data), want)
			}
		}
		if err == nil {
			// C4 (§5.2e): verify the piece SHA-256 against the bound
			// metadata's expected digest. A mismatch discards the piece and
			// marks the upstream faulty (the next attempt switches).
			err = d.verifyPieceDigest(index, data)
		}
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
		// Persistent failure: leave the piece missing and cool it down so a
		// dead or misbehaving peer cannot cause a hot retry loop. Waiters are
		// bounded by their own context; active demand or a topology change
		// clears the cooldown early.
		d.mu.Lock()
		if !errors.Is(err, errNoUpstream) {
			d.cooldown[index] = time.Now().Add(pieceCooldown)
		}
		d.mu.Unlock()
		return
	}
	if err := d.store.Put(d.filename, index, data); err != nil {
		return
	}
	// C2: accumulate the piece's SHA-256 as it is stored (CRC64 remains the
	// transport fast-check; the index still records crc64 existence).
	h := sha256.Sum256(data)
	d.mu.Lock()
	if index >= 0 && index < int64(len(d.pieceHashes)) {
		d.pieceHashes[index] = h[:]
	}
	d.mu.Unlock()
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

// leaseRPCTimeout bounds each upstream lease RPC.
const leaseRPCTimeout = 5 * time.Second

// maxMetadataSize caps a copied metadata stream (a buggy/malicious upstream
// must not be able to OOM the receiver before the hash check).
const maxMetadataSize = 64 << 20

// ensureUpstreamLeases renews this node's subscription to every upstream for
// this file (at most once per leaseTTL/2) while it is fetching. Parents keep
// the file's downloader alive (child need) for the lease duration; when this
// downloader stops and stops renewing, the leases expire and the stop
// propagates toward the source (design §3.3).
func (d *Downloader) ensureUpstreamLeases(ctx context.Context) {
	if d.leaseTTL <= 0 || d.topo.PullFromSource() {
		return
	}
	renewBefore := time.Now().Add(-d.leaseTTL / 2)
	for _, addr := range d.topo.UpstreamAddrs() {
		d.mu.Lock()
		last := d.upstreamLastRenew[addr]
		_, subscribed := d.upstreamLeases[addr]
		d.mu.Unlock()
		if subscribed && last.After(renewBefore) {
			continue // freshly renewed
		}
		if err := d.renewUpstreamLease(ctx, addr); err != nil {
			if errors.Is(err, errFileBanned) {
				d.fail(errFileBanned)
				return
			}
			// Best effort: GetPiece still works without a lease.
			continue
		}
	}
}

// renewUpstreamLease subscribes (idempotent renewal) to one upstream.
func (d *Downloader) renewUpstreamLease(ctx context.Context, addr string) error {
	client, err := d.peers.client(addr)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, leaseRPCTimeout)
	defer cancel()
	resp, err := client.Subscribe(callCtx, &pppv1.SubscribeRequest{
		Key:          &pppv1.TreeKey{TreeId: d.treeID, Filename: d.filename},
		JobId:        d.jobID,
		ChildNodeId:  d.nodeID,
		LeaseSeconds: int64(d.leaseTTL.Seconds()),
	})
	if err != nil {
		return err
	}
	if resp.GetBanned() {
		return errFileBanned
	}
	d.mu.Lock()
	d.upstreamLeases[addr] = struct{}{}
	d.upstreamLastRenew[addr] = time.Now()
	d.mu.Unlock()
	return nil
}

// releaseUpstreamLeases explicitly unsubscribes from every upstream so the
// stop propagates immediately instead of waiting for natural lease expiry.
func (d *Downloader) releaseUpstreamLeases() {
	d.mu.Lock()
	addrs := make([]string, 0, len(d.upstreamLeases))
	for addr := range d.upstreamLeases {
		addrs = append(addrs, addr)
	}
	d.upstreamLeases = make(map[string]struct{})
	d.mu.Unlock()
	for _, addr := range addrs {
		client, err := d.peers.client(addr)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), leaseRPCTimeout)
		_, _ = client.Unsubscribe(ctx, &pppv1.UnsubscribeRequest{
			Key:         &pppv1.TreeKey{TreeId: d.treeID, Filename: d.filename},
			JobId:       d.jobID,
			ChildNodeId: d.nodeID,
		})
		cancel()
	}
}

// fetchOnce fetches one piece from the source (primary root) or from the
// upstream parents.
func (d *Downloader) fetchOnce(index int64) ([]byte, error) {
	// C4 (§5.2f): once the authoritative metadata is bound from an upstream
	// (including a root that copied from a sibling — the failover case), the
	// pieces come from the upstreams; only a root that self-builds (no bound
	// metadata) pulls from the source.
	d.mu.Lock()
	bound := d.meta != nil
	d.mu.Unlock()
	if d.topo.PullFromSource() && !bound {
		if d.treeSource == nil {
			return nil, errors.New("agent: pull-from-source but no source configured")
		}
		return d.source.FetchPiece(d.ctx, d.treeSource, d.treeID, d.filename, index, d.size, PieceSize)
	}
	upstreams := d.topo.UpstreamAddrs()
	if len(upstreams) == 0 {
		return nil, errNoUpstream
	}
	d.ensureUpstreamLeases(d.ctx)
	var lastErr error
	for _, addr := range upstreams {
		data, err := d.fetchFromPeer(addr, index)
		if err == nil {
			// C4 (§5.2e): verify the piece SHA-256 against the bound
			// metadata BEFORE accepting it from this upstream; a mismatch is
			// PIECE_DIGEST_MISMATCH and the fetch switches to the next
			// upstream (the piece is discarded, never stored).
			if verr := d.verifyPieceDigest(index, data); verr != nil {
				lastErr = verr
				continue
			}
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
	// Bound each upstream call so a hung peer cannot starve the file; the
	// parent still gets pieceFetchTimeout to be downloading the piece itself.
	callCtx, cancel := context.WithTimeout(d.ctx, pieceFetchTimeout)
	defer cancel()
	resp, err := client.GetPiece(callCtx, &pppv1.GetPieceRequest{
		Key:   &pppv1.TreeKey{TreeId: d.treeID, Filename: d.filename},
		Index: index,
		Size:  d.size,
		JobId: d.jobID,
		From:  from,
		// C4 (§5.1): the request is bound to the validated metadata; an
		// upstream whose artifact differs returns CONTENT_CONFLICT and is
		// never mixed with the current content.
		MetadataId: d.metaID,
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
		h := p.GetInfo().GetHash()
		if h == 0 {
			// Defensive: a zero hash is invalid; treat it as a failed fetch.
			return nil, errors.New("agent: peer returned zero hash")
		}
		if crc64.Checksum(data, crcTable) != h {
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
	if e.GetCode() == pppv1.Error_CONTENT_CONFLICT {
		return errContentConflict
	}
	if e.GetCode() == pppv1.Error_PIECE_DIGEST_MISMATCH {
		return errPieceDigestMismatch
	}
	return fmt.Errorf("agent: peer error %s: %s", e.GetCode(), e.GetMessage())
}

// peerPool caches Data gRPC client connections per address.
// peerIdleTimeout is how long an unused peer connection is kept before the
// pool's opportunistic GC closes it, so topology churn cannot grow the pool
// unbounded.
const peerIdleTimeout = 10 * time.Minute

type peerPool struct {
	mu       sync.Mutex
	creds    credentials.TransportCredentials
	conns    map[string]*grpc.ClientConn
	clients  map[string]pppv1.DataClient
	lastUsed map[string]time.Time
}

func newPeerPool(creds credentials.TransportCredentials) *peerPool {
	return &peerPool{
		creds:    creds,
		conns:    make(map[string]*grpc.ClientConn),
		clients:  make(map[string]pppv1.DataClient),
		lastUsed: make(map[string]time.Time),
	}
}

func (p *peerPool) client(addr string) (pppv1.DataClient, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	// Opportunistic GC: close conns idle beyond the timeout (P3-optimization).
	for a, c := range p.conns {
		if now.Sub(p.lastUsed[a]) > peerIdleTimeout {
			_ = c.Close()
			delete(p.conns, a)
			delete(p.clients, a)
			delete(p.lastUsed, a)
		}
	}
	if c, ok := p.clients[addr]; ok {
		p.lastUsed[addr] = now
		return c, nil
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(credsOrInsecure(p.creds)),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxGRPCMessageSize),
			grpc.MaxCallSendMsgSize(maxGRPCMessageSize),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("agent: dial peer %s: %w", addr, err)
	}
	p.conns[addr] = conn
	p.lastUsed[addr] = now
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
	leaseTTL    time.Duration

	mu    sync.Mutex
	files map[string]*Downloader
}

// NewDownloaderManager creates the manager. treeSource is the tree default
// source (nil until the register response arrives); leaseTTL is the upstream
// session-lease duration downloaders request while fetching.
func NewDownloaderManager(store PieceStore, banned *BannedList, topo topologyProvider, source Source, treeSource *pppv1.Source, nodeID string, concurrency int, leaseTTL time.Duration, peerCreds credentials.TransportCredentials) *DownloaderManager {
	return &DownloaderManager{
		store:       store,
		banned:      banned,
		topo:        topo,
		source:      source,
		treeSource:  treeSource,
		peers:       newPeerPool(peerCreds),
		nodeID:      nodeID,
		concurrency: concurrency,
		leaseTTL:    leaseTTL,
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
		d = newDownloader(need, m.store, m.banned, m.topo, m.source, src, m.peers, m.nodeID, m.concurrency, m, m.leaseTTL)
		m.files[key] = d
	}
	d.Ensure(need.Size)
	return d
}

// Build isolation (§4.1): the manager keeps exactly ONE downloader per
// (tree, filename) key, so concurrent triggers for the same filename (even
// from different jobs) share that downloader — the store serializes the
// piece writes and the artifact is content-identical, so no two jobs can
// double-write or overwrite each other's staging. This simpler scheme (a
// shared downloader + the store mutex + an atomic Seal) satisfies the
// "different jobs, same filename must not destroy each other" requirement;
// per-Job staging directories are not needed because the shared downloader
// IS the build mutual exclusion (later arrivals reuse/wait).

// IsBuilding reports whether an unsealed downloader is actively fetching (the
// BUILDING state, §3.4): a root must not serve such files (NOT_READY) and a
// GetPiece must not re-trigger their build.
func (m *DownloaderManager) IsBuilding(treeID, filename string) bool {
	d := m.Get(treeID, filename)
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.running && !d.complete && d.fileErr == nil
}

// BoundMetadataID returns the active downloader's bound metadata_id (empty
// when none is bound yet). Used by the dataserver to serve in-progress pieces
// (the root-below pipeline) only under the artifact's true identity.
func (m *DownloaderManager) BoundMetadataID(treeID, filename string) []byte {
	d := m.Get(treeID, filename)
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]byte(nil), d.metaID...)
}

// Get returns the active downloader for a file, or nil.
func (m *DownloaderManager) Get(treeID, filename string) *Downloader {
	key := treeID + "\x00" + filename
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.files[key]
}

// removeTerminal drops a terminal (complete or failed) downloader from the
// map so the manager cannot grow unbounded. The guard prevents removing a
// newer downloader that replaced this one.
func (m *DownloaderManager) removeTerminal(d *Downloader) {
	key := d.treeID + "\x00" + d.filename
	m.mu.Lock()
	if m.files[key] == d {
		delete(m.files, key)
	}
	m.mu.Unlock()
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

// CancelAll stops every downloader (agent shutdown).
func (m *DownloaderManager) CancelAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, d := range m.files {
		delete(m.files, key)
		d.stop(errors.New("agent: stopping"))
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
