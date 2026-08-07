package agent

import (
	"bytes"
	"context"
	"errors"
	"hash/crc64"
	"path/filepath"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DataServer implements the Data gRPC service: piece serving with subtask
// back-to-source, whole-file progress streaming and session leases.
type DataServer struct {
	pppv1.UnimplementedDataServer

	nodeID string
	// treeID is this node's tree id; requests for another tree are rejected
	// (storage is tree-agnostic but the data plane is single-tree).
	treeID string
	// downloadPath is the piece store root; used to resolve local_path.
	downloadPath string
	store        PieceStore
	banned       *BannedList
	dm           *DownloaderManager
	leases       *LeaseManager
	// root marks a primary-root node: BUILDING artifacts (decision 1) are
	// invisible to GetPiece/GetFileInfo until sealed.
	root bool
}

// NewDataServer creates the Data service handler (non-root).
func NewDataServer(nodeID, treeID, downloadPath string, store PieceStore, banned *BannedList, dm *DownloaderManager, leases *LeaseManager) *DataServer {
	return newRootDataServer(nodeID, treeID, downloadPath, store, banned, dm, leases, false)
}

// newRootDataServer constructs a DataServer with an explicit root flag.
func newRootDataServer(nodeID, treeID, downloadPath string, store PieceStore, banned *BannedList, dm *DownloaderManager, leases *LeaseManager, root bool) *DataServer {
	return &DataServer{
		nodeID: nodeID, treeID: treeID, downloadPath: downloadPath,
		store: store, banned: banned, dm: dm, leases: leases, root: root,
	}
}

func errResp(code pppv1.Error_ErrorCode, msg string) *pppv1.GetPieceResponse {
	return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Error{
		Error: &pppv1.Error{Code: code, Message: msg},
	}}
}

// validateKey rejects keys for another tree or with an unsafe filename.
// Returns a non-empty message when invalid, empty otherwise.
func (s *DataServer) validateKey(treeID, filename string) string {
	if treeID != s.treeID {
		return "tree_id mismatch"
	}
	if !validBasename(filename) {
		return "invalid filename"
	}
	return ""
}

// GetPiece serves one piece. A miss triggers a full-file subtask download
// (the default mode for machine-room distribution), caches it locally, then
// serves the requested piece.
func (s *DataServer) GetPiece(ctx context.Context, req *pppv1.GetPieceRequest) (*pppv1.GetPieceResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return errResp(pppv1.Error_BAD_REQUEST, "key (tree_id, filename) is required"), nil
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return errResp(pppv1.Error_BAD_REQUEST, msg), nil
	}
	if req.GetSize() <= 0 || req.GetIndex() < 0 || req.GetIndex()*PieceSize >= req.GetSize() {
		return errResp(pppv1.Error_BAD_REQUEST, "invalid piece index or file size"), nil
	}
	// C4 (§5.1): every request is bound to the validated metadata_id.
	if len(req.GetMetadataId()) == 0 {
		return errResp(pppv1.Error_BAD_REQUEST, "metadata_id is required"), nil
	}
	// Loop prevention: a request whose hop chain already contains this node
	// has come back around.
	for _, hop := range req.GetFrom() {
		if hop.GetNodeId() == s.nodeID {
			return errResp(pppv1.Error_LOOP_DETECTED, "loop detected"), nil
		}
	}
	// Banned gate: banned files are neither served nor back-to-source.
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return errResp(pppv1.Error_BANNED, "file is banned"), nil
	}
	// Local hit: a sealed artifact is served only when the request's
	// metadata_id matches its own; a mismatch is CONTENT_CONFLICT (§5.1) —
	// never serve one artifact under another artifact's identity.
	if data, err := s.store.Get(key.GetFilename(), req.GetIndex()); err == nil {
		if meta, ok, _ := s.store.ReadMetadata(key.GetFilename()); ok && !bytes.Equal(req.GetMetadataId(), MetadataID(meta)) {
			return errResp(pppv1.Error_CONTENT_CONFLICT, "content conflict: metadata_id mismatch"), nil
		}
		return s.pieceResponse(key, req.GetIndex(), data), nil
	} else if !errors.Is(err, ErrPieceNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Root three-state gate (decision 1, invariant #4, §3.4/§4.3): a root's
	// GetPiece serves only a SEALED artifact (the local hit above). While the
	// artifact is BUILDING (a running, unsealed downloader) it is NOT_READY,
	// and when there is no sealed artifact AND no build in progress it is also
	// NOT_READY — a root's build is driven by the Job (watchJobsLoop), never
	// by a downstream request, so this request is neither served nor triggers
	// a build (the C3 gate previously leaked the no-artifact case through to
	// the back-to-source path, starting a build and serving mid-build pieces).
	if s.root {
		if s.dm.IsBuilding(key.GetTreeId(), key.GetFilename()) {
			return errResp(pppv1.Error_NOT_READY, "artifact is building"), nil
		}
		return errResp(pppv1.Error_NOT_READY, "artifact is not ready"), nil
	}
	// Subtask back-to-source: ensure the downloader exists and wait for the
	// piece.
	d := s.dm.Ensure(FileNeed{
		TreeID:   key.GetTreeId(),
		Filename: key.GetFilename(),
		Size:     req.GetSize(),
		JobID:    req.GetJobId(),
		From:     req.GetFrom(),
	})
	data, err := d.WaitPiece(ctx, req.GetIndex())
	if err != nil {
		if errors.Is(err, errFileBanned) {
			return errResp(pppv1.Error_BANNED, "file is banned"), nil
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, err
		}
		return errResp(pppv1.Error_INTERNAL, err.Error()), nil
	}
	return s.pieceResponse(key, req.GetIndex(), data), nil
}

func (s *DataServer) pieceResponse(key *pppv1.TreeKey, index int64, data []byte) *pppv1.GetPieceResponse {
	info := &pppv1.PieceInfo{
		Hash:   crc64.Checksum(data, crcTable),
		Index:  index,
		Size:   int32(len(data)),
		Offset: index * PieceSize,
	}
	return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Piece{
		Piece: &pppv1.Piece{Info: info, Data: data},
	}}
}

// metadataChunkSize is the GetMetadata stream chunk size.
const metadataChunkSize = 64 << 10

// sealedFileInfo returns the FileInfo of a locally sealed artifact (ok=false
// when absent or unsealed). The metadata sidecar is authoritative: the
// metadata_id is SHA-256 over its bytes.
func (s *DataServer) sealedFileInfo(key *pppv1.TreeKey) (*pppv1.FileInfo, bool) {
	meta, ok, err := s.store.ReadMetadata(key.GetFilename())
	if err != nil || !ok {
		return nil, false
	}
	if !s.store.IsComplete(key.GetFilename()) {
		return nil, false
	}
	m, err := DecodeMetadata(meta)
	if err != nil {
		return nil, false
	}
	return &pppv1.FileInfo{
		Key:             key,
		FileSize:        m.FileSize,
		PieceSize:       m.PieceSize,
		PieceCount:      int64(m.PieceCount),
		MetadataId:      MetadataID(meta),
		MetadataSize:    int64(len(meta)),
		DigestAlgorithm: m.DigestAlgo,
	}, true
}

// GetFileInfo returns the sealed artifact's info (§5.1): banned -> BANNED;
// sealed -> FileInfo; building -> NOT_READY; no artifact -> NOT_FOUND (no
// build is triggered — a root builds only via Job, a non-root via GetPiece).
func (s *DataServer) GetFileInfo(_ context.Context, req *pppv1.GetFileInfoRequest) (*pppv1.GetFileInfoResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "key (tree_id, filename) is required")
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return &pppv1.GetFileInfoResponse{Result: &pppv1.GetFileInfoResponse_Error{
			Error: &pppv1.Error{Code: pppv1.Error_BANNED, Message: "file is banned"},
		}}, nil
	}
	if info, ok := s.sealedFileInfo(key); ok {
		return &pppv1.GetFileInfoResponse{Result: &pppv1.GetFileInfoResponse_Info{Info: info}}, nil
	}
	if s.dm.IsBuilding(key.GetTreeId(), key.GetFilename()) {
		return &pppv1.GetFileInfoResponse{Result: &pppv1.GetFileInfoResponse_Error{
			Error: &pppv1.Error{Code: pppv1.Error_NOT_READY, Message: "artifact is building"},
		}}, nil
	}
	return &pppv1.GetFileInfoResponse{Result: &pppv1.GetFileInfoResponse_Error{
		Error: &pppv1.Error{Code: pppv1.Error_NOT_FOUND, Message: "artifact not found"},
	}}, nil
}

// GetMetadata streams the sealed metadata bytes in chunks (§5.1): banned /
// not-ready / not-found / content-conflict are returned as gRPC statuses
// (the MetadataChunk message has no error field); a sealed artifact with a
// matching metadata_id streams its canonical bytes.
func (s *DataServer) GetMetadata(req *pppv1.GetMetadataRequest, stream pppv1.Data_GetMetadataServer) error {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return status.Error(codes.InvalidArgument, "key (tree_id, filename) is required")
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return status.Error(codes.InvalidArgument, msg)
	}
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return status.Error(codes.PermissionDenied, "file is banned")
	}
	meta, ok, err := s.store.ReadMetadata(key.GetFilename())
	if err != nil || !ok || !s.store.IsComplete(key.GetFilename()) {
		if s.dm.IsBuilding(key.GetTreeId(), key.GetFilename()) {
			return status.Error(codes.FailedPrecondition, "artifact is building")
		}
		return status.Error(codes.NotFound, "artifact not found")
	}
	want := req.GetMetadataId()
	if len(want) > 0 && !bytes.Equal(want, MetadataID(meta)) {
		return status.Error(codes.FailedPrecondition, "content conflict: metadata_id mismatch")
	}
	metaID := MetadataID(meta)
	for off := 0; off < len(meta); off += metadataChunkSize {
		end := off + metadataChunkSize
		if end > len(meta) {
			end = len(meta)
		}
		if err := stream.Send(&pppv1.MetadataChunk{
			MetadataId: metaID,
			Offset:     int64(off),
			Data:       meta[off:end],
		}); err != nil {
			return err
		}
	}
	return nil
}

// DownloadFile streams whole-file download progress for leaf consumers.
func (s *DataServer) DownloadFile(req *pppv1.DownloadFileRequest, stream pppv1.Data_DownloadFileServer) error {
	ctx := stream.Context()
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" || req.GetSize() <= 0 {
		return status.Error(codes.InvalidArgument, "key and a positive size are required")
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return status.Error(codes.InvalidArgument, msg)
	}
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return stream.Send(&pppv1.ProgressState{
			TreeId: key.GetTreeId(), Filename: key.GetFilename(), Size: req.GetSize(),
			State:     pppv1.ProgressState_BANNED,
			LocalPath: filepath.Join(s.downloadPath, key.GetFilename()),
		})
	}
	d := s.dm.Ensure(FileNeed{
		TreeID:   key.GetTreeId(),
		Filename: key.GetFilename(),
		Size:     req.GetSize(),
		JobID:    req.GetJobId(),
		From:     req.GetFrom(),
	})
	// INVARIANT: every path that needs a file must addNeed (or an equivalent
	// start) — Ensure only records the size and never starts the fetch loop.
	// The DownloadFile caller is a local need: addNeed starts fetching and
	// keeps the downloader alive while the stream is open; releaseNeed lets it
	// stop when the leaf leaves. addNeed is harmless when the file is already
	// complete (the short-lived run finds no missing pieces).
	d.addNeed()
	defer d.releaseNeed()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
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
		if err := stream.Send(&pppv1.ProgressState{
			TreeId: key.GetTreeId(), Filename: key.GetFilename(), Size: size,
			DownloadedBytes: downloaded, Progress: progressPercent(downloaded, size), State: state,
			LocalPath: filepath.Join(s.downloadPath, key.GetFilename()),
		}); err != nil {
			return err
		}
		if complete || err != nil {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

// progressPercent converts downloaded bytes into a 0-100 percentage.
func progressPercent(downloaded, size int64) int32 {
	if size <= 0 {
		return 0
	}
	p := downloaded * 100 / size
	if p > 100 {
		p = 100
	}
	return int32(p)
}

// Subscribe (re)establishes a session lease for a child; idempotent renewal.
// The granted duration is min(requested, server TTL) and matches the actual
// expiry stored by the lease manager, so a child renewing by the granted
// duration never falls out of alignment. Subscribing also adds one unit of
// child need to the file's downloader (kept alive while subscribed).
func (s *DataServer) Subscribe(_ context.Context, req *pppv1.SubscribeRequest) (*pppv1.SubscribeResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" || req.GetChildNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "key and child_node_id are required")
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return &pppv1.SubscribeResponse{Accepted: false, Banned: true}, nil
	}
	requested := time.Duration(req.GetLeaseSeconds()) * time.Second
	if requested <= 0 || requested > s.leases.ttl {
		requested = s.leases.ttl
	}
	created := s.leases.Renew(requested, key.GetTreeId(), key.GetFilename(), req.GetJobId(), req.GetChildNodeId(), time.Now())
	// Child need is added only when the subscription is NEW: idempotent
	// renewals must not grow the need counter. Files already fully cached
	// need no downloader (serving is store hits only).
	if created && !s.store.IsComplete(key.GetFilename()) {
		s.dm.Ensure(FileNeed{TreeID: key.GetTreeId(), Filename: key.GetFilename()}).addNeed()
	}
	return &pppv1.SubscribeResponse{Accepted: true, GrantedLeaseSeconds: int64(requested.Seconds())}, nil
}

// Unsubscribe removes a session lease and releases its child need.
func (s *DataServer) Unsubscribe(_ context.Context, req *pppv1.UnsubscribeRequest) (*pppv1.UnsubscribeResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	if s.leases.Remove(key.GetTreeId(), key.GetFilename(), req.GetJobId(), req.GetChildNodeId()) {
		if d := s.dm.Get(key.GetTreeId(), key.GetFilename()); d != nil {
			d.releaseNeed()
		}
	}
	return &pppv1.UnsubscribeResponse{Ok: true}, nil
}

// ResolvePath returns the final on-disk path of a file on this node and
// whether it is currently present (complete).
func (s *DataServer) ResolvePath(_ context.Context, req *pppv1.ResolvePathRequest) (*pppv1.ResolvePathResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "key (tree_id, filename) is required")
	}
	if msg := s.validateKey(key.GetTreeId(), key.GetFilename()); msg != "" {
		return nil, status.Error(codes.InvalidArgument, msg)
	}
	return &pppv1.ResolvePathResponse{
		LocalPath: filepath.Join(s.downloadPath, key.GetFilename()),
		Exist:     s.store.IsComplete(key.GetFilename()),
	}, nil
}
