package agent

import (
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
	// Local hit.
	if data, err := s.store.Get(key.GetFilename(), req.GetIndex()); err == nil {
		return s.pieceResponse(key, req.GetIndex(), data), nil
	} else if !errors.Is(err, ErrPieceNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
	}
	// BUILDING gate (decision 1, §3.4): a root that is actively building the
	// artifact must not serve pieces and must not have a GetPiece re-trigger
	// its build. "No artifact" (no downloader) may still trigger a build via
	// the back-to-source path below; "building" (a running, unsealed
	// downloader) is NOT_READY.
	if s.root && s.dm.IsBuilding(key.GetTreeId(), key.GetFilename()) {
		return errResp(pppv1.Error_NOT_READY, "artifact is building"), nil
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
