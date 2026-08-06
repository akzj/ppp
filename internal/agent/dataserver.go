package agent

import (
	"context"
	"errors"
	"hash/crc64"
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
	store  PieceStore
	banned *BannedList
	dm     *DownloaderManager
	leases *LeaseManager
}

// NewDataServer creates the Data service handler.
func NewDataServer(nodeID string, store PieceStore, banned *BannedList, dm *DownloaderManager, leases *LeaseManager) *DataServer {
	return &DataServer{nodeID: nodeID, store: store, banned: banned, dm: dm, leases: leases}
}

func errResp(code pppv1.Error_ErrorCode, msg string) *pppv1.GetPieceResponse {
	return &pppv1.GetPieceResponse{Result: &pppv1.GetPieceResponse_Error{
		Error: &pppv1.Error{Code: code, Message: msg},
	}}
}

// GetPiece serves one piece. A miss triggers a full-file subtask download
// (the default mode for machine-room distribution), caches it locally, then
// serves the requested piece.
func (s *DataServer) GetPiece(ctx context.Context, req *pppv1.GetPieceRequest) (*pppv1.GetPieceResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return errResp(pppv1.Error_BAD_REQUEST, "key (tree_id, filename) is required"), nil
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
	if data, err := s.store.Get(key.GetTreeId(), key.GetFilename(), req.GetIndex()); err == nil {
		return s.pieceResponse(key, req.GetIndex(), data), nil
	} else if !errors.Is(err, ErrPieceNotFound) {
		return nil, status.Error(codes.Internal, err.Error())
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
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return stream.Send(&pppv1.ProgressState{
			TreeId: key.GetTreeId(), Filename: key.GetFilename(), Size: req.GetSize(),
			State: pppv1.ProgressState_BANNED,
		})
	}
	d := s.dm.Ensure(FileNeed{
		TreeID:   key.GetTreeId(),
		Filename: key.GetFilename(),
		Size:     req.GetSize(),
		JobID:    req.GetJobId(),
		From:     req.GetFrom(),
	})
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
func (s *DataServer) Subscribe(_ context.Context, req *pppv1.SubscribeRequest) (*pppv1.SubscribeResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" || req.GetChildNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "key and child_node_id are required")
	}
	if s.banned.IsBanned(key.GetTreeId(), key.GetFilename()) {
		return &pppv1.SubscribeResponse{Accepted: false, Banned: true}, nil
	}
	lease := time.Duration(req.GetLeaseSeconds()) * time.Second
	if lease <= 0 {
		lease = s.leases.ttl
	}
	s.leases.Renew(key.GetTreeId(), key.GetFilename(), req.GetJobId(), req.GetChildNodeId(), time.Now())
	return &pppv1.SubscribeResponse{Accepted: true, GrantedLeaseSeconds: int64(lease.Seconds())}, nil
}

// Unsubscribe removes a session lease.
func (s *DataServer) Unsubscribe(_ context.Context, req *pppv1.UnsubscribeRequest) (*pppv1.UnsubscribeResponse, error) {
	key := req.GetKey()
	if key == nil || key.GetTreeId() == "" || key.GetFilename() == "" {
		return nil, status.Error(codes.InvalidArgument, "key is required")
	}
	s.leases.Remove(key.GetTreeId(), key.GetFilename(), req.GetJobId(), req.GetChildNodeId())
	return &pppv1.UnsubscribeResponse{Ok: true}, nil
}
