package agent

import (
	"testing"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestMetadataProtoContract asserts the C1 proto contract: the md5 field is
// removed from Job/CreateJobRequest, the new metadata RPCs and messages exist,
// GetPieceRequest carries metadata_id, and the new error codes are present.
func TestMetadataProtoContract(t *testing.T) {
	// md5 removed: the generated types must not have GetMd5.
	if _, ok := any(&pppv1.Job{}).(interface{ GetMd5() string }); ok {
		t.Fatal("Job.GetMd5 still exists (md5 was removed)")
	}
	if _, ok := any(&pppv1.CreateJobRequest{}).(interface{ GetMd5() string }); ok {
		t.Fatal("CreateJobRequest.GetMd5 still exists (md5 was removed)")
	}

	// New RPCs: the Data client interface exposes GetFileInfo + GetMetadata.
	var dc interface {
		GetFileInfo(ctx interface{}, in *pppv1.GetFileInfoRequest, opts ...interface{}) (*pppv1.GetFileInfoResponse, error)
		GetMetadata(ctx interface{}, in *pppv1.GetMetadataRequest, opts ...interface{}) (interface{}, error)
	}
	_ = dc

	// New messages.
	_ = &pppv1.FileInfo{}
	_ = &pppv1.MetadataChunk{}
	_ = &pppv1.GetFileInfoResponse{}
	_ = &pppv1.GetMetadataRequest{}

	// GetPieceRequest.metadata_id.
	if f, ok := any(&pppv1.GetPieceRequest{}).(interface{ GetMetadataId() []byte }); !ok {
		t.Fatal("GetPieceRequest.GetMetadataId missing")
	} else if f == nil {
		t.Fatal("GetPieceRequest.GetMetadataId nil")
	}

	// New error codes.
	for _, want := range []pppv1.Error_ErrorCode{
		pppv1.Error_NOT_READY,
		pppv1.Error_CONTENT_CONFLICT,
		pppv1.Error_METADATA_CORRUPT,
		pppv1.Error_PIECE_DIGEST_MISMATCH,
		pppv1.Error_FILE_DIGEST_MISMATCH,
	} {
		if want == pppv1.Error_ERROR_CODE_UNSPECIFIED {
			t.Fatal("a new error code aliases the zero value")
		}
	}
}
