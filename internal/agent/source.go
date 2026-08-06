package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// ErrSourceNotImplemented is returned for source types without an
// implementation yet (OSS/S3 arrive in phase 4).
var ErrSourceNotImplemented = errors.New("agent: source type not implemented")

// Source fetches file pieces from the origin.
type Source interface {
	// FetchPiece returns the bytes of piece index of the file.
	FetchPiece(ctx context.Context, src *pppv1.Source, treeID, filename string, index, size, pieceSize int64) ([]byte, error)
}

// NewSource returns the Source implementation for the given source type.
func NewSource(typ pppv1.Source_Type) (Source, error) {
	switch typ {
	case pppv1.Source_HTTP, pppv1.Source_HTTPS:
		return &httpSource{client: newHTTPClient()}, nil
	case pppv1.Source_OSS, pppv1.Source_S3:
		return nil, fmt.Errorf("%w: %s", ErrSourceNotImplemented, typ)
	case pppv1.Source_TYPE_UNSPECIFIED:
		return nil, errors.New("agent: source type unspecified")
	default:
		return nil, fmt.Errorf("agent: unknown source type %v", typ)
	}
}

// newHTTPClient returns the HTTP client used for source fetches.
func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second}
}

// dispatchSource is the agent's default Source: it dispatches every fetch on
// the source type carried by the tree/job Source message. HTTP/HTTPS are
// implemented in phase 2; OSS/S3 arrive in phase 4.
type dispatchSource struct {
	http *httpSource
}

func (d *dispatchSource) FetchPiece(ctx context.Context, src *pppv1.Source, treeID, filename string, index, size, pieceSize int64) ([]byte, error) {
	switch src.GetType() {
	case pppv1.Source_HTTP, pppv1.Source_HTTPS:
		return d.http.FetchPiece(ctx, src, treeID, filename, index, size, pieceSize)
	case pppv1.Source_OSS, pppv1.Source_S3:
		return nil, fmt.Errorf("%w: %s", ErrSourceNotImplemented, src.GetType())
	default:
		return nil, fmt.Errorf("agent: source type %v not supported", src.GetType())
	}
}

// httpSource fetches pieces over HTTP(S) with a Range request. Multiple urls
// act as mirrors: the first one that returns the expected bytes wins.
type httpSource struct {
	client *http.Client
}

func (h *httpSource) FetchPiece(ctx context.Context, src *pppv1.Source, treeID, filename string, index, size, pieceSize int64) ([]byte, error) {
	if src == nil || len(src.GetUrls()) == 0 {
		return nil, errors.New("agent: source has no urls")
	}
	offset := index * pieceSize
	end := offset + pieceSize - 1
	if end > size-1 {
		end = size - 1
	}
	var lastErr error
	for _, url := range src.GetUrls() {
		data, err := h.fetchRange(ctx, url, src.GetHeaders(), offset, end)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (h *httpSource) fetchRange(ctx context.Context, url string, headers map[string]string, offset, end int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, end))

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		return nil, fmt.Errorf("agent: source range %d-%d not satisfiable", offset, end)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("agent: source GET %s: status %d", url, resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	// A 200 means the server ignored the Range header and sent the whole
	// body; keep only the requested window (which starts at offset).
	expected := end - offset + 1
	if resp.StatusCode == http.StatusOK && int64(len(data)) >= offset+expected {
		data = data[offset : offset+expected]
	}
	if int64(len(data)) != expected {
		return nil, fmt.Errorf("agent: source returned %d bytes, want %d", len(data), expected)
	}
	return data, nil
}
