package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
		return &s3Source{}, nil
	case pppv1.Source_TYPE_UNSPECIFIED:
		return nil, errors.New("agent: source type unspecified")
	default:
		return nil, fmt.Errorf("agent: unknown source type %v", typ)
	}
}

// s3Source fetches pieces from S3-compatible object storage (OSS/MinIO/AWS S3)
// through the AWS SDK v2 with GetObject + a Range header, mirroring the HTTP
// source's piece-window model.
//
// Endpoint convention (no proto change): Source.urls[0] is used as a custom
// BaseEndpoint for OSS/MinIO/S3-compatible stores; when empty, the region's
// default AWS endpoint is used. Multiple urls act as mirrors with failover.
// Bucket/Key come from Source.bucket/Source.key. Credentials come from the
// environment (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY); region from
// Source.region or the environment.
type s3Source struct {
	mu       sync.Mutex
	client   *s3.Client
	built    bool
	buildErr error
}

// clientFor lazily builds the S3 client (once) from the source configuration.
func (s *s3Source) clientFor(src *pppv1.Source) (*s3.Client, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.built {
		return s.client, s.buildErr
	}
	opts := []func(*awsconfig.LoadOptions) error{}
	if region := src.GetRegion(); region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		s.built = true
		s.buildErr = err
		return nil, err
	}
	s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		// Path-style addressing works with OSS/MinIO/S3-compatible stores and
		// the test endpoint.
		o.UsePathStyle = true
		// S3-compatible endpoints often omit response checksums; validating
		// only when required avoids noisy warnings.
		o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
	})
	s.built = true
	return s.client, nil
}

// s3GetTimeout bounds each GetObject call.
const s3GetTimeout = 60 * time.Second

func (s *s3Source) FetchPiece(ctx context.Context, src *pppv1.Source, treeID, filename string, index, size, pieceSize int64) ([]byte, error) {
	if src == nil || src.GetBucket() == "" || src.GetKey() == "" {
		return nil, errors.New("agent: s3 source requires bucket and key")
	}
	client, err := s.clientFor(src)
	if err != nil {
		return nil, err
	}
	offset := index * pieceSize
	end := offset + pieceSize - 1
	if end > size-1 {
		end = size - 1
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", offset, end)
	expected := end - offset + 1

	// Mirror failover: each url is a base endpoint; empty means the client's
	// default (region-based) endpoint.
	urls := src.GetUrls()
	if len(urls) == 0 {
		urls = []string{""}
	}
	var lastErr error
	for _, endpoint := range urls {
		data, err := s.fetchOnce(ctx, client, src, endpoint, rangeHeader, expected)
		if err == nil {
			return data, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

func (s *s3Source) fetchOnce(ctx context.Context, client *s3.Client, src *pppv1.Source, endpoint, rangeHeader string, expected int64) ([]byte, error) {
	callCtx, cancel := context.WithTimeout(ctx, s3GetTimeout)
	defer cancel()
	out, err := client.GetObject(callCtx, &s3.GetObjectInput{
		Bucket: aws.String(src.GetBucket()),
		Key:    aws.String(src.GetKey()),
		Range:  aws.String(rangeHeader),
	}, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("agent: s3 get %s/%s: %w", src.GetBucket(), src.GetKey(), err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != expected {
		return nil, fmt.Errorf("agent: s3 returned %d bytes, want %d", len(data), expected)
	}
	return data, nil
}

// newHTTPClient returns the HTTP client used for source fetches.
func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second}
}

// dispatchSource is the agent's default Source: it dispatches every fetch on
// the source type carried by the tree/job Source message.
type dispatchSource struct {
	http *httpSource
	s3   *s3Source
}

func (d *dispatchSource) FetchPiece(ctx context.Context, src *pppv1.Source, treeID, filename string, index, size, pieceSize int64) ([]byte, error) {
	switch src.GetType() {
	case pppv1.Source_HTTP, pppv1.Source_HTTPS:
		return d.http.FetchPiece(ctx, src, treeID, filename, index, size, pieceSize)
	case pppv1.Source_OSS, pppv1.Source_S3:
		return d.s3.FetchPiece(ctx, src, treeID, filename, index, size, pieceSize)
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
