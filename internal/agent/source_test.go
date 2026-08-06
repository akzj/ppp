package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pppv1 "github.com/akzj/ppp/gen/ppp/v1"
)

// TestSourceHTTPRange verifies piece fetches over HTTP with Range support.
func TestSourceHTTPRange(t *testing.T) {
	content := []byte("0123456789abcdefghij") // 20 bytes
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var start, end int
		if _, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if start < 0 || end >= len(content) || start > end {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(content[start : end+1])
	}))
	defer srv.Close()

	src := &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{srv.URL}}
	s, err := NewSource(pppv1.Source_HTTP)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}

	// Piece 0 with pieceSize 8 -> bytes 0..7.
	p0, err := s.FetchPiece(context.Background(), src, "t1", "f", 0, int64(len(content)), 8)
	if err != nil {
		t.Fatalf("FetchPiece(0): %v", err)
	}
	if string(p0) != "01234567" {
		t.Fatalf("piece 0 = %q, want 01234567", p0)
	}
	// Piece 2 -> bytes 16..19 (last piece shorter).
	p2, err := s.FetchPiece(context.Background(), src, "t1", "f", 2, int64(len(content)), 8)
	if err != nil {
		t.Fatalf("FetchPiece(2): %v", err)
	}
	if string(p2) != "ghij" {
		t.Fatalf("piece 2 = %q, want ghij", p2)
	}
	// Out-of-range piece.
	if _, err := s.FetchPiece(context.Background(), src, "t1", "f", 3, int64(len(content)), 8); err == nil {
		t.Fatal("FetchPiece(out of range) = nil error, want error")
	}
}

// TestSourceHTTPIgnoresRange verifies a server that ignores Range (returns
// 200 with the whole body) still yields the right window.
func TestSourceHTTPIgnoresRange(t *testing.T) {
	content := []byte("0123456789abcdefghij")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content) // ignore Range, always 200
	}))
	defer srv.Close()

	s, err := NewSource(pppv1.Source_HTTP)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	src := &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{srv.URL}}
	p1, err := s.FetchPiece(context.Background(), src, "t1", "f", 1, int64(len(content)), 8)
	if err != nil {
		t.Fatalf("FetchPiece: %v", err)
	}
	if string(p1) != "89abcdef" {
		t.Fatalf("piece 1 = %q, want 89abcdef", p1)
	}
}

// TestSourceHTTPMirrorFailover verifies a failing mirror falls back to the
// next url.
func TestSourceHTTPMirrorFailover(t *testing.T) {
	content := []byte("abcdef")
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(content)
	}))
	defer good.Close()

	s, err := NewSource(pppv1.Source_HTTP)
	if err != nil {
		t.Fatalf("NewSource: %v", err)
	}
	src := &pppv1.Source{Type: pppv1.Source_HTTP, Urls: []string{bad.URL, good.URL}}
	data, err := s.FetchPiece(context.Background(), src, "t1", "f", 0, int64(len(content)), 8)
	if err != nil {
		t.Fatalf("FetchPiece: %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("piece = %q, want abcdef", data)
	}
}

// TestSourceNotImplemented verifies OSS/S3 return a clear error for phase 2.
func TestSourceNotImplemented(t *testing.T) {
	for _, typ := range []pppv1.Source_Type{pppv1.Source_OSS, pppv1.Source_S3} {
		if _, err := NewSource(typ); err == nil || !strings.Contains(err.Error(), "not implemented") {
			t.Fatalf("NewSource(%v) = %v, want not-implemented error", typ, err)
		}
	}
}
