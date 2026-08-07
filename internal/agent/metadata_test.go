package agent

import (
	"bytes"
	"crypto/sha256"
	"strings"
	"testing"
)

func testDigest(b byte) []byte {
	d := make([]byte, MetadataDigestSize)
	for i := range d {
		d[i] = b
	}
	return d
}

// TestMetadataRoundtrip verifies Build -> Encode -> Decode preserves every
// field, and that ID() is the SHA-256 of the canonical bytes.
func TestMetadataRoundtrip(t *testing.T) {
	pieces := [][]byte{testDigest(1), testDigest(2), testDigest(3)}
	fileDigest := testDigest(9)
	m, err := BuildMetadata("file.bin", 3*PieceSize, PieceSize, pieces, fileDigest)
	if err != nil {
		t.Fatalf("BuildMetadata: %v", err)
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	id, err := m.ID()
	if err != nil {
		t.Fatalf("ID: %v", err)
	}
	wantID := sha256.Sum256(data)
	if !bytes.Equal(id, wantID[:]) {
		t.Fatal("ID != SHA-256(canonical bytes)")
	}

	decoded, err := DecodeMetadata(data)
	if err != nil {
		t.Fatalf("DecodeMetadata: %v", err)
	}
	if decoded.Filename != "file.bin" || decoded.FileSize != 3*PieceSize ||
		decoded.PieceSize != PieceSize || decoded.PieceCount != 3 ||
		decoded.DigestAlgo != DigestAlgorithmSHA256 {
		t.Fatalf("decoded fields mismatch: %+v", decoded)
	}
	if !bytes.Equal(decoded.FileDigest, fileDigest) {
		t.Fatal("file digest mismatch")
	}
	for i, want := range pieces {
		got, err := decoded.PieceDigest(i)
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("piece digest %d mismatch (err=%v)", i, err)
		}
	}
	// Re-encoding the decoded metadata yields the same bytes.
	data2, err := decoded.Encode()
	if err != nil || !bytes.Equal(data2, data) {
		t.Fatal("re-encode of decoded metadata differs")
	}
}

// TestMetadataDeterminism locks the deterministic-encoding invariant: the same
// input twice must produce identical bytes and therefore the same metadata_id.
func TestMetadataDeterminism(t *testing.T) {
	pieces := [][]byte{testDigest(7), testDigest(8)}
	m1, err := BuildMetadata("a.bin", 2*PieceSize, PieceSize, pieces, testDigest(1))
	if err != nil {
		t.Fatalf("BuildMetadata: %v", err)
	}
	m2, err := BuildMetadata("a.bin", 2*PieceSize, PieceSize, pieces, testDigest(1))
	if err != nil {
		t.Fatalf("BuildMetadata: %v", err)
	}
	b1, _ := m1.Encode()
	b2, _ := m2.Encode()
	if !bytes.Equal(b1, b2) {
		t.Fatal("identical inputs produced different bytes")
	}
	id1, _ := m1.ID()
	id2, _ := m2.ID()
	if !bytes.Equal(id1, id2) {
		t.Fatal("identical inputs produced different metadata_id")
	}
}

// TestMetadataSizeHelper verifies the piece_count × 32 size calculation.
func TestMetadataSizeHelper(t *testing.T) {
	if MetadataDigestArraySize(0) != 0 {
		t.Fatalf("MetadataDigestArraySize(0) = %d, want 0", MetadataDigestArraySize(0))
	}
	if MetadataDigestArraySize(25600) != 25600*32 {
		t.Fatalf("MetadataDigestArraySize(25600) = %d, want %d", MetadataDigestArraySize(25600), 25600*32)
	}
	// The full size must equal the encoded length.
	m, _ := BuildMetadata("file.bin", 4*PieceSize, PieceSize,
		[][]byte{testDigest(1), testDigest(2), testDigest(3), testDigest(4)}, testDigest(9))
	enc, _ := m.Encode()
	if MetadataSizeFor("file.bin", DigestAlgorithmSHA256, 4) != int64(len(enc)) {
		t.Fatalf("MetadataSizeFor = %d, want %d", MetadataSizeFor("file.bin", DigestAlgorithmSHA256, 4), len(enc))
	}
}

// TestMetadataInvalid verifies malformed encodings and inconsistent metadata
// are rejected.
func TestMetadataInvalid(t *testing.T) {
	// Validate rejects size/piece inconsistencies and bad digest lengths.
	if _, err := BuildMetadata("", 100, PieceSize, [][]byte{testDigest(1)}, testDigest(9)); err == nil {
		t.Fatal("empty filename accepted")
	}
	if _, err := BuildMetadata("a.bin", 0, PieceSize, [][]byte{testDigest(1)}, testDigest(9)); err == nil {
		t.Fatal("size 0 with pieces accepted")
	}
	if _, err := BuildMetadata("a.bin", 100, 0, [][]byte{testDigest(1)}, testDigest(9)); err == nil {
		t.Fatal("piece size 0 accepted")
	}
	if _, err := BuildMetadata("a.bin", 100, PieceSize, [][]byte{{1, 2}}, testDigest(9)); err == nil {
		t.Fatal("short piece digest accepted")
	}
	if _, err := BuildMetadata("a.bin", 100, PieceSize, [][]byte{testDigest(1)}, []byte("short")); err == nil {
		t.Fatal("short file digest accepted")
	}

	// Decode rejects malformed bytes.
	bad := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"truncated header", []byte("PP")},
		{"bad magic", append([]byte("XXXX"), metadataMagic[:]...)},
		{"trailing bytes", append(mustEncodedMetadata(t), 0)},
	}
	for _, b := range bad {
		if _, err := DecodeMetadata(b.data); err == nil {
			t.Fatalf("DecodeMetadata(%s) = nil error, want rejection", b.name)
		}
	}
}

func mustEncodedMetadata(t *testing.T) []byte {
	t.Helper()
	m, err := BuildMetadata("a.bin", PieceSize, PieceSize, [][]byte{testDigest(1)}, testDigest(9))
	if err != nil {
		t.Fatalf("BuildMetadata: %v", err)
	}
	data, err := m.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return data
}

// TestMetadataDecodeRejectsHugePieceCount locks the P1 OOM/DoS fix: a header
// claiming an enormous piece_count must be rejected immediately, before any
// large allocation, with a clear error.
func TestMetadataDecodeRejectsHugePieceCount(t *testing.T) {
	header := func(pieceCount uint32) []byte {
		// minimal valid header: magic + version + empty filename + sizes +
		// empty algorithm + 32-byte file digest
		data := make([]byte, 0, 64)
		data = append(data, metadataMagic[:]...)
		data = append(data, MetadataMagicVersion)
		data = append(data, 0, 0)                         // filename length 0
		data = append(data, 0, 0, 0, 0, 0, 0, 0, 100)     // file_size 100 (i64 BE)
		data = append(data, 0, 0, 0, 0, 0x00, 0x10, 0, 0) // piece_size 1MiB
		data = append(data, byte(pieceCount>>24), byte(pieceCount>>16), byte(pieceCount>>8), byte(pieceCount))
		data = append(data, 0, 0)                                // algorithm length 0
		data = append(data, make([]byte, MetadataDigestSize)...) // file digest
		return data
	}
	for _, n := range []uint32{1_000_000, ^uint32(0)} {
		data := header(n)
		_, err := DecodeMetadata(data)
		if err == nil {
			t.Fatalf("DecodeMetadata(piece_count=%d) = nil, want rejection", n)
		}
		if !strings.Contains(err.Error(), "exceeds data length") {
			t.Fatalf("DecodeMetadata(piece_count=%d) err = %v, want 'exceeds data length'", n, err)
		}
	}
}
