package agent

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// FileMetadataV1 is the fixed, deterministic metadata format of the
// file-distribution core (design §3.2). Content identity is SHA-256: the
// metadata_id of an artifact is SHA-256 over its canonical bytes, piece
// digests are the expected value of each received piece, and the file digest
// is the whole-file check before publication. CRC64 remains only a fast
// transport checksum (PieceInfo.hash is untouched).
//
// Encoding choice — fixed binary layout (not proto deterministic marshal):
//   - deterministic by construction: fixed field order + length-prefixed
//     strings + raw 32-byte digests; nothing map-ordered, timestamped or
//     node/Job-ID-derived can leak in, so identical fields always produce
//     identical bytes and therefore the same metadata_id;
//   - matches the design's "固定版本、确定性编码的二进制格式";
//   - the digest array is exactly piece_count×32 raw SHA-256 bytes (§3.3 size
//     math: 25,600 pieces × 32 = 800 KiB).
//
// Layout (big-endian):
//
//	0        4 bytes  magic "PPPM"
//	4        1 byte   version (1)
//	5        2 bytes  filename length
//	7        filename bytes
//	+        8 bytes  file_size (int64)
//	+        8 bytes  piece_size (int64)
//	+        4 bytes  piece_count (uint32)
//	+        2 bytes  digest_algorithm length
//	+        digest_algorithm bytes
//	+        32 bytes file_digest (SHA-256)
//	+        piece_count × 32 bytes piece_digests
const (
	// MetadataMagicVersion is the FileMetadataV1 format version.
	MetadataMagicVersion = 1
	// MetadataDigestSize is the size of one SHA-256 digest in bytes.
	MetadataDigestSize = 32
	// DigestAlgorithmSHA256 is the digest algorithm used by FileMetadataV1.
	DigestAlgorithmSHA256 = "SHA-256"
)

var metadataMagic = [4]byte{'P', 'P', 'P', 'M'}

// FileMetadataV1 describes one artifact's content identity.
type FileMetadataV1 struct {
	Filename     string
	FileSize     int64
	PieceSize    int64
	PieceCount   int
	DigestAlgo   string
	FileDigest   []byte   // MetadataDigestSize bytes
	PieceDigests [][]byte // PieceCount × MetadataDigestSize bytes
}

// MetadataDigestArraySize returns the size of the piece digest array for a
// piece count (§3.3): piece_count × 32 bytes.
func MetadataDigestArraySize(pieceCount int) int64 {
	return int64(pieceCount) * MetadataDigestSize
}

// MetadataSizeFor returns the encoded metadata size for the given filename,
// digest algorithm and piece count.
func MetadataSizeFor(filename, algo string, pieceCount int) int64 {
	if algo == "" {
		algo = DigestAlgorithmSHA256
	}
	header := 4 + 1 + 2 + len(filename) + 8 + 8 + 4 + 2 + len(algo) + MetadataDigestSize
	return int64(header) + MetadataDigestArraySize(pieceCount)
}

// BuildMetadata creates a FileMetadataV1 (digest algorithm SHA-256).
func BuildMetadata(filename string, size, pieceSize int64, pieceDigests [][]byte, fileDigest []byte) (*FileMetadataV1, error) {
	m := &FileMetadataV1{
		Filename:     string(filename),
		FileSize:     size,
		PieceSize:    pieceSize,
		PieceCount:   len(pieceDigests),
		DigestAlgo:   DigestAlgorithmSHA256,
		FileDigest:   fileDigest,
		PieceDigests: pieceDigests,
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// Validate checks the size/piece consistency and digest lengths.
func (m *FileMetadataV1) Validate() error {
	if m == nil {
		return errors.New("metadata: nil")
	}
	if m.Filename == "" {
		return errors.New("metadata: filename is required")
	}
	if m.FileSize < 0 {
		return errors.New("metadata: file_size must be non-negative")
	}
	if m.PieceSize <= 0 {
		return errors.New("metadata: piece_size must be positive")
	}
	if m.PieceCount == 0 {
		if m.FileSize != 0 {
			return errors.New("metadata: piece_count is 0 but file_size is non-zero")
		}
	} else if int64(m.PieceCount) != (m.FileSize+m.PieceSize-1)/m.PieceSize {
		return fmt.Errorf("metadata: piece_count %d != ceil(file_size/piece_size)=%d", m.PieceCount, (m.FileSize+m.PieceSize-1)/m.PieceSize)
	}
	if m.DigestAlgo != "" && m.DigestAlgo != DigestAlgorithmSHA256 {
		return fmt.Errorf("metadata: unsupported digest algorithm %q", m.DigestAlgo)
	}
	if len(m.FileDigest) != MetadataDigestSize {
		return errors.New("metadata: file_digest must be 32 bytes")
	}
	for i, d := range m.PieceDigests {
		if len(d) != MetadataDigestSize {
			return fmt.Errorf("metadata: piece digest %d must be 32 bytes", i)
		}
	}
	return nil
}

// Encode returns the canonical bytes of the metadata.
func (m *FileMetadataV1) Encode() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	algo := m.DigestAlgo
	if algo == "" {
		algo = DigestAlgorithmSHA256
	}
	if len(m.Filename) > 65535 || len(algo) > 65535 {
		return nil, errors.New("metadata: filename or digest algorithm too long")
	}
	buf := make([]byte, 0, MetadataSizeFor(m.Filename, algo, m.PieceCount))
	buf = append(buf, metadataMagic[:]...)
	buf = append(buf, MetadataMagicVersion)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(m.Filename)))
	buf = append(buf, m.Filename...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(m.FileSize))
	buf = binary.BigEndian.AppendUint64(buf, uint64(m.PieceSize))
	buf = binary.BigEndian.AppendUint32(buf, uint32(m.PieceCount))
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(algo)))
	buf = append(buf, algo...)
	buf = append(buf, m.FileDigest...)
	for _, d := range m.PieceDigests {
		buf = append(buf, d...)
	}
	return buf, nil
}

// ID returns the metadata_id: SHA-256 over the canonical bytes.
func (m *FileMetadataV1) ID() ([]byte, error) {
	data, err := m.Encode()
	if err != nil {
		return nil, err
	}
	return MetadataID(data), nil
}

// MetadataID computes the metadata_id of canonical metadata bytes.
func MetadataID(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

// PieceDigest returns the expected digest of a piece index.
func (m *FileMetadataV1) PieceDigest(index int) ([]byte, error) {
	if index < 0 || index >= len(m.PieceDigests) {
		return nil, fmt.Errorf("metadata: piece %d out of range", index)
	}
	return m.PieceDigests[index], nil
}

// DecodeMetadata parses canonical bytes into a FileMetadataV1, rejecting
// malformed input.
func DecodeMetadata(data []byte) (*FileMetadataV1, error) {
	r := &byteReader{data: data}
	if err := r.take(len(metadataMagic)); err != nil {
		return nil, errors.New("metadata: truncated header")
	}
	if string(r.data[0:len(metadataMagic)]) != string(metadataMagic[:]) {
		return nil, errors.New("metadata: bad magic")
	}
	r.pos = len(metadataMagic)
	version, err := r.u8()
	if err != nil {
		return nil, err
	}
	if version != MetadataMagicVersion {
		return nil, fmt.Errorf("metadata: unsupported version %d", version)
	}
	nameLen, err := r.u16()
	if err != nil {
		return nil, err
	}
	filename, err := r.bytes(int(nameLen))
	if err != nil {
		return nil, errors.New("metadata: truncated filename")
	}
	fileSize, err := r.i64()
	if err != nil {
		return nil, errors.New("metadata: truncated file_size")
	}
	pieceSize, err := r.i64()
	if err != nil {
		return nil, errors.New("metadata: truncated piece_size")
	}
	pieceCount, err := r.u32()
	if err != nil {
		return nil, errors.New("metadata: truncated piece_count")
	}
	algoLen, err := r.u16()
	if err != nil {
		return nil, err
	}
	algo, err := r.bytes(int(algoLen))
	if err != nil {
		return nil, errors.New("metadata: truncated digest algorithm")
	}
	fileDigest, err := r.bytes(MetadataDigestSize)
	if err != nil {
		return nil, errors.New("metadata: truncated file_digest")
	}
	pieceDigests := make([][]byte, 0, pieceCount)
	for i := uint32(0); i < pieceCount; i++ {
		d, err := r.bytes(MetadataDigestSize)
		if err != nil {
			return nil, errors.New("metadata: truncated piece_digests")
		}
		pieceDigests = append(pieceDigests, d)
	}
	if r.pos != len(r.data) {
		return nil, errors.New("metadata: trailing bytes")
	}
	m := &FileMetadataV1{
		Filename:     string(filename),
		FileSize:     fileSize,
		PieceSize:    pieceSize,
		PieceCount:   int(pieceCount),
		DigestAlgo:   string(algo),
		FileDigest:   fileDigest,
		PieceDigests: pieceDigests,
	}
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return m, nil
}

// byteReader is a minimal big-endian reader for the fixed layout.
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) take(n int) error {
	if r.pos+n > len(r.data) {
		return errors.New("metadata: short read")
	}
	return nil
}

func (r *byteReader) u8() (byte, error) {
	if err := r.take(1); err != nil {
		return 0, err
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

func (r *byteReader) u16() (uint16, error) {
	if err := r.take(2); err != nil {
		return 0, errors.New("metadata: short read")
	}
	v := binary.BigEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *byteReader) u32() (uint32, error) {
	if err := r.take(4); err != nil {
		return 0, errors.New("metadata: short read")
	}
	v := binary.BigEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

func (r *byteReader) i64() (int64, error) {
	if err := r.take(8); err != nil {
		return 0, errors.New("metadata: short read")
	}
	v := int64(binary.BigEndian.Uint64(r.data[r.pos:]))
	r.pos += 8
	return v, nil
}

func (r *byteReader) bytes(n int) ([]byte, error) {
	if err := r.take(n); err != nil {
		return nil, errors.New("metadata: short read")
	}
	v := r.data[r.pos : r.pos+n]
	r.pos += n
	return v, nil
}
