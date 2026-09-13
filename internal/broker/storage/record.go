// Package storage is the broker's durable log. It knows nothing about
// the wire protocol or consumer groups; it only appends and reads
// records in per-partition segment files.
//
// On-disk layout:
//
//	$DATA_DIR/topics/<topic>/partition-<n>/<base_offset>.log
//	$DATA_DIR/topics/<topic>/partition-<n>/<base_offset>.index
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Record layout on disk (all big-endian):
//
//	[u32 crc][u64 offset][i64 timestamp_ms][u32 key_len][key]
//	[u32 val_len][value][u16 headers_count][headers...]
//
//	header := [u16 key_len][key bytes][u16 val_len][value bytes]
//
// crc is CRC-32 (Castagnoli) over every byte AFTER the crc field.
// key_len/val_len of 0 mean an empty (not null) key/value.
// headers_count may be 0; header keys and values are capped at 64 KiB
// by the u16 length.
//
// The fixed part before the key is 24 bytes: 4 crc + 8 offset +
// 8 timestamp + 4 key_len.
const recordFixedSize = 4 + 8 + 8 + 4

// MaxHeaderLen is the u16 cap on header keys and values.
const MaxHeaderLen = 1<<16 - 1

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt means a record failed validation (CRC mismatch, absurd
// length, non-monotonic offset). ErrTruncated means the file ends in
// the middle of a record — the classic torn write after a crash.
// Recovery treats both the same way: truncate the segment at the last
// good byte.
var (
	ErrCorrupt   = errors.New("storage: corrupt record")
	ErrTruncated = errors.New("storage: truncated record")
)

// Header is record metadata as stored on disk.
type Header struct {
	Key   string
	Value []byte
}

// Record is one message in the log.
type Record struct {
	Offset      uint64
	TimestampMs int64
	Key         []byte
	Value       []byte
	Headers     []Header
}

// EncodeRecord appends the on-disk encoding of r to buf and returns the
// extended slice. Callers pass the batch buffer so a whole produce batch
// is written with one syscall.
func EncodeRecord(buf []byte, r *Record) ([]byte, error) {
	for _, h := range r.Headers {
		if len(h.Key) > MaxHeaderLen || len(h.Value) > MaxHeaderLen {
			return nil, fmt.Errorf("storage: header %q too large (max %d bytes)", h.Key, MaxHeaderLen)
		}
	}
	start := len(buf)
	buf = binary.BigEndian.AppendUint32(buf, 0) // crc placeholder
	buf = binary.BigEndian.AppendUint64(buf, r.Offset)
	buf = binary.BigEndian.AppendUint64(buf, uint64(r.TimestampMs))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Key)))
	buf = append(buf, r.Key...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(r.Value)))
	buf = append(buf, r.Value...)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(r.Headers)))
	for _, h := range r.Headers {
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.Key)))
		buf = append(buf, h.Key...)
		buf = binary.BigEndian.AppendUint16(buf, uint16(len(h.Value)))
		buf = append(buf, h.Value...)
	}
	crc := crc32.Checksum(buf[start+4:], castagnoli)
	binary.BigEndian.PutUint32(buf[start:], crc)
	return buf, nil
}

// DecodeRecord parses one record from the front of b. It returns the
// record and the number of bytes consumed. CRC is always verified.
func DecodeRecord(b []byte) (Record, int, error) {
	var r Record
	if len(b) < recordFixedSize {
		return r, 0, ErrTruncated
	}
	wantCRC := binary.BigEndian.Uint32(b[0:4])
	r.Offset = binary.BigEndian.Uint64(b[4:12])
	r.TimestampMs = int64(binary.BigEndian.Uint64(b[12:20]))
	keyLen := int(binary.BigEndian.Uint32(b[20:24]))
	pos := recordFixedSize
	if keyLen > len(b)-pos {
		return r, 0, ErrTruncated
	}
	r.Key = append([]byte(nil), b[pos:pos+keyLen]...)
	pos += keyLen

	if len(b)-pos < 4 {
		return r, 0, ErrTruncated
	}
	valLen := int(binary.BigEndian.Uint32(b[pos : pos+4]))
	pos += 4
	if valLen > len(b)-pos {
		return r, 0, ErrTruncated
	}
	r.Value = append([]byte(nil), b[pos:pos+valLen]...)
	pos += valLen

	if len(b)-pos < 2 {
		return r, 0, ErrTruncated
	}
	headerCount := int(binary.BigEndian.Uint16(b[pos : pos+2]))
	pos += 2
	for i := 0; i < headerCount; i++ {
		if len(b)-pos < 2 {
			return r, 0, ErrTruncated
		}
		kLen := int(binary.BigEndian.Uint16(b[pos : pos+2]))
		pos += 2
		if kLen > len(b)-pos {
			return r, 0, ErrTruncated
		}
		key := string(b[pos : pos+kLen])
		pos += kLen
		if len(b)-pos < 2 {
			return r, 0, ErrTruncated
		}
		vLen := int(binary.BigEndian.Uint16(b[pos : pos+2]))
		pos += 2
		if vLen > len(b)-pos {
			return r, 0, ErrTruncated
		}
		val := append([]byte(nil), b[pos:pos+vLen]...)
		pos += vLen
		r.Headers = append(r.Headers, Header{Key: key, Value: val})
	}

	// pos is now the full record size; verify CRC over everything after
	// the crc field.
	if crc32.Checksum(b[4:pos], castagnoli) != wantCRC {
		return r, 0, ErrCorrupt
	}
	return r, pos, nil
}
