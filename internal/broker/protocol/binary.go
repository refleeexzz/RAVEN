package protocol

import (
	"encoding/binary"
	"fmt"
)

// Binary payload layouts for the hot path (PRODUCE / FETCH). Everything
// is big-endian. Length-prefixed byte fields use u32 lengths; short
// strings (topic, group, member, header keys/values) use u16 lengths.
//
// PRODUCE request payload:
//
//	[u16 topic_len][topic][i32 partition][u32 record_count]
//	record := [u32 key_len][key][u32 val_len][value][u16 header_count][header...]
//	header := [u16 key_len][key][u16 val_len][value]
//
// partition -1 asks the broker to pick: hash(key) when a key is present,
// round-robin otherwise.
//
// PRODUCE response payload:
//
//	[u32 count]  then per record: [i32 partition][u64 offset]
//
// One entry per input record, in input order, because a batch can span
// partitions (key hashing).
//
// FETCH request payload:
//
//	[u16 topic_len][topic][i32 partition][u64 offset][u32 max_records][u32 max_bytes]
//	[u16 group_len][group][u16 member_len][member][i32 generation]
//
// group may be empty (group_len 0): a plain reader without a consumer
// group. With a group, generation must be current or the broker answers
// REBALANCE so the consumer re-joins.
//
// FETCH response payload:
//
//	[u64 high_watermark][u32 record_count]
//	record := [u64 offset][i64 timestamp_ms][u32 key_len][key][u32 val_len][value][u16 header_count][header...]

// Header is a record header: small key/value metadata.
type Header struct {
	Key   string
	Value []byte
}

// Message is a record as produced by a client (no offset yet).
type Message struct {
	Key     []byte
	Value   []byte
	Headers []Header
}

// FetchedMessage is a record as returned by FETCH: offset and
// timestamp are assigned by the broker.
type FetchedMessage struct {
	Offset    uint64
	Timestamp int64 // unix milliseconds
	Key       []byte
	Value     []byte
	Headers   []Header
}

// ProduceRequest is the decoded PRODUCE payload. Partition -1 means the
// broker chooses per record.
type ProduceRequest struct {
	Topic     string
	Partition int32
	Records   []Message
}

// ProduceResult is the assigned partition and offset of one record.
type ProduceResult struct {
	Partition int32
	Offset    uint64
}

type ProduceResponse struct {
	Results []ProduceResult
}

// FetchRequest is the decoded FETCH payload.
type FetchRequest struct {
	Topic      string
	Partition  int32
	Offset     uint64
	MaxRecords uint32
	MaxBytes   uint32
	Group      string
	MemberID   string
	Generation int32
}

type FetchResponse struct {
	HighWatermark uint64
	Records       []FetchedMessage
}

// ---- encode/decode helpers ----

// writer appends big-endian fields to a byte slice.
type writer struct{ buf []byte }

func (w *writer) u16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *writer) u64(v uint64) { w.buf = binary.BigEndian.AppendUint64(w.buf, v) }
func (w *writer) i32(v int32)  { w.u32(uint32(v)) }
func (w *writer) i64(v int64)  { w.u64(uint64(v)) }
func (w *writer) raw(b []byte) { w.buf = append(w.buf, b...) }

func (w *writer) shortString(s string) {
	w.u16(uint16(len(s)))
	w.raw([]byte(s))
}

func (w *writer) longBytes(b []byte) {
	w.u32(uint32(len(b)))
	w.raw(b)
}

// reader is a bounds-checked cursor. Errors are sticky: once a read
// fails, every later read returns zero values and the original error
// stays in err.
type reader struct {
	buf []byte
	off int
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || len(r.buf)-r.off < n {
		r.err = fmt.Errorf("protocol: truncated payload (need %d bytes, have %d)", n, len(r.buf)-r.off)
		return nil
	}
	b := r.buf[r.off : r.off+n : r.off+n]
	r.off += n
	return b
}

func (r *reader) u16() uint16 {
	b := r.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}

func (r *reader) u32() uint32 {
	b := r.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *reader) u64() uint64 {
	b := r.take(8)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (r *reader) i32() int32 { return int32(r.u32()) }
func (r *reader) i64() int64 { return int64(r.u64()) }

func (r *reader) shortString() string {
	n := int(r.u16())
	return string(r.take(n))
}

func (r *reader) longBytes() []byte {
	n := int64(r.u32())
	if r.err != nil {
		return nil
	}
	if n > int64(len(r.buf)-r.off) {
		r.err = fmt.Errorf("protocol: field length %d exceeds remaining %d", n, len(r.buf)-r.off)
		return nil
	}
	return r.take(int(n))
}

func (r *reader) headers() []Header {
	if r.err != nil {
		return nil
	}
	n := int(r.u16())
	if n == 0 {
		return nil
	}
	hs := make([]Header, 0, n)
	for i := 0; i < n; i++ {
		k := r.take(int(r.u16()))
		v := r.take(int(r.u16()))
		if r.err != nil {
			return nil
		}
		hs = append(hs, Header{Key: string(k), Value: append([]byte(nil), v...)})
	}
	return hs
}

func writeHeaders(w *writer, hs []Header) {
	w.u16(uint16(len(hs)))
	for _, h := range hs {
		w.u16(uint16(len(h.Key)))
		w.raw([]byte(h.Key))
		w.u16(uint16(len(h.Value)))
		w.raw(h.Value)
	}
}

// ---- PRODUCE ----

func EncodeProduceRequest(req *ProduceRequest) []byte {
	w := &writer{}
	w.shortString(req.Topic)
	w.i32(req.Partition)
	w.u32(uint32(len(req.Records)))
	for i := range req.Records {
		rec := &req.Records[i]
		w.longBytes(rec.Key)
		w.longBytes(rec.Value)
		writeHeaders(w, rec.Headers)
	}
	return w.buf
}

func DecodeProduceRequest(payload []byte) (*ProduceRequest, error) {
	r := &reader{buf: payload}
	req := &ProduceRequest{}
	req.Topic = r.shortString()
	req.Partition = r.i32()
	n := r.u32()
	if r.err != nil {
		return nil, r.err
	}
	if n > 1<<20 {
		return nil, fmt.Errorf("protocol: produce batch too large: %d records", n)
	}
	req.Records = make([]Message, 0, min(int(n), 1024))
	for i := uint32(0); i < n; i++ {
		var m Message
		m.Key = append([]byte(nil), r.longBytes()...)
		m.Value = append([]byte(nil), r.longBytes()...)
		m.Headers = r.headers()
		if r.err != nil {
			return nil, r.err
		}
		req.Records = append(req.Records, m)
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("protocol: empty topic in produce request")
	}
	return req, nil
}

func EncodeProduceResponse(resp *ProduceResponse) []byte {
	w := &writer{}
	w.u32(uint32(len(resp.Results)))
	for _, res := range resp.Results {
		w.i32(res.Partition)
		w.u64(res.Offset)
	}
	return w.buf
}

func DecodeProduceResponse(payload []byte) (*ProduceResponse, error) {
	r := &reader{buf: payload}
	n := r.u32()
	if r.err != nil {
		return nil, r.err
	}
	if n > 1<<20 {
		return nil, fmt.Errorf("protocol: produce response too large: %d results", n)
	}
	resp := &ProduceResponse{Results: make([]ProduceResult, 0, min(int(n), 1024))}
	for i := uint32(0); i < n; i++ {
		res := ProduceResult{Partition: r.i32(), Offset: r.u64()}
		if r.err != nil {
			return nil, r.err
		}
		resp.Results = append(resp.Results, res)
	}
	return resp, nil
}

// ---- FETCH ----

func EncodeFetchRequest(req *FetchRequest) []byte {
	w := &writer{}
	w.shortString(req.Topic)
	w.i32(req.Partition)
	w.u64(req.Offset)
	w.u32(req.MaxRecords)
	w.u32(req.MaxBytes)
	w.shortString(req.Group)
	w.shortString(req.MemberID)
	w.i32(req.Generation)
	return w.buf
}

func DecodeFetchRequest(payload []byte) (*FetchRequest, error) {
	r := &reader{buf: payload}
	req := &FetchRequest{}
	req.Topic = r.shortString()
	req.Partition = r.i32()
	req.Offset = r.u64()
	req.MaxRecords = r.u32()
	req.MaxBytes = r.u32()
	req.Group = r.shortString()
	req.MemberID = r.shortString()
	req.Generation = r.i32()
	if r.err != nil {
		return nil, r.err
	}
	if req.Topic == "" {
		return nil, fmt.Errorf("protocol: empty topic in fetch request")
	}
	return req, nil
}

func EncodeFetchResponse(resp *FetchResponse) []byte {
	w := &writer{}
	w.u64(resp.HighWatermark)
	w.u32(uint32(len(resp.Records)))
	for i := range resp.Records {
		rec := &resp.Records[i]
		w.u64(rec.Offset)
		w.i64(rec.Timestamp)
		w.longBytes(rec.Key)
		w.longBytes(rec.Value)
		writeHeaders(w, rec.Headers)
	}
	return w.buf
}

func DecodeFetchResponse(payload []byte) (*FetchResponse, error) {
	r := &reader{buf: payload}
	resp := &FetchResponse{}
	resp.HighWatermark = r.u64()
	n := r.u32()
	if r.err != nil {
		return nil, r.err
	}
	if n > 1<<20 {
		return nil, fmt.Errorf("protocol: fetch response too large: %d records", n)
	}
	resp.Records = make([]FetchedMessage, 0, min(int(n), 1024))
	for i := uint32(0); i < n; i++ {
		var m FetchedMessage
		m.Offset = r.u64()
		m.Timestamp = r.i64()
		m.Key = append([]byte(nil), r.longBytes()...)
		m.Value = append([]byte(nil), r.longBytes()...)
		m.Headers = r.headers()
		if r.err != nil {
			return nil, r.err
		}
		resp.Records = append(resp.Records, m)
	}
	return resp, nil
}
