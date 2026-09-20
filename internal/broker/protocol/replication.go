package protocol

import "fmt"

// Wire payloads for REPLICATE (v1.2, node-to-node). The request is JSON
// (small, control-ish); the response is binary because it carries the
// record batch — the same reason FETCH is binary.
//
// REPLICATE request payload (JSON):
//
//	{"from","topic","partition","offset","max_records","max_bytes"}
//
// offset is the follower's next offset: "give me everything from here".
//
// REPLICATE response payload (binary):
//
//	[u8 status][u64 first_offset][u64 high_watermark][u64 committed][u32 record_count]
//	record := same layout as the FETCH response record
//
// first_offset/high_watermark/committed describe the LEADER's log.
// committed is the stable high-water mark: records below it are on a
// quorum and will never be rolled back.

// ReplicateRequest is the decoded REPLICATE payload.
type ReplicateRequest struct {
	From       string `json:"from"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     uint64 `json:"offset"`
	MaxRecords uint32 `json:"max_records"`
	MaxBytes   uint32 `json:"max_bytes"`
}

// Replicate response statuses (the u8 header).
const (
	// ReplicateOK: records (possibly empty) follow.
	ReplicateOK byte = 0
	// ReplicateNotLeader: this node does not lead the partition. The
	// follower stops its loop and re-resolves the assignment.
	ReplicateNotLeader byte = 1
	// ReplicateTruncate: the follower's log diverged from the leader's
	// (it holds records the leader does not, or it sits below the
	// leader's first offset). The follower must truncate its log to
	// high_watermark (cut tail) or jump to first_offset (forward hole),
	// then fetch again.
	ReplicateTruncate byte = 2
	// ReplicateTopicUnknown: the leader does not know the topic (yet).
	// The follower backs off; topic sync reconciles.
	ReplicateTopicUnknown byte = 3
)

// ReplicateResponse is the decoded REPLICATE response.
type ReplicateResponse struct {
	Status byte
	// FirstOffset is the leader's low-water mark (oldest retained
	// record). A follower below it must jump forward.
	FirstOffset uint64
	// HighWatermark is the leader's log end (next offset to assign).
	HighWatermark uint64
	// Committed is the leader's committed offset (stable HWM).
	Committed uint64
	Records   []FetchedMessage
}

// EncodeReplicateResponse serializes the binary REPLICATE response.
func EncodeReplicateResponse(resp *ReplicateResponse) []byte {
	w := &writer{}
	w.buf = append(w.buf, resp.Status)
	w.u64(resp.FirstOffset)
	w.u64(resp.HighWatermark)
	w.u64(resp.Committed)
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

// DecodeReplicateResponse parses the binary REPLICATE response.
func DecodeReplicateResponse(payload []byte) (*ReplicateResponse, error) {
	r := &reader{buf: payload}
	resp := &ReplicateResponse{}
	status := r.take(1)
	if status == nil {
		return nil, r.err
	}
	resp.Status = status[0]
	resp.FirstOffset = r.u64()
	resp.HighWatermark = r.u64()
	resp.Committed = r.u64()
	n := r.u32()
	if r.err != nil {
		return nil, r.err
	}
	if n > 1<<20 {
		return nil, errReplicateTooLarge(n)
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

func errReplicateTooLarge(n uint32) error {
	return fmt.Errorf("protocol: replicate response too large: %d records", n)
}
