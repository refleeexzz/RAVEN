package protocol

import "encoding/json"

// Control operations (everything except PRODUCE/FETCH) use JSON payloads.
// Fields are kept small and stable; unknown fields are ignored so v1
// clients keep working when v2 adds fields.

// marshalJSON is a thin wrapper so call sites stay one-liners.
func marshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		// Types in this file are all marshalable; this is a bug guard.
		return []byte(`{"code":"INTERNAL","message":"marshal failed"}`)
	}
	return b
}

func unmarshalJSON(payload []byte, v any) error {
	return json.Unmarshal(payload, v)
}

// ---- CREATE_TOPIC ----

// CreateTopicRequest asks for a topic. Partitions <= 0 means "use the
// broker default" (3).
type CreateTopicRequest struct {
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}

type CreateTopicResponse struct {
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}

// ---- LIST_TOPICS ----

type ListTopicsRequest struct{}

// TopicInfo describes one topic for ops and clients.
type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// PartitionInfo reports the high-water mark (next offset to be written,
// i.e. one past the last record).
type PartitionInfo struct {
	ID            int32  `json:"id"`
	HighWatermark uint64 `json:"high_watermark"`
}

type ListTopicsResponse struct {
	Topics []TopicInfo `json:"topics"`
}

// ---- COMMIT_OFFSET ----

// CommitOffsetRequest records that group has consumed up to Offset-1;
// Offset is the NEXT record the group will read. Generation must match
// the group's current generation or the broker answers REBALANCE.
type CommitOffsetRequest struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     uint64 `json:"offset"`
	Generation int32  `json:"generation"`
}

type CommitOffsetResponse struct{}

// ---- FETCH_OFFSET ----

// FetchOffsetRequest reads the committed offset for a group. MemberID
// and Generation are required (BRKR-03): the member must be joined to
// the group and in the current generation, otherwise any client could
// read any group's offsets just by naming the group id.
type FetchOffsetRequest struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Generation int32  `json:"generation"`
}

// FetchOffsetResponse returns the next offset to consume (0 when the
// group never committed).
type FetchOffsetResponse struct {
	Offset uint64 `json:"offset"`
}

// ---- JOIN_GROUP ----

type JoinGroupRequest struct {
	Group    string   `json:"group"`
	MemberID string   `json:"member_id"`
	Topics   []string `json:"topics"`
}

// Assignment lists the partitions of one topic a member owns.
type Assignment struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

type JoinGroupResponse struct {
	Generation  int32        `json:"generation"`
	MemberID    string       `json:"member_id"`
	Assignments []Assignment `json:"assignments"`
}

// ---- LEAVE_GROUP ----

type LeaveGroupRequest struct {
	Group    string `json:"group"`
	MemberID string `json:"member_id"`
}

type LeaveGroupResponse struct{}

// ---- HEARTBEAT ----

type HeartbeatRequest struct {
	Group      string `json:"group"`
	MemberID   string `json:"member_id"`
	Generation int32  `json:"generation"`
}

type HeartbeatResponse struct{}

// ---- AUTH (v1.1) ----

// AuthRequest authenticates the connection against the broker's
// configured API keys. It must be the first frame on an auth-enabled
// connection: every other opcode is rejected with UNAUTHENTICATED
// until AUTH succeeds. The secret travels as plaintext inside the
// payload — pair auth with TLS (BROKER_TLS_*) on any real network; the
// broker only ever stores and compares its SHA-256, in constant time.
type AuthRequest struct {
	ID     string `json:"id"`
	Secret string `json:"secret"`
}

// AuthResponse confirms authentication and echoes the granted ACL, so
// clients can fail fast locally instead of discovering denials one
// request at a time.
type AuthResponse struct {
	ID          string   `json:"id"`
	TopicsRead  []string `json:"topics_read"`
	TopicsWrite []string `json:"topics_write"`
	Admin       bool     `json:"admin"`
}
