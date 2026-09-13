// Package protocol defines the RAVEN broker wire protocol (v1).
//
// Every message on the wire is a frame:
//
//	[u32 payload_len][u8 opcode][u64 correlation_id][payload]
//
// All integers are big-endian. payload_len counts ONLY the payload bytes
// (not the 13-byte header). A payload larger than MaxPayloadSize is
// rejected before reading, so a bad client can never make us allocate
// huge buffers.
//
// Control operations (create topic, list topics, group management,
// offsets) carry a JSON payload. PRODUCE and FETCH carry a compact
// binary payload (see binary.go) because they are the hot path and JSON
// + base64 would waste both CPU and bandwidth on every message.
//
// Request/response rule: every request gets exactly one response frame
// with the same correlation id and the same opcode. On failure the
// response has opcode OpError and a JSON body {"code","message"}.
// Pipelining is allowed: a client may have many in-flight requests on
// one connection; responses may come back out of order and are matched
// by correlation id.
package protocol

import "fmt"

// MaxPayloadSize is the hard cap for one frame payload (4 MiB).
// The full frame on the wire is MaxPayloadSize + headerSize.
const MaxPayloadSize = 4 << 20

// headerSize: u32 payload_len + u8 opcode + u64 correlation id.
const headerSize = 4 + 1 + 8

// Opcode identifies the operation a frame carries.
type Opcode uint8

// Opcodes, v1. OpError is only used for responses.
const (
	OpError        Opcode = 0x00
	OpCreateTopic  Opcode = 0x01
	OpListTopics   Opcode = 0x02
	OpProduce      Opcode = 0x03
	OpFetch        Opcode = 0x04
	OpCommitOffset Opcode = 0x05
	OpFetchOffset  Opcode = 0x06
	OpJoinGroup    Opcode = 0x07
	OpLeaveGroup   Opcode = 0x08
	OpHeartbeat    Opcode = 0x09
)

func (o Opcode) String() string {
	switch o {
	case OpError:
		return "ERROR"
	case OpCreateTopic:
		return "CREATE_TOPIC"
	case OpListTopics:
		return "LIST_TOPICS"
	case OpProduce:
		return "PRODUCE"
	case OpFetch:
		return "FETCH"
	case OpCommitOffset:
		return "COMMIT_OFFSET"
	case OpFetchOffset:
		return "FETCH_OFFSET"
	case OpJoinGroup:
		return "JOIN_GROUP"
	case OpLeaveGroup:
		return "LEAVE_GROUP"
	case OpHeartbeat:
		return "HEARTBEAT"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", uint8(o))
	}
}

// Stable machine error codes sent inside OpError frames.
const (
	CodeBadRequest       = "BAD_REQUEST"
	CodeUnknownOpcode    = "UNKNOWN_OPCODE"
	CodeTopicExists      = "TOPIC_EXISTS"
	CodeTopicNotFound    = "TOPIC_NOT_FOUND"
	CodeBrokerBusy       = "BROKER_BUSY"
	CodeRebalance        = "REBALANCE"
	CodeUnknownMember    = "UNKNOWN_MEMBER"
	CodeOffsetOutOfRange = "OFFSET_OUT_OF_RANGE"
	CodeInternal         = "INTERNAL"
)

// Error is a broker-side failure that maps 1:1 onto an OpError frame.
// Handlers return *Error for expected failures (busy, rebalance, ...);
// anything else is reported as CodeInternal.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// NewError builds a wire-level error with a stable code.
func NewError(code, message string) *Error {
	return &Error{Code: code, Message: message}
}

// ErrorCode extracts the stable code from err, defaulting to
// CodeInternal for plain errors.
func ErrorCode(err error) string {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return CodeInternal
}
