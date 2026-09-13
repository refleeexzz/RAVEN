// Package websocket implements RAVEN's realtime edge: WebSocket connections
// grouped into rooms, presence tracking and Redis-backed fanout so multiple
// replicas of the service behave as one logical node.
//
// Wire protocol (one JSON object per text frame):
//
//	client → server:
//	  {"op":"join","room":"jobs"}
//	  {"op":"leave","room":"jobs"}
//	  {"op":"msg","room":"jobs","data":{...}}
//	  {"op":"dm","to":"<userID>","data":{...}}
//	  {"op":"ping"}
//	server → client:
//	  {"op":"joined","room":"jobs"}
//	  {"op":"msg","room":"jobs","from":"<userID>","data":{...},"at":"..."}
//	  {"op":"msg","from":"<userID>","data":{...},"at":"..."}   (dm: no room)
//	  {"op":"event","room":"jobs","data":{...},"at":"..."}
//	  {"op":"presence","user":"<userID>","online":true,"at":"..."}
//	  {"op":"pong"}
//	  {"op":"error","message":"..."}
//
// Room names must match roomNameRE. Rooms prefixed with "user:" are
// reserved: every connection auto-joins user:<own id> at connect time (job
// events addressed at a user are delivered there) and clients may not join,
// leave or publish to them directly.
package websocket

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	ravenerrors "github.com/refleeexzz/RAVEN/pkg/errors"
)

// Client → server ops.
const (
	opJoin  = "join"
	opLeave = "leave"
	opMsg   = "msg"
	opDM    = "dm"
	opPing  = "ping"
)

// Server → client ops. opMsg is reused for server → client delivery.
const (
	opJoined   = "joined"
	opEvent    = "event"
	opPresence = "presence"
	opPong     = "pong"
	opError    = "error"
)

const userRoomPrefix = "user:"

// userRoom returns the personal room every connection auto-joins.
func userRoom(userID string) string { return userRoomPrefix + userID }

var roomNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:._-]{0,127}$`)

// validRoom reports whether s is a well-formed room name.
func validRoom(s string) bool { return roomNameRE.MatchString(s) }

// reservedRoom reports whether a room is server-managed and off-limits for
// direct client join/leave/msg operations.
func reservedRoom(s string) bool { return strings.HasPrefix(s, userRoomPrefix) }

// inbound is a client → server frame.
type inbound struct {
	Op   string          `json:"op"`
	Room string          `json:"room,omitempty"`
	To   string          `json:"to,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// decodeInbound parses and validates one client frame.
func decodeInbound(raw []byte) (inbound, error) {
	var m inbound
	if err := json.Unmarshal(raw, &m); err != nil {
		return inbound{}, ravenerrors.E(ravenerrors.KindInvalid, "bad_frame", "frame is not valid JSON", err)
	}
	switch m.Op {
	case opJoin, opLeave:
		if !validRoom(m.Room) {
			return inbound{}, errBadRoom()
		}
	case opMsg:
		if !validRoom(m.Room) {
			return inbound{}, errBadRoom()
		}
		if len(m.Data) == 0 {
			return inbound{}, ravenerrors.E(ravenerrors.KindInvalid, "bad_data", "msg requires a data payload", nil)
		}
	case opDM:
		if m.To == "" {
			return inbound{}, ravenerrors.E(ravenerrors.KindInvalid, "bad_recipient", "dm requires a recipient", nil)
		}
		if len(m.Data) == 0 {
			return inbound{}, ravenerrors.E(ravenerrors.KindInvalid, "bad_data", "dm requires a data payload", nil)
		}
	case opPing:
		// no fields required
	default:
		return inbound{}, ravenerrors.E(ravenerrors.KindInvalid, "unknown_op",
			fmt.Sprintf("unknown op %q", m.Op), nil)
	}
	return m, nil
}

func errBadRoom() error {
	return ravenerrors.E(ravenerrors.KindInvalid, "bad_room",
		"room must be 1-128 chars of [a-zA-Z0-9:._-] and start alphanumeric", nil)
}

// outbound is a server → client frame. Pointer fields keep omitempty honest:
// online=false and a zero At must be emitted when set, omitted when absent.
type outbound struct {
	Op      string          `json:"op"`
	Room    string          `json:"room,omitempty"`
	From    string          `json:"from,omitempty"`
	User    string          `json:"user,omitempty"`
	Online  *bool           `json:"online,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	Message string          `json:"message,omitempty"`
	At      *time.Time      `json:"at,omitempty"`
}

func marshalFrame(m outbound) ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, ravenerrors.E(ravenerrors.KindUnknown, "frame_marshal", "could not encode frame", err)
	}
	return b, nil
}

func joinedFrame(room string) ([]byte, error) {
	return marshalFrame(outbound{Op: opJoined, Room: room})
}

// msgFrame builds a room message; pass room="" for direct messages.
func msgFrame(room, from string, data json.RawMessage, at time.Time) ([]byte, error) {
	return marshalFrame(outbound{Op: opMsg, Room: room, From: from, Data: data, At: &at})
}

func eventFrame(room string, data json.RawMessage, at time.Time) ([]byte, error) {
	return marshalFrame(outbound{Op: opEvent, Room: room, Data: data, At: &at})
}

func presenceFrame(user string, online bool, at time.Time) ([]byte, error) {
	return marshalFrame(outbound{Op: opPresence, User: user, Online: &online, At: &at})
}

// pongFrame and errorFrame cannot fail to marshal (no RawMessage input),
// so they return the bare bytes.
func pongFrame() []byte {
	b, _ := marshalFrame(outbound{Op: opPong})
	return b
}

func errorFrame(message string) []byte {
	b, _ := marshalFrame(outbound{Op: opError, Message: message})
	return b
}
