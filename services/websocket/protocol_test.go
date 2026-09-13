package websocket

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDecodeInboundValid(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantOp   string
		wantRoom string
		wantTo   string
		wantData string
	}{
		{"join", `{"op":"join","room":"jobs"}`, opJoin, "jobs", "", ""},
		{"leave", `{"op":"leave","room":"jobs"}`, opLeave, "jobs", "", ""},
		{"msg", `{"op":"msg","room":"jobs","data":{"n":1}}`, opMsg, "jobs", "", `{"n":1}`},
		{"dm", `{"op":"dm","to":"alice","data":{"hi":true}}`, opDM, "", "alice", `{"hi":true}`},
		{"ping", `{"op":"ping"}`, opPing, "", "", ""},
		{"colons in room", `{"op":"join","room":"jobs:eu-1"}`, opJoin, "jobs:eu-1", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := decodeInbound([]byte(tt.raw))
			if err != nil {
				t.Fatalf("decodeInbound: %v", err)
			}
			if m.Op != tt.wantOp || m.Room != tt.wantRoom || m.To != tt.wantTo {
				t.Errorf("got op=%q room=%q to=%q", m.Op, m.Room, m.To)
			}
			if tt.wantData != "" && string(m.Data) != tt.wantData {
				t.Errorf("data = %s, want %s", m.Data, tt.wantData)
			}
		})
	}
}

func TestDecodeInboundInvalid(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"not json", `{"op":`},
		{"empty op", `{"room":"jobs"}`},
		{"unknown op", `{"op":"explode"}`},
		{"join missing room", `{"op":"join"}`},
		{"join bad room chars", `{"op":"join","room":"has space"}`},
		{"join room too long", `{"op":"join","room":"` + string(make([]byte, 200)) + `"}`},
		{"msg missing data", `{"op":"msg","room":"jobs"}`},
		{"dm missing recipient", `{"op":"dm","data":{}}`},
		{"dm missing data", `{"op":"dm","to":"alice"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := tt.raw
			if tt.name == "join room too long" {
				b := make([]byte, 200)
				for i := range b {
					b[i] = 'a'
				}
				raw = `{"op":"join","room":"` + string(b) + `"}`
			}
			if _, err := decodeInbound([]byte(raw)); err == nil {
				t.Fatalf("expected error for %s", raw)
			}
		})
	}
}

func TestFrameBuilders(t *testing.T) {
	at := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)

	t.Run("joined", func(t *testing.T) {
		b, err := joinedFrame("jobs")
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if m["op"] != "joined" || m["room"] != "jobs" {
			t.Fatalf("joined frame: %s", b)
		}
	})

	t.Run("msg passthrough data", func(t *testing.T) {
		b, err := msgFrame("jobs", "alice", json.RawMessage(`{"n":1}`), at)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if m["op"] != "msg" || m["room"] != "jobs" || m["from"] != "alice" {
			t.Fatalf("msg frame: %s", b)
		}
		data, ok := m["data"].(map[string]any)
		if !ok || data["n"] != float64(1) {
			t.Fatalf("msg data: %s", b)
		}
		if m["at"] != "2026-09-13T12:00:00Z" {
			t.Fatalf("msg at: %s", b)
		}
	})

	t.Run("dm has no room key", func(t *testing.T) {
		b, err := msgFrame("", "alice", json.RawMessage(`{}`), at)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if _, hasRoom := m["room"]; hasRoom {
			t.Fatalf("dm frame must omit room: %s", b)
		}
	})

	t.Run("presence online=false is emitted", func(t *testing.T) {
		b, err := presenceFrame("alice", false, at)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		online, present := m["online"]
		if !present {
			t.Fatalf("presence frame must contain online: %s", b)
		}
		if online != false || m["user"] != "alice" {
			t.Fatalf("presence frame: %s", b)
		}
	})

	t.Run("pong and error", func(t *testing.T) {
		var pong map[string]any
		if err := json.Unmarshal(pongFrame(), &pong); err != nil {
			t.Fatal(err)
		}
		if pong["op"] != "pong" {
			t.Fatalf("pong: %s", pongFrame())
		}
		var e map[string]any
		if err := json.Unmarshal(errorFrame("nope"), &e); err != nil {
			t.Fatal(err)
		}
		if e["op"] != "error" || e["message"] != "nope" {
			t.Fatalf("error: %v", e)
		}
	})

	t.Run("event", func(t *testing.T) {
		b, err := eventFrame("jobs", json.RawMessage(`{"type":"job_status"}`), at)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatal(err)
		}
		if m["op"] != "event" || m["room"] != "jobs" {
			t.Fatalf("event frame: %s", b)
		}
	})
}

func TestReservedRooms(t *testing.T) {
	if !reservedRoom("user:alice") {
		t.Error("user:alice should be reserved")
	}
	if reservedRoom("jobs") {
		t.Error("jobs should not be reserved")
	}
	if got := userRoom("alice"); got != "user:alice" {
		t.Errorf("userRoom = %q", got)
	}
}
