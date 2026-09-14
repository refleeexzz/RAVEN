package server_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/server"
)

// aclKeys maps key ids to (secret, grants) for the matrix.
func aclKeys() staticAuth {
	return staticAuth{
		"admin":   {"s", &server.Principal{ID: "admin", Admin: true}},
		"worker":  {"s", &server.Principal{ID: "worker", TopicsRead: []string{"jobs"}, TopicsWrite: []string{"jobs"}}},
		"reader":  {"s", &server.Principal{ID: "reader", TopicsRead: []string{"jobs"}}},
		"writer":  {"s", &server.Principal{ID: "writer", TopicsWrite: []string{"jobs"}}},
		"wild":    {"s", &server.Principal{ID: "wild", TopicsRead: []string{"*"}, TopicsWrite: []string{"*"}}},
		"prefix":  {"s", &server.Principal{ID: "prefix", TopicsRead: []string{"jobs.*"}}},
		"nogrant": {"s", &server.Principal{ID: "nogrant"}},
	}
}

// opReq builds one request frame per opcode for the matrix.
func opReq(op protocol.Opcode, corr uint64) *protocol.Frame {
	j := func(v any) []byte { b, _ := json.Marshal(v); return b }
	switch op {
	case protocol.OpCreateTopic:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: j(protocol.CreateTopicRequest{Topic: "x", Partitions: 1})}
	case protocol.OpListTopics:
		return &protocol.Frame{Opcode: op, CorrelationID: corr}
	case protocol.OpProduce:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: protocol.EncodeProduceRequest(&protocol.ProduceRequest{
			Topic: "jobs", Partition: -1, Records: []protocol.Message{{Value: []byte("v")}}})}
	case protocol.OpFetch:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: protocol.EncodeFetchRequest(&protocol.FetchRequest{
			Topic: "jobs", Partition: 0})}
	case protocol.OpCommitOffset:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: j(protocol.CommitOffsetRequest{
			Group: "g", MemberID: "m", Topic: "jobs", Partition: 0, Offset: 1, Generation: 1})}
	case protocol.OpFetchOffset:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: j(protocol.FetchOffsetRequest{
			Group: "g", MemberID: "m", Topic: "jobs", Partition: 0, Generation: 1})}
	case protocol.OpJoinGroup:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: j(protocol.JoinGroupRequest{
			Group: "g", MemberID: "m", Topics: []string{"jobs"}})}
	case protocol.OpLeaveGroup:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: j(protocol.LeaveGroupRequest{Group: "g", MemberID: "m"})}
	case protocol.OpHeartbeat:
		return &protocol.Frame{Opcode: op, CorrelationID: corr, Payload: j(protocol.HeartbeatRequest{Group: "g", MemberID: "m", Generation: 1})}
	default:
		panic("unknown op in matrix")
	}
}

// TestACLMatrix runs the full principal x operation matrix against a
// live server: every cell is either served (no UNAUTHORIZED) or denied
// with exactly UNAUTHORIZED.
func TestACLMatrix(t *testing.T) {
	t.Parallel()
	var deniedCount int
	s := startServerOpts(t, nil,
		server.WithAuthenticator(aclKeys()),
		server.WithSecurityHooks(server.SecurityHooks{OnACLDenied: func() { deniedCount++ }}),
	)

	allOps := []protocol.Opcode{
		protocol.OpCreateTopic, protocol.OpListTopics, protocol.OpProduce, protocol.OpFetch,
		protocol.OpCommitOffset, protocol.OpFetchOffset, protocol.OpJoinGroup,
		protocol.OpLeaveGroup, protocol.OpHeartbeat,
	}
	allow := func(ops ...protocol.Opcode) map[protocol.Opcode]bool {
		m := make(map[protocol.Opcode]bool, len(ops))
		for _, op := range ops {
			m[op] = true
		}
		return m
	}
	readOps := []protocol.Opcode{protocol.OpFetch, protocol.OpCommitOffset, protocol.OpFetchOffset,
		protocol.OpJoinGroup, protocol.OpLeaveGroup, protocol.OpHeartbeat}

	matrix := []struct {
		keyID   string
		allowed map[protocol.Opcode]bool
	}{
		{"admin", allow(allOps...)},
		{"worker", allow(append(readOps, protocol.OpProduce)...)},
		{"reader", allow(readOps...)},
		{"writer", allow(protocol.OpProduce, protocol.OpLeaveGroup, protocol.OpHeartbeat)},
		{"wild", allow(append(readOps, protocol.OpProduce)...)}, // "*" still not admin
		{"prefix", allow(protocol.OpLeaveGroup, protocol.OpHeartbeat)},
		{"nogrant", allow(protocol.OpLeaveGroup, protocol.OpHeartbeat)},
	}

	for _, row := range matrix {
		for _, op := range allOps {
			op := op
			t.Run(row.keyID+"/"+op.String(), func(t *testing.T) {
				conn := dial(t, s)
				defer conn.Close()
				mustAuth(t, conn, row.keyID, "s")
				req := opReq(op, 7)
				if err := protocol.WriteFrame(conn, req); err != nil {
					t.Fatalf("write: %v", err)
				}
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				f, err := protocol.ReadFrame(conn)
				if err != nil {
					t.Fatalf("read: %v", err)
				}
				wantAllowed := row.allowed[op]
				if wantAllowed {
					if f.Opcode == protocol.OpError {
						e := protocol.DecodeErrorFrame(f.Payload)
						t.Fatalf("%s on %s: denied (%s), want allowed", row.keyID, op, e.Code)
					}
					if f.Opcode != op {
						t.Fatalf("%s on %s: response opcode %s", row.keyID, op, f.Opcode)
					}
					return
				}
				if f.Opcode != protocol.OpError {
					t.Fatalf("%s on %s: allowed, want UNAUTHORIZED", row.keyID, op)
				}
				if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnauthorized {
					t.Fatalf("%s on %s: code %q, want UNAUTHORIZED", row.keyID, op, e.Code)
				}
			})
		}
	}
	if deniedCount == 0 {
		t.Fatal("OnACLDenied never fired across the whole denial matrix")
	}
}

// TestACLPrefixWildcard pins the "jobs.*" prefix semantics: it matches
// "jobs.p1" but not the bare "jobs" (the prefix includes the dot).
func TestACLPrefixWildcard(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithAuthenticator(aclKeys()))
	conn := dial(t, s)
	defer conn.Close()
	mustAuth(t, conn, "prefix", "s")

	fetch := func(topic string, corr uint64) *protocol.Frame {
		req := &protocol.Frame{Opcode: protocol.OpFetch, CorrelationID: corr,
			Payload: protocol.EncodeFetchRequest(&protocol.FetchRequest{Topic: topic, Partition: 0})}
		if err := protocol.WriteFrame(conn, req); err != nil {
			t.Fatalf("write: %v", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		f, err := protocol.ReadFrame(conn)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		return f
	}

	if f := fetch("jobs.p1", 1); f.Opcode != protocol.OpFetch {
		t.Fatalf("fetch jobs.p1: got %s, want allowed", f.Opcode)
	}
	f := fetch("jobs", 2)
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnauthorized {
		t.Fatalf("fetch jobs (bare): code %q, want UNAUTHORIZED (prefix is \"jobs.\" + suffix)", e.Code)
	}
}

// TestACLJoinGroupMultiTopic: joining is a subscription to every
// listed topic, so one denied topic sinks the whole join.
func TestACLJoinGroupMultiTopic(t *testing.T) {
	t.Parallel()
	s := startServerOpts(t, nil, server.WithAuthenticator(aclKeys()))
	conn := dial(t, s)
	defer conn.Close()
	mustAuth(t, conn, "reader", "s") // read: jobs only

	payload, _ := json.Marshal(protocol.JoinGroupRequest{Group: "g", MemberID: "m", Topics: []string{"jobs", "secret"}})
	if err := protocol.WriteFrame(conn, &protocol.Frame{Opcode: protocol.OpJoinGroup, CorrelationID: 1, Payload: payload}); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	f, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if e := protocol.DecodeErrorFrame(f.Payload); e.Code != protocol.CodeUnauthorized {
		t.Fatalf("join [jobs secret]: code %q, want UNAUTHORIZED", e.Code)
	}
}
