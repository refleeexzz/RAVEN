package cluster

import (
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// Wire payloads for the cluster opcodes. Control messages are JSON
// (they are small and infrequent); the replication data path uses the
// compact binary layout in replication.go. Unknown JSON fields are
// ignored, so v1.2 nodes tolerate fields added later.

// ---- NODE_PING ----

// NodePingRequest is the membership heartbeat. Every node pings every
// peer on the HeartbeatEvery cadence; any valid inbound frame also
// marks the sender as seen, so the ping response doubles as the
// peer's proof of life for us.
type NodePingRequest struct {
	ID NodeID `json:"id"`
	// Leaving announces a graceful shutdown: the receiver marks the
	// sender LEAVING instead of waiting for the DEAD timeout.
	Leaving bool `json:"leaving,omitempty"`
}

// NodePingResponse proves the responder is alive and clustered.
type NodePingResponse struct {
	ID NodeID `json:"id"`
}

// errFrame builds an OpError response frame with a stable code.
func errFrame(correlationID uint64, code, msg string) *protocol.Frame {
	return protocol.ErrorFrame(correlationID, protocol.NewError(code, msg))
}
