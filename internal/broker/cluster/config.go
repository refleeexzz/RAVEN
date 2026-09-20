// Package cluster turns the single-node broker into a fault-tolerant
// cluster: static membership with failure detection, partition
// replication with a committed offset (high-water mark), raft-style
// per-partition leader election and partition reassignment.
//
// Cluster mode is strictly opt-in. With BROKER_NODE_ID and
// BROKER_CLUSTER_NODES both empty the broker keeps the exact
// single-node behavior it always had; nothing in this package runs.
package cluster

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// NodeID identifies one broker process in the cluster. It is stable
// across restarts (the data dir belongs to the id, not to the address).
type NodeID string

// NodeInfo is one statically configured cluster member.
type NodeInfo struct {
	ID NodeID `json:"id"`
	// Addr is the node's cluster listener (node-to-node traffic only),
	// host:port. Client traffic stays on the main TCP listener.
	Addr string `json:"addr"`
}

// Config holds every cluster knob. Values come from env vars through
// ParseEnv plus explicit overrides; withDefaults fills the rest.
type Config struct {
	// NodeID is this node's identity (env BROKER_NODE_ID). Must be one
	// of the ids in Nodes.
	NodeID NodeID
	// Nodes is the full static membership (env BROKER_CLUSTER_NODES,
	// JSON [{"id","addr"}]). v1 is static: every node boots with the
	// same list.
	Nodes []NodeInfo

	// ReplicationFactor is how many replicas each partition gets,
	// capped at len(Nodes) (env BROKER_REPLICATION_FACTOR, default
	// min(3, len(Nodes))).
	ReplicationFactor int

	// HeartbeatEvery is the node-to-node ping cadence (env
	// BROKER_CLUSTER_HEARTBEAT_MS, default 500ms).
	HeartbeatEvery time.Duration
	// SuspectAfter marks a silent peer SUSPECT (env
	// BROKER_CLUSTER_SUSPECT_MS, default 3s).
	SuspectAfter time.Duration
	// DeadAfter marks a silent peer DEAD (env BROKER_CLUSTER_DEAD_MS,
	// default 10s). DEAD leaders lose their partitions to elections.
	DeadAfter time.Duration

	// FetchInterval is how often a follower polls its leader when it is
	// fully caught up (env BROKER_CLUSTER_FETCH_MS, default 200ms).
	FetchInterval time.Duration
	// FetchMaxBytes caps one replicate response payload (default 2MiB;
	// must stay under the 4MiB frame cap with room for framing).
	FetchMaxBytes int

	// ReplicaLagMax is how far (in offsets) a follower may trail the
	// leader before it leaves the ISR (env BROKER_CLUSTER_ISR_LAG_MAX,
	// default 1000).
	ReplicaLagMax uint64

	// ElectionTimeoutMin/Max bound the randomized per-partition
	// election timeout (env BROKER_CLUSTER_ELECTION_MS, default
	// 1500–3000ms; Max = 2*Min when unset).
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// QuorumAckTimeout bounds how long an acks=all produce waits for
	// the ISR quorum before answering NOT_ENOUGH_REPLICAS (env
	// BROKER_CLUSTER_QUORUM_ACK_MS, default 5000ms).
	QuorumAckTimeout time.Duration

	// LeaderAbdicateAfter is how long a leader may go without reaching
	// a quorum before it voluntarily steps down (env
	// BROKER_CLUSTER_ABDICATE_MS, default 10s). This is what makes an
	// isolated leader stop accepting writes during a network partition.
	LeaderAbdicateAfter time.Duration
}

// ParseEnv builds a cluster Config from the raw BROKER_NODE_ID /
// BROKER_CLUSTER_NODES values. enabled is false only when BOTH are
// empty — the standalone default. A half-filled configuration is a
// hard error (fail closed, same policy as BROKER_API_KEYS): booting a
// node that thinks it is clustered but has no peers, or peers without
// an identity, would silently corrupt placement decisions.
func ParseEnv(nodeID, nodesJSON string) (cfg Config, enabled bool, err error) {
	if nodeID == "" && nodesJSON == "" {
		return Config{}, false, nil
	}
	if nodeID == "" {
		return Config{}, false, errors.New("BROKER_NODE_ID is required when BROKER_CLUSTER_NODES is set")
	}
	if nodesJSON == "" {
		return Config{}, false, errors.New("BROKER_CLUSTER_NODES is required when BROKER_NODE_ID is set")
	}
	var nodes []NodeInfo
	if err := json.Unmarshal([]byte(nodesJSON), &nodes); err != nil {
		return Config{}, false, fmt.Errorf("BROKER_CLUSTER_NODES is not valid JSON: %w", err)
	}
	if len(nodes) == 0 {
		return Config{}, false, errors.New("BROKER_CLUSTER_NODES must list at least one node")
	}
	seen := make(map[NodeID]struct{}, len(nodes))
	selfFound := false
	for i, n := range nodes {
		if n.ID == "" {
			return Config{}, false, fmt.Errorf("BROKER_CLUSTER_NODES[%d]: empty id", i)
		}
		if n.Addr == "" {
			return Config{}, false, fmt.Errorf("BROKER_CLUSTER_NODES[%d] (%s): empty addr", i, n.ID)
		}
		if _, dup := seen[n.ID]; dup {
			return Config{}, false, fmt.Errorf("BROKER_CLUSTER_NODES: duplicate node id %q", n.ID)
		}
		seen[n.ID] = struct{}{}
		if n.ID == NodeID(nodeID) {
			selfFound = true
		}
	}
	if !selfFound {
		return Config{}, false, fmt.Errorf("BROKER_NODE_ID %q is not present in BROKER_CLUSTER_NODES", nodeID)
	}
	cfg = Config{NodeID: NodeID(nodeID), Nodes: nodes}.withDefaults()
	return cfg, true, nil
}

// withDefaults fills zero fields so tests can build partial configs.
func (c Config) withDefaults() Config {
	if c.ReplicationFactor <= 0 {
		c.ReplicationFactor = 3
	}
	if c.ReplicationFactor > len(c.Nodes) {
		c.ReplicationFactor = len(c.Nodes)
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = 500 * time.Millisecond
	}
	if c.SuspectAfter <= 0 {
		c.SuspectAfter = 3 * time.Second
	}
	if c.DeadAfter <= 0 {
		c.DeadAfter = 10 * time.Second
	}
	if c.FetchInterval <= 0 {
		c.FetchInterval = 200 * time.Millisecond
	}
	if c.FetchMaxBytes <= 0 {
		c.FetchMaxBytes = 2 << 20
	}
	if c.ReplicaLagMax <= 0 {
		c.ReplicaLagMax = 1000
	}
	if c.ElectionTimeoutMin <= 0 {
		c.ElectionTimeoutMin = 1500 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.QuorumAckTimeout <= 0 {
		c.QuorumAckTimeout = 5 * time.Second
	}
	if c.LeaderAbdicateAfter <= 0 {
		c.LeaderAbdicateAfter = 10 * time.Second
	}
	return c
}

// Self returns this node's own entry.
func (c Config) Self() NodeInfo {
	for _, n := range c.Nodes {
		if n.ID == c.NodeID {
			return n
		}
	}
	return NodeInfo{ID: c.NodeID}
}

// Peers returns every node except this one, in configuration order.
func (c Config) Peers() []NodeInfo {
	out := make([]NodeInfo, 0, len(c.Nodes)-1)
	for _, n := range c.Nodes {
		if n.ID != c.NodeID {
			out = append(out, n)
		}
	}
	return out
}

// sortedIDs returns every node id in stable sorted order. Deterministic
// placement depends on every node agreeing on this order.
func (c Config) sortedIDs() []NodeID {
	ids := make([]NodeID, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		ids = append(ids, n.ID)
	}
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	return ids
}
