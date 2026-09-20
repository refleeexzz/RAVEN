package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/cluster"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/storage"
)

// testBrokerNode is one broker of an in-process cluster.
type testBrokerNode struct {
	cfg    Config
	b      *Broker
	cancel context.CancelFunc
	done   chan error
}

// stop gracefully shuts the node down (idempotent).
func (nd *testBrokerNode) stop() {
	if nd.cancel == nil {
		return
	}
	nd.cancel()
	<-nd.done
	nd.cancel = nil
}

// restart boots the node again with the same config (same cluster id
// and data dir): a process restart, not a new node.
func (nd *testBrokerNode) restart(t *testing.T) {
	t.Helper()
	if nd.cancel != nil {
		t.Fatal("restart of a running node")
	}
	b, err := New(nd.cfg, nil, nil)
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	nd.b = b
	nd.cancel = cancel
	nd.done = make(chan error, 1)
	go func() { nd.done <- b.Run(ctx) }()
	waitFor(t, 5*time.Second, func() bool { return b.Addr() != "" })
}

// startBrokerCluster boots n brokers in cluster mode with fast
// timeouts. Cluster listener ports are discovered by pre-binding (the
// same trick the config tests use); client listeners use :0.
func startBrokerCluster(t *testing.T, n int) []*testBrokerNode {
	t.Helper()
	_, addrs, cleanup := clusterEnv(t, n)
	cleanup() // release discovery listeners; brokers bind them below
	nodesJSON := "["
	for i, a := range addrs {
		if i > 0 {
			nodesJSON += ","
		}
		nodesJSON += fmt.Sprintf(`{"id":"n%d","addr":%q}`, i+1, a)
	}
	nodesJSON += "]"
	nodes := make([]*testBrokerNode, n)
	for i := 0; i < n; i++ {
		clusterCfg, on, err := cluster.ParseEnv(fmt.Sprintf("n%d", i+1), nodesJSON)
		if err != nil {
			t.Fatal(err)
		}
		// Fast cluster timings for tests.
		clusterCfg.HeartbeatEvery = 50 * time.Millisecond
		clusterCfg.SuspectAfter = 300 * time.Millisecond
		clusterCfg.DeadAfter = 900 * time.Millisecond
		clusterCfg.FetchInterval = 10 * time.Millisecond
		clusterCfg.ElectionTimeoutMin = 300 * time.Millisecond
		clusterCfg.ElectionTimeoutMax = 600 * time.Millisecond
		clusterCfg.QuorumAckTimeout = 1200 * time.Millisecond
		clusterCfg.LeaderAbdicateAfter = 2 * time.Second
		cfg := Config{
			TCPAddr:        "127.0.0.1:0",
			DataDir:        t.TempDir(),
			Cluster:        clusterCfg,
			ClusterEnabled: on,
			FsyncEvery:     time.Hour, // tests flush explicitly
		}
		b, err := New(cfg, nil, nil)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		nd := &testBrokerNode{cfg: cfg, b: b, cancel: cancel, done: make(chan error, 1)}
		go func() { nd.done <- b.Run(ctx) }()
		nodes[i] = nd
	}
	// Wait for every client listener to come up.
	for _, nd := range nodes {
		waitFor(t, 5*time.Second, func() bool { return nd.b.Addr() != "" })
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.stop()
		}
	})
	return nodes
}

// waitFor polls pred until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within", d)
}

// leaderOf returns the node leading topic/partition (all nodes compute
// the same assignment).
func leaderOf(t *testing.T, nodes []*testBrokerNode, topic string, partition int32) *testBrokerNode {
	t.Helper()
	a := nodes[0].b.cluster.AssignmentFor(topic, partition)
	for _, nd := range nodes {
		if nd.b.cluster.Self() == a.Leader {
			return nd
		}
	}
	t.Fatalf("no node leads %s/%d (assignment says %s)", topic, partition, a.Leader)
	return nil
}

// followersOf returns the non-leader replicas of the partition.
func followersOf(nodes []*testBrokerNode, topic string, partition int32) []*testBrokerNode {
	a := nodes[0].b.cluster.AssignmentFor(topic, partition)
	var out []*testBrokerNode
	for _, nd := range nodes {
		if nd.b.cluster.Self() == a.Leader {
			continue
		}
		for _, r := range a.Replicas {
			if nd.b.cluster.Self() == r {
				out = append(out, nd)
			}
		}
	}
	return out
}

// produceOne appends one record to a specific partition through the
// leader's backend (the same path a wire PRODUCE takes).
func produceOne(t *testing.T, nd *testBrokerNode, topic string, partition int32, value string) uint64 {
	t.Helper()
	resp, err := nd.b.Produce(context.Background(), &protocol.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Records:   []protocol.Message{{Value: []byte(value)}},
	})
	if err != nil {
		t.Fatalf("produce %s/%d: %v", topic, partition, err)
	}
	return resp.Results[0].Offset
}

// fetchAll reads everything a node exposes for the partition (capped at
// committed in cluster mode).
func fetchAll(t *testing.T, nd *testBrokerNode, topic string, partition int32) []protocol.FetchedMessage {
	t.Helper()
	resp, err := nd.b.Fetch(context.Background(), &protocol.FetchRequest{
		Topic: topic, Partition: partition, Offset: 0, MaxRecords: 10000, MaxBytes: 3 << 20,
	})
	if err != nil {
		t.Fatalf("fetch %s/%d on %s: %v", topic, partition, nd.b.cluster.Self(), err)
	}
	return resp.Records
}

func TestReplicationCatchUp(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "repl", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "repl", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	// Topic sync must materialize the topic on every replica.
	for _, nd := range nodes {
		nd := nd
		waitFor(t, 5*time.Second, func() bool {
			_, err := nd.b.store.Topic("repl")
			return err == nil
		})
	}
	for i := 0; i < 50; i++ {
		produceOne(t, leader, "repl", 0, fmt.Sprintf("v-%03d", i))
	}
	// Every follower converges to the leader's log and exposes it
	// (committed == hwm with the static leader).
	for _, f := range followersOf(nodes, "repl", 0) {
		f := f
		waitFor(t, 5*time.Second, func() bool {
			return len(fetchAll(t, f, "repl", 0)) == 50
		})
		recs := fetchAll(t, f, "repl", 0)
		for i, r := range recs {
			if r.Offset != uint64(i) || string(r.Value) != fmt.Sprintf("v-%03d", i) {
				t.Fatalf("follower %s rec %d: offset=%d value=%q",
					f.b.cluster.Self(), i, r.Offset, r.Value)
			}
		}
	}
}

func TestReplicationFollowerFetchCappedUntilCatchUp(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "cap", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "cap", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	followers := followersOf(nodes, "cap", 0)
	if len(followers) == 0 {
		t.Fatal("want at least one follower")
	}
	f := followers[0]
	// The follower learns the topic through topic sync; before that a
	// fetch is TOPIC_NOT_FOUND, after it the committed cap applies.
	waitFor(t, 5*time.Second, func() bool {
		_, err := f.b.store.Topic("cap")
		return err == nil
	})
	// Produce on the leader; the follower converges and exposes all.
	for i := 0; i < 10; i++ {
		produceOne(t, leader, "cap", 0, "x")
	}
	waitFor(t, 5*time.Second, func() bool {
		return len(fetchAll(t, f, "cap", 0)) == 10
	})
}

func TestReplicationDivergenceTruncates(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "div", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "div", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	f := followersOf(nodes, "div", 0)[0]
	waitFor(t, 5*time.Second, func() bool {
		_, err := f.b.store.Topic("div")
		return err == nil
	})
	for i := 0; i < 5; i++ {
		produceOne(t, leader, "div", 0, fmt.Sprintf("real-%d", i))
	}
	waitFor(t, 5*time.Second, func() bool { return len(fetchAll(t, f, "div", 0)) == 5 })

	// Inject a divergent, never-committed record straight into the
	// follower's log (what a crashed former leader would leave behind).
	topic, err := f.b.store.Topic("div")
	if err != nil {
		t.Fatal(err)
	}
	p := topic.Partitions[0]
	if err := p.AppendReplica([]storage.Record{{Offset: 5, TimestampMs: 1, Value: []byte("divergent")}}); err != nil {
		t.Fatal(err)
	}
	if hwm := p.HighWatermark(); hwm != 6 {
		t.Fatalf("setup: follower hwm %d want 6", hwm)
	}

	// Log matching must cut the divergent tail: the follower returns to
	// exactly the leader's log.
	waitFor(t, 5*time.Second, func() bool {
		return p.HighWatermark() == 5
	})
	recs := fetchAll(t, f, "div", 0)
	if len(recs) != 5 {
		t.Fatalf("follower exposes %d records after truncation, want 5", len(recs))
	}
	for i, r := range recs {
		if string(r.Value) != fmt.Sprintf("real-%d", i) {
			t.Fatalf("rec %d = %q, want real-%d", i, r.Value, i)
		}
	}
}

func TestProduceRejectedOnNonLeader(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "nl", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "nl", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	// Find a node that does not lead nl/0.
	var other *testBrokerNode
	for _, nd := range nodes {
		if nd != leader {
			other = nd
			break
		}
	}
	// Wait for topic sync on that node; until then the answer would be
	// TOPIC_NOT_FOUND, which is correct but not what we are asserting.
	waitFor(t, 5*time.Second, func() bool {
		_, err := other.b.store.Topic("nl")
		return err == nil
	})
	_, err := other.b.Produce(context.Background(), &protocol.ProduceRequest{
		Topic: "nl", Partition: 0, Records: []protocol.Message{{Value: []byte("x")}},
	})
	if !protocol.IsCode(err, protocol.CodeNotLeader) {
		t.Fatalf("got %v want NOT_LEADER", err)
	}
	// Standalone-mode check: the same call on the leader works.
	if _, err := leader.b.Produce(context.Background(), &protocol.ProduceRequest{
		Topic: "nl", Partition: 0, Records: []protocol.Message{{Value: []byte("x")}},
	}); err != nil {
		t.Fatalf("leader produce: %v", err)
	}
}

func TestClusterStatusOpsSurface(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "ops", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "ops", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 7; i++ {
		produceOne(t, leader, "ops", 0, "x")
	}
	// Ask the leader for its CLUSTER_STATUS through the cluster wire:
	// replicas, ISR and per-follower progress must all be there, and
	// eventually every replica reports in-sync.
	waitFor(t, 5*time.Second, func() bool {
		payload, err := nodes[0].b.cluster.CallPeer(context.Background(),
			cluster.NodeID(string(leader.b.cluster.Self())), protocol.OpClusterStatus, []byte("{}"))
		if err != nil {
			return false
		}
		var st clusterStatusResponse
		if err := json.Unmarshal(payload, &st); err != nil {
			return false
		}
		if len(st.Members) != 3 || len(st.Topics) != 1 {
			return false
		}
		for _, ps := range st.Partitions {
			if ps.Topic != "ops" || ps.Leader != leader.b.cluster.Self() {
				return false
			}
			if ps.Topic == "ops" && ps.Partition == 0 {
				if ps.HWM != 7 || ps.Committed != 7 {
					return false
				}
				if len(ps.ISR) != 3 || len(ps.Followers) != 2 {
					return false
				}
				for _, fp := range ps.Followers {
					if !fp.InISR || fp.Offset != 7 || fp.Lag != 0 {
						return false
					}
				}
				return true
			}
		}
		return false
	})
}
