package broker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/cluster"
	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// produceAcks appends one record with an explicit acks mode through the
// leader's backend (the exact path a wire PRODUCE takes).
func produceAcks(t *testing.T, nd *testBrokerNode, topic string, partition int32, acks byte, value string) (uint64, error) {
	t.Helper()
	resp, err := nd.b.Produce(context.Background(), &protocol.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Records:   []protocol.Message{{Value: []byte(value)}},
		Acks:      acks,
	})
	if err != nil {
		return 0, err
	}
	return resp.Results[0].Offset, nil
}

// waitCommittedAtLeast polls until the node's committed offset for the
// partition reaches off.
func waitCommittedAtLeast(t *testing.T, nd *testBrokerNode, topic string, partition int32, off uint64) {
	t.Helper()
	waitFor(t, 10*time.Second, func() bool {
		return nd.b.repl.committedFor(topic, partition) >= off
	})
}

func TestProduceAcksAllCommitsWithQuorum(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "qa", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "qa", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	waitISRSize(t, leader, "qa", 0, 3)
	// acks=all returns only after a majority confirmed. With all three
	// replicas up that must simply succeed, and the record must be
	// committed (visible) on every replica afterwards.
	off, err := produceAcks(t, leader, "qa", 0, protocol.AcksAll, "quorum-write")
	if err != nil {
		t.Fatalf("acks=all: %v", err)
	}
	waitCommittedAtLeast(t, leader, "qa", 0, off+1)
	for _, nd := range nodes {
		nd := nd
		waitFor(t, 5*time.Second, func() bool {
			return len(fetchAll(t, nd, "qa", 0)) == 1
		})
	}
}

func TestProduceAcksAllToleratesOneReplicaDown(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "qt", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "qt", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	waitISRSize(t, leader, "qt", 0, 3)
	followers := followersOf(nodes, "qt", 0)
	if len(followers) != 2 {
		t.Fatalf("want 2 followers, got %d", len(followers))
	}
	// Kill one replica and wait until the leader declares it DEAD: the
	// ISR shrinks to {leader, survivor} and the quorum recomputes.
	followers[0].stop()
	deadID := followers[0].b.cluster.Self()
	// A graceful stop announces LEAVING; a crash would go DEAD. Either
	// way the replica stops counting as alive and leaves the ISR.
	waitFor(t, 5*time.Second, func() bool {
		return !leader.b.cluster.MemberState(deadID).Alive()
	})
	// acks=all still works with 2 of 3 replicas.
	off, err := produceAcks(t, leader, "qt", 0, protocol.AcksAll, "after-kill")
	if err != nil {
		t.Fatalf("acks=all with one replica down: %v", err)
	}
	waitCommittedAtLeast(t, leader, "qt", 0, off+1)
	// The dead replica is out of the ISR on the ops surface.
	if inISR(leader, "qt", 0, deadID) {
		t.Fatal("dead replica still in ISR")
	}
}

func TestProduceAcksAllMinorityCannotCommit(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "qm", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "qm", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	waitISRSize(t, leader, "qm", 0, 3)
	followers := followersOf(nodes, "qm", 0)
	if _, err := produceAcks(t, leader, "qm", 0, protocol.AcksAll, "warm"); err != nil {
		t.Fatal(err)
	}
	committedBefore := leader.b.repl.committedFor("qm", 0)
	// Kill BOTH followers: the leader is a minority of one. Committed
	// must freeze; acks=all must fail.
	followers[0].stop()
	followers[1].stop()
	waitFor(t, 5*time.Second, func() bool {
		return !leader.b.cluster.MemberState(followers[0].b.cluster.Self()).Alive() &&
			!leader.b.cluster.MemberState(followers[1].b.cluster.Self()).Alive()
	})
	_, err := produceAcks(t, leader, "qm", 0, protocol.AcksAll, "minority-write")
	if !protocol.IsCode(err, protocol.CodeNotEnoughReplicas) {
		t.Fatalf("got %v want NOT_ENOUGH_REPLICAS", err)
	}
	if c := leader.b.repl.committedFor("qm", 0); c != committedBefore {
		t.Fatalf("committed moved without quorum: %d -> %d", committedBefore, c)
	}
	// The timed-out record sits uncommitted on the leader: acks=1
	// accepts it (leader WAL), but consumers must NOT see it.
	if _, err := produceAcks(t, leader, "qm", 0, protocol.AcksLeader, "uncommitted"); err != nil {
		t.Fatalf("acks=1 on isolated leader: %v", err)
	}
	if got := len(fetchAll(t, leader, "qm", 0)); got != int(committedBefore) {
		t.Fatalf("fetch exposes %d records, want %d (uncommitted tail must stay hidden)",
			got, committedBefore)
	}
	// Healing: both replicas return, catch up, and the pending tail
	// commits without any new produce.
	followers[0].restart(t)
	followers[1].restart(t)
	waitFor(t, 10*time.Second, func() bool {
		_, err := followers[0].b.store.Topic("qm")
		return err == nil
	})
	waitCommittedAtLeast(t, leader, "qm", 0, committedBefore+2)
	if got := len(fetchAll(t, leader, "qm", 0)); got != int(committedBefore)+2 {
		t.Fatalf("after healing fetch exposes %d, want %d", got, committedBefore+2)
	}
}

func TestProduceAcksValidation(t *testing.T) {
	nodes := startBrokerCluster(t, 2)
	leader := leaderOf(t, nodes, "av", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "av", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	_, err := produceAcks(t, leader, "av", 0, 99, "bad")
	if !protocol.IsCode(err, protocol.CodeBadRequest) {
		t.Fatalf("got %v want BAD_REQUEST", err)
	}
	// acks=1 (explicit) and acks=0 (default) both work.
	if _, err := produceAcks(t, leader, "av", 0, protocol.AcksLeader, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := produceAcks(t, leader, "av", 0, 0, "default"); err != nil {
		t.Fatal(err)
	}
}

func TestAcksAllTimeoutIsNotDuplicated(t *testing.T) {
	nodes := startBrokerCluster(t, 3)
	leader := leaderOf(t, nodes, "dup", 0)
	if _, err := leader.b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "dup", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	waitISRSize(t, leader, "dup", 0, 3)
	followers := followersOf(nodes, "dup", 0)
	if _, err := produceAcks(t, leader, "dup", 0, protocol.AcksAll, "warm"); err != nil {
		t.Fatal(err)
	}
	followers[0].stop()
	followers[1].stop()
	waitFor(t, 5*time.Second, func() bool {
		return !leader.b.cluster.MemberState(followers[0].b.cluster.Self()).Alive() &&
			!leader.b.cluster.MemberState(followers[1].b.cluster.Self()).Alive()
	})
	// Two acks=all attempts for the "same" logical record while
	// isolated: both fail. After healing, a retried producer writes it
	// once and the log holds exactly one copy of each value.
	v1 := fmt.Sprintf("attempt-%d", 1)
	if _, err := produceAcks(t, leader, "dup", 0, protocol.AcksAll, v1); !protocol.IsCode(err, protocol.CodeNotEnoughReplicas) {
		t.Fatalf("attempt 1: got %v", err)
	}
	if _, err := produceAcks(t, leader, "dup", 0, protocol.AcksAll, v1+"-retry"); !protocol.IsCode(err, protocol.CodeNotEnoughReplicas) {
		t.Fatalf("attempt 2: got %v", err)
	}
	followers[0].restart(t)
	followers[1].restart(t)
	waitFor(t, 10*time.Second, func() bool {
		_, err := followers[0].b.store.Topic("dup")
		return err == nil
	})
	// Both attempts DID land on the leader's WAL and commit on healing:
	// exactly the Kafka acks=all contract (at-least-once for the
	// producer, no loss for the cluster).
	waitCommittedAtLeast(t, leader, "dup", 0, 3)
	recs := fetchAll(t, leader, "dup", 0)
	if len(recs) != 3 {
		t.Fatalf("want 3 records (warm + 2 retried), got %d", len(recs))
	}
}

// inISR reports whether id is in the partition's ISR on nd's view.
func inISR(nd *testBrokerNode, topic string, partition int32, id cluster.NodeID) bool {
	t, err := nd.b.store.Topic(topic)
	if err != nil {
		return false
	}
	a := nd.b.cluster.AssignmentFor(topic, partition)
	for _, r := range nd.b.repl.isr(a, t.Partitions[partition].HighWatermark()) {
		if r == id {
			return true
		}
	}
	return false
}

// waitISRSize waits until the partition's ISR on nd reaches size — i.e.
// the topic has synced, follower loops are running and first progress
// reports landed. acks=all tests must form the ISR before producing.
func waitISRSize(t *testing.T, nd *testBrokerNode, topic string, partition int32, size int) {
	t.Helper()
	waitFor(t, 10*time.Second, func() bool {
		topic_, err := nd.b.store.Topic(topic)
		if err != nil {
			return false
		}
		a := nd.b.cluster.AssignmentFor(topic, partition)
		return len(nd.b.repl.isr(a, topic_.Partitions[partition].HighWatermark())) == size
	})
}
