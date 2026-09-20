package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"log/slog"
)

// testNode is one in-process cluster node on a real loopback listener.
type testNode struct {
	cfg    Config
	cl     *Cluster
	ln     net.Listener
	cancel context.CancelFunc
	done   chan struct{}
}

// stop gracefully shuts the node down (the leaving announce path).
func (nd *testNode) stop() {
	nd.cancel()
	<-nd.done
	nd.cl.Close()
}

// startTestCluster boots n nodes with fast timeouts and returns them in
// id order. t.Cleanup shuts everything down.
func startTestCluster(t *testing.T, n int) []*testNode {
	t.Helper()
	// Bind all listeners first so every node's config carries the real
	// addresses.
	lns := make([]net.Listener, n)
	infos := make([]NodeInfo, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		infos[i] = NodeInfo{ID: NodeID(fmt.Sprintf("n%d", i+1)), Addr: ln.Addr().String()}
	}
	nodes := make([]*testNode, n)
	for i := 0; i < n; i++ {
		cfg := Config{
			NodeID:             infos[i].ID,
			Nodes:              infos,
			HeartbeatEvery:     50 * time.Millisecond,
			SuspectAfter:       300 * time.Millisecond,
			DeadAfter:          900 * time.Millisecond,
			ReplicationFactor:  n,
			ElectionTimeoutMin: 300 * time.Millisecond,
			ElectionTimeoutMax: 600 * time.Millisecond,
			FetchInterval:      20 * time.Millisecond,
			QuorumAckTimeout:   2 * time.Second,
		}.withDefaults()
		cl, err := New(cfg, slog.Default(), WithListener(tcpListener{lns[i]}))
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &testNode{cfg: cfg, cl: cl, ln: lns[i], done: make(chan struct{})}
	}
	for _, nd := range nodes {
		nd := nd
		ctx, cancel := context.WithCancel(context.Background())
		nd.cancel = cancel
		go func() { defer close(nd.done); _ = nd.cl.Run(ctx) }()
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			select {
			case <-nd.done:
			default:
				nd.stop()
			}
		}
	})
	return nodes
}

// waitForState polls until pred holds for all nodes or the deadline hits.
func waitForState(t *testing.T, d time.Duration, pred func(*Cluster) bool, nodes ...*testNode) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		ok := true
		for _, nd := range nodes {
			if !pred(nd.cl) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("condition not met within", d)
}

func TestClusterHeartbeatsActivateAllMembers(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 3)
	waitForState(t, 3*time.Second, func(c *Cluster) bool {
		for _, m := range c.Members() {
			if m.State != StateActive {
				return false
			}
		}
		return true
	}, nodes...)
}

func TestClusterDetectsDeadPeer(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 3)
	waitForState(t, 3*time.Second, func(c *Cluster) bool {
		return c.MemberState("n3") == StateActive
	}, nodes[0], nodes[1])

	// Kill n3 abruptly: cancel its context with the leaving announce
	// disabled — a hard kill (SIGKILL), not a graceful shutdown.
	nodes[2].cl.noLeaveAnnounce.Store(true)
	nodes[2].cancel()
	<-nodes[2].done
	nodes[2].cl.Close()

	// n1 and n2 must walk n3 through SUSPECT to DEAD.
	waitForState(t, 3*time.Second, func(c *Cluster) bool {
		return c.MemberState("n3") == StateDead
	}, nodes[0], nodes[1])
}

func TestClusterGracefulLeaveAnnounced(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 3)
	waitForState(t, 3*time.Second, func(c *Cluster) bool {
		return c.MemberState("n3") == StateActive
	}, nodes[0], nodes[1])

	// A clean Run shutdown sends the leaving announce: peers must see
	// LEAVING quickly (well before the DEAD timeout).
	nodes[2].stop()

	waitForState(t, 3*time.Second, func(c *Cluster) bool {
		return c.MemberState("n3") == StateLeaving
	}, nodes[0], nodes[1])
}

func TestClusterPlacementDeterministic(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 3)
	for _, topic := range []string{"orders", "payments", "events"} {
		for p := int32(0); p < 4; p++ {
			want := nodes[0].cl.AssignmentFor(topic, p)
			for _, nd := range nodes[1:] {
				got := nd.cl.AssignmentFor(topic, p)
				if got.Leader != want.Leader {
					t.Fatalf("%s/%d leader: %s vs %s", topic, p, got.Leader, want.Leader)
				}
				if len(got.Replicas) != len(want.Replicas) {
					t.Fatalf("%s/%d replicas: %v vs %v", topic, p, got.Replicas, want.Replicas)
				}
				for i := range got.Replicas {
					if got.Replicas[i] != want.Replicas[i] {
						t.Fatalf("%s/%d replica[%d]: %s vs %s", topic, p, i, got.Replicas[i], want.Replicas[i])
					}
				}
			}
			// Leader must be one of the replicas.
			found := false
			for _, r := range want.Replicas {
				if r == want.Leader {
					found = true
				}
			}
			if !found {
				t.Fatalf("%s/%d: leader %s not in replicas %v", topic, p, want.Leader, want.Replicas)
			}
		}
	}
}

func TestClusterPlacementSpreadsLeaders(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 3)
	// Over enough partitions every node must lead some.
	leaders := map[NodeID]int{}
	for p := int32(0); p < 30; p++ {
		a := nodes[0].cl.AssignmentFor("spread", p)
		leaders[a.Leader]++
	}
	if len(leaders) < 2 {
		t.Fatalf("leader spread too narrow: %v", leaders)
	}
}

func TestClusterOverlayPrecedence(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 3)
	c := nodes[0].cl
	base := c.AssignmentFor("t", 0)
	custom := Assignment{Topic: "t", Partition: 0, Leader: "n3", Replicas: []NodeID{"n3", "n1"}}
	c.setAssignmentOverlay(custom)
	got := c.AssignmentFor("t", 0)
	if got.Leader != "n3" || len(got.Replicas) != 2 {
		t.Fatalf("overlay ignored: got %+v", got)
	}
	c.clearAssignmentOverlay("t", 0)
	back := c.AssignmentFor("t", 0)
	if back.Leader != base.Leader {
		t.Fatalf("clear did not restore ring placement: got %+v want %+v", back, base)
	}
}

func TestClusterSelfAndConfig(t *testing.T) {
	t.Parallel()
	nodes := startTestCluster(t, 2)
	if nodes[0].cl.Self() != "n1" {
		t.Fatalf("Self: got %s", nodes[0].cl.Self())
	}
	if nodes[0].cl.Config().NodeID != "n1" {
		t.Fatalf("Config().NodeID: got %s", nodes[0].cl.Config().NodeID)
	}
	if !nodes[0].cl.IsLeaderFor("x", 0) && !nodes[1].cl.IsLeaderFor("x", 0) {
		t.Fatal("exactly one node must lead x/0")
	}
	if nodes[0].cl.IsLeaderFor("x", 0) == nodes[1].cl.IsLeaderFor("x", 0) {
		t.Fatal("both nodes think they lead x/0")
	}
}

func TestNewRejectsUnenabledConfig(t *testing.T) {
	t.Parallel()
	if _, err := New(Config{}, slog.Default()); err == nil {
		t.Fatal("empty config must fail")
	}
}
