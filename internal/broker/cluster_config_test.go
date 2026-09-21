package broker

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/cluster"
)

// clusterEnv builds a BROKER_CLUSTER_NODES JSON list from loopback
// addresses the test already bound.
func clusterEnv(t *testing.T, n int) (nodesJSON string, addrs []string, cleanup func()) {
	t.Helper()
	lns := make([]net.Listener, n)
	addrs = make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		addrs[i] = ln.Addr().String()
	}
	nodesJSON = "["
	for i, a := range addrs {
		if i > 0 {
			nodesJSON += ","
		}
		nodesJSON += fmt.Sprintf(`{"id":"n%d","addr":%q}`, i+1, a)
	}
	nodesJSON += "]"
	return nodesJSON, addrs, func() {
		for _, ln := range lns {
			_ = ln.Close()
		}
	}
}

func TestClusterConfigStandaloneDefault(t *testing.T) {
	// No t.Parallel: env vars are process-global.
	t.Setenv("BROKER_NODE_ID", "")
	t.Setenv("BROKER_CLUSTER_NODES", "")
	cfg := ConfigFromEnv()
	if cfg.ClusterEnabled {
		t.Fatal("ClusterEnabled: got true, want false (standalone default)")
	}
	if cfg.clusterErr != nil {
		t.Fatalf("clusterErr: %v", cfg.clusterErr)
	}
	// withDefaults must preserve the standalone decision.
	cfg = cfg.withDefaults()
	if cfg.ClusterEnabled {
		t.Fatal("withDefaults flipped ClusterEnabled")
	}
}

func TestClusterConfigFromEnv(t *testing.T) {
	nodesJSON, _, cleanup := clusterEnv(t, 2)
	defer cleanup()
	t.Setenv("BROKER_NODE_ID", "n1")
	t.Setenv("BROKER_CLUSTER_NODES", nodesJSON)
	t.Setenv("BROKER_REPLICATION_FACTOR", "2")
	t.Setenv("BROKER_CLUSTER_HEARTBEAT_MS", "100")
	t.Setenv("BROKER_CLUSTER_DEAD_MS", "1000")
	cfg := ConfigFromEnv()
	if !cfg.ClusterEnabled {
		t.Fatal("ClusterEnabled: got false, want true")
	}
	if cfg.clusterErr != nil {
		t.Fatalf("clusterErr: %v", cfg.clusterErr)
	}
	if cfg.Cluster.NodeID != "n1" {
		t.Fatalf("NodeID: got %q", cfg.Cluster.NodeID)
	}
	if cfg.Cluster.ReplicationFactor != 2 {
		t.Fatalf("ReplicationFactor: got %d want 2", cfg.Cluster.ReplicationFactor)
	}
	if cfg.Cluster.HeartbeatEvery != 100*time.Millisecond {
		t.Fatalf("HeartbeatEvery: got %v", cfg.Cluster.HeartbeatEvery)
	}
	if cfg.Cluster.DeadAfter != time.Second {
		t.Fatalf("DeadAfter: got %v", cfg.Cluster.DeadAfter)
	}
	// withDefaults must keep the cluster config intact.
	if d := cfg.withDefaults(); !d.ClusterEnabled || d.Cluster.NodeID != "n1" {
		t.Fatalf("withDefaults lost cluster config: %+v", d.Cluster)
	}
}

func TestClusterConfigMalformedFailsBoot(t *testing.T) {
	t.Setenv("BROKER_NODE_ID", "n1")
	t.Setenv("BROKER_CLUSTER_NODES", "{not json")
	cfg := ConfigFromEnv()
	if cfg.clusterErr == nil {
		t.Fatal("clusterErr: got nil, want parse failure")
	}
	// The error must survive withDefaults so New fails closed.
	cfg = cfg.withDefaults()
	if cfg.clusterErr == nil {
		t.Fatal("withDefaults dropped clusterErr")
	}
	b, err := New(cfg, nil, nil)
	if err == nil {
		b.store.Close()
		t.Fatal("New: got nil error, want fail-closed boot")
	}
}

func TestBrokerClusterModeBoots(t *testing.T) {
	nodesJSON, addrs, cleanup := clusterEnv(t, 2)
	// Release the discovery listeners before the broker binds the
	// cluster listener to n1's address.
	cleanup()
	cfg := Config{
		TCPAddr:    "127.0.0.1:0",
		DataDir:    t.TempDir(),
		FsyncEvery: time.Second,
	}
	// Resolve the cluster part through the same parser env uses.
	clusterCfg, on, err := cluster.ParseEnv("n1", nodesJSON)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Cluster = clusterCfg
	cfg.ClusterEnabled = on
	b, err := New(cfg, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Cluster() == nil {
		t.Fatal("Cluster(): got nil in cluster mode")
	}
	// The cluster listener must be bound to n1's configured address.
	if got := b.Cluster().Addr(); got != addrs[0] {
		t.Fatalf("cluster addr: got %q want %q", got, addrs[0])
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	// Give the control plane a moment, then shut down cleanly.
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestBrokerStandaloneHasNoCluster(t *testing.T) {
	cfg := Config{TCPAddr: "127.0.0.1:0", DataDir: t.TempDir()}
	b, err := New(cfg, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if b.Cluster() != nil {
		t.Fatal("Cluster(): got non-nil in standalone mode")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}
