package cluster

import (
	"strings"
	"testing"
	"time"
)

func TestParseEnvStandaloneDefault(t *testing.T) {
	t.Parallel()
	cfg, enabled, err := ParseEnv("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if enabled {
		t.Fatal("enabled: got true, want false (standalone default)")
	}
	if cfg.NodeID != "" || len(cfg.Nodes) != 0 {
		t.Fatalf("cfg should be zero, got %+v", cfg)
	}
}

func TestParseEnvRequiresBothVars(t *testing.T) {
	t.Parallel()
	if _, _, err := ParseEnv("n1", ""); err == nil {
		t.Fatal("node id without nodes: want error")
	}
	if _, _, err := ParseEnv("", `[{"id":"n1","addr":"127.0.0.1:9201"}]`); err == nil {
		t.Fatal("nodes without node id: want error")
	}
}

func TestParseEnvValidatesJSON(t *testing.T) {
	t.Parallel()
	if _, _, err := ParseEnv("n1", "not-json"); err == nil {
		t.Fatal("bad JSON: want error")
	}
	if _, _, err := ParseEnv("n1", "[]"); err == nil {
		t.Fatal("empty node list: want error")
	}
}

func TestParseEnvValidatesEntries(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		id    string
		nodes string
	}{
		{"empty id", "n1", `[{"id":"","addr":"a:1"}]`},
		{"empty addr", "n1", `[{"id":"n1","addr":""}]`},
		{"dup id", "n1", `[{"id":"n1","addr":"a:1"},{"id":"n1","addr":"a:2"}]`},
		{"self missing", "n9", `[{"id":"n1","addr":"a:1"}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := ParseEnv(tc.id, tc.nodes); err == nil {
				t.Fatalf("want error for %s", tc.name)
			}
		})
	}
}

func TestParseEnvOK(t *testing.T) {
	t.Parallel()
	cfg, enabled, err := ParseEnv("n2",
		`[{"id":"n1","addr":"a:1"},{"id":"n2","addr":"a:2"},{"id":"n3","addr":"a:3"}]`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !enabled {
		t.Fatal("enabled: got false, want true")
	}
	if cfg.NodeID != "n2" {
		t.Fatalf("NodeID: got %q", cfg.NodeID)
	}
	if cfg.ReplicationFactor != 3 {
		t.Fatalf("ReplicationFactor: got %d want 3", cfg.ReplicationFactor)
	}
	if self := cfg.Self(); self.Addr != "a:2" {
		t.Fatalf("Self addr: got %q", self.Addr)
	}
	if peers := cfg.Peers(); len(peers) != 2 {
		t.Fatalf("Peers: got %d want 2", len(peers))
	}
}

func TestReplicationFactorCapsAtClusterSize(t *testing.T) {
	t.Parallel()
	cfg, _, err := ParseEnv("n1", `[{"id":"n1","addr":"a:1"},{"id":"n2","addr":"a:2"}]`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if cfg.ReplicationFactor != 2 {
		t.Fatalf("ReplicationFactor: got %d want 2 (capped at cluster size)", cfg.ReplicationFactor)
	}
}

func TestDefaults(t *testing.T) {
	t.Parallel()
	cfg, _, err := ParseEnv("n1", `[{"id":"n1","addr":"a:1"}]`)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	checks := map[string]struct{ got, want time.Duration }{
		"HeartbeatEvery":     {cfg.HeartbeatEvery, 500 * time.Millisecond},
		"SuspectAfter":       {cfg.SuspectAfter, 3 * time.Second},
		"DeadAfter":          {cfg.DeadAfter, 10 * time.Second},
		"FetchInterval":      {cfg.FetchInterval, 200 * time.Millisecond},
		"ElectionTimeoutMin": {cfg.ElectionTimeoutMin, 1500 * time.Millisecond},
		"ElectionTimeoutMax": {cfg.ElectionTimeoutMax, 3 * time.Second},
		"QuorumAckTimeout":   {cfg.QuorumAckTimeout, 5 * time.Second},
		"AbdicateAfter":      {cfg.LeaderAbdicateAfter, 10 * time.Second},
	}
	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s: got %v want %v", name, c.got, c.want)
		}
	}
	if cfg.ReplicaLagMax != 1000 {
		t.Errorf("ReplicaLagMax: got %d want 1000", cfg.ReplicaLagMax)
	}
	if cfg.FetchMaxBytes != 2<<20 {
		t.Errorf("FetchMaxBytes: got %d", cfg.FetchMaxBytes)
	}
}

func TestParseEnvErrorMentionsVar(t *testing.T) {
	t.Parallel()
	_, _, err := ParseEnv("n1", "{")
	if err == nil || !strings.Contains(err.Error(), "BROKER_CLUSTER_NODES") {
		t.Fatalf("error should name BROKER_CLUSTER_NODES, got: %v", err)
	}
}
