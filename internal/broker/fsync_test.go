package broker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/storage"
)

func discardLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitCond(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestFsyncRecordsPolicy: the partition writer must flush+fsync once
// FsyncRecords records accumulated, without waiting for the interval
// ticker. We watch RecordsSinceFlush: it climbs per append and drops
// to 0 exactly when the count policy fires.
func TestFsyncRecordsPolicy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := storage.OpenStore(dir, storage.Options{MaxSegmentBytes: 64 << 20, IndexIntervalBytes: 4096}, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	topic, err := store.CreateTopic("jobs", 1)
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{
		ProduceQueueSize: 16,
		FsyncRecords:     2,
		FsyncEvery:       time.Hour, // ticker must NOT fire: only the record count may flush
	}.withDefaults()
	b := &Broker{
		cfg:     cfg,
		log:     discardLog(),
		store:   store,
		metrics: newMetrics(),
		writers: make(map[topicPart]*partWriter),
	}
	b.registerWriter("jobs", topic.Partitions[0])
	w := b.writers[topicPart{"jobs", 0}]
	t.Cleanup(func() { close(w.in); <-w.done })

	produceOne := func() {
		t.Helper()
		req := &protocol.ProduceRequest{
			Topic: "jobs", Partition: 0,
			Records: []protocol.Message{{Value: []byte("x")}},
		}
		if _, err := b.Produce(context.Background(), req); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	p := topic.Partitions[0]

	produceOne() // rsf = 1, below the policy
	waitCond(t, "no flush below FsyncRecords", time.Second, func() bool {
		return p.RecordsSinceFlush() == 1
	})
	produceOne() // rsf = 2 → policy fires → flush → rsf = 0
	waitCond(t, "flush at FsyncRecords", 2*time.Second, func() bool {
		return p.RecordsSinceFlush() == 0
	})
	produceOne() // rsf = 1 again
	waitCond(t, "counter restarts after flush", time.Second, func() bool {
		return p.RecordsSinceFlush() == 1
	})
}

// TestFsyncIntervalPolicy: with the record-count policy effectively
// disabled, the background ticker must flush dirty partitions on the
// BROKER_FSYNC_MS cadence. Runs the full broker (storage + writers +
// ticker + TCP) but drives it in-process.
func TestFsyncIntervalPolicy(t *testing.T) {
	t.Parallel()
	cfg := Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        t.TempDir(),
		FsyncEvery:     30 * time.Millisecond,
		FsyncRecords:   1 << 30, // count policy never fires
		SessionTimeout: 5 * time.Second,
	}
	b, err := New(cfg, discardLog(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("broker did not stop")
		}
	})

	if _, err := b.CreateTopic(context.Background(), &protocol.CreateTopicRequest{Topic: "jobs", Partitions: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Produce(context.Background(), &protocol.ProduceRequest{
		Topic: "jobs", Partition: 0,
		Records: []protocol.Message{{Value: []byte("x")}},
	}); err != nil {
		t.Fatal(err)
	}

	topic, err := b.store.Topic("jobs")
	if err != nil {
		t.Fatal(err)
	}
	p := topic.Partitions[0]
	if got := p.RecordsSinceFlush(); got != 1 {
		t.Fatalf("records since flush right after produce: %d", got)
	}
	// The 30 ms ticker flushes it shortly after.
	waitCond(t, "interval flush", 3*time.Second, func() bool {
		return p.RecordsSinceFlush() == 0
	})
}

// TestConfigFromEnvHardening: the new hardening knobs load from env and
// fall back to their documented defaults, including on garbage input.
func TestConfigFromEnvHardening(t *testing.T) {
	// Not parallel: touches process env.
	t.Run("defaults", func(t *testing.T) {
		cfg := Config{}.withDefaults()
		if cfg.MaxConnections != 1024 {
			t.Fatalf("MaxConnections default: %d", cfg.MaxConnections)
		}
		if cfg.IdleTimeout != 5*time.Minute {
			t.Fatalf("IdleTimeout default: %s", cfg.IdleTimeout)
		}
		if cfg.WriteTimeout != 30*time.Second {
			t.Fatalf("WriteTimeout default: %s", cfg.WriteTimeout)
		}
	})
	t.Run("from env", func(t *testing.T) {
		t.Setenv("BROKER_MAX_CONNECTIONS", "7")
		t.Setenv("BROKER_IDLE_TIMEOUT", "90s")
		t.Setenv("BROKER_WRITE_TIMEOUT", "1500ms")
		cfg := ConfigFromEnv()
		if cfg.MaxConnections != 7 {
			t.Fatalf("MaxConnections: %d", cfg.MaxConnections)
		}
		if cfg.IdleTimeout != 90*time.Second {
			t.Fatalf("IdleTimeout: %s", cfg.IdleTimeout)
		}
		if cfg.WriteTimeout != 1500*time.Millisecond {
			t.Fatalf("WriteTimeout: %s", cfg.WriteTimeout)
		}
	})
	t.Run("garbage falls back", func(t *testing.T) {
		t.Setenv("BROKER_MAX_CONNECTIONS", "lots")
		t.Setenv("BROKER_IDLE_TIMEOUT", "eventually")
		t.Setenv("BROKER_WRITE_TIMEOUT", "-5s")
		cfg := ConfigFromEnv()
		if cfg.MaxConnections != 1024 {
			t.Fatalf("MaxConnections on garbage: %d", cfg.MaxConnections)
		}
		if cfg.IdleTimeout != 5*time.Minute {
			t.Fatalf("IdleTimeout on garbage: %s", cfg.IdleTimeout)
		}
		// GetDuration parses "-5s" successfully; withDefaults keeps the
		// default for anything <= 0 downstream. Document the behavior:
		if cfg.WriteTimeout != -5*time.Second {
			t.Fatalf("WriteTimeout passthrough: %s", cfg.WriteTimeout)
		}
		if cfg.withDefaults().WriteTimeout != 30*time.Second {
			t.Fatalf("WriteTimeout negative not defaulted: %s", cfg.withDefaults().WriteTimeout)
		}
	})
}
