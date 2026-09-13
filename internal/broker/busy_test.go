package broker

import (
	"context"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
	"github.com/refleeexzz/RAVEN/internal/broker/storage"
)

// TestProduceBusyWhenQueueFull verifies the backpressure contract: when
// the per-partition writer queue is full, PRODUCE fails with
// BROKER_BUSY instead of blocking or growing memory. White-box: the
// writer goroutine is intentionally not started, so its queue never
// drains.
func TestProduceBusyWhenQueueFull(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store, err := storage.OpenStore(dir, storage.Options{MaxSegmentBytes: 64 << 20, IndexIntervalBytes: 4096}, 3, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	topic, err := store.CreateTopic("jobs", 1)
	if err != nil {
		t.Fatal(err)
	}

	cfg := Config{ProduceQueueSize: 1}.withDefaults()
	b := &Broker{
		cfg:     cfg,
		store:   store,
		metrics: newMetrics(),
		writers: make(map[topicPart]*partWriter),
	}
	w := &partWriter{
		topic:        "jobs",
		partitionID:  0,
		partition:    topic.Partitions[0],
		in:           make(chan appendRequest, 1),
		fsyncRecords: cfg.FsyncRecords,
		metrics:      b.metrics,
		done:         make(chan struct{}),
	}
	b.writers[topicPart{"jobs", 0}] = w

	msg := protocol.Message{Value: []byte("x")}
	req := &protocol.ProduceRequest{Topic: "jobs", Partition: 0, Records: []protocol.Message{msg}}

	// First produce fills the queue; it then blocks waiting for the
	// (never running) writer until its ctx expires.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	_, _ = b.Produce(ctx, req)
	cancel()

	// Second produce hits a full queue → BROKER_BUSY.
	_, err = b.Produce(context.Background(), req)
	pe, ok := err.(*protocol.Error)
	if !ok || pe.Code != protocol.CodeBrokerBusy {
		t.Fatalf("got %v, want BROKER_BUSY", err)
	}
}
