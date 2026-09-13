package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startBroker boots an in-process broker on a random port.
func startBroker(t *testing.T, mutate func(*broker.Config)) (*broker.Broker, context.CancelFunc, chan error) {
	t.Helper()
	cfg := broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        t.TempDir(),
		FsyncEvery:     20 * time.Millisecond,
		FsyncRecords:   1000,
		SessionTimeout: 5 * time.Second,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	b, err := broker.New(cfg, quietLogger(), nil)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()

	// Wait until the TCP listener accepts.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if addr := b.Addr(); addr != "" {
			c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
			if err == nil {
				c.Close()
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("broker did not start listening in time")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return b, cancel, done
}

// waitFor polls cond until it holds or the deadline expires.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

func TestE2EProduceConsume(t *testing.T) {
	t.Parallel()
	finish := checkLeaks(t, 15*time.Second)
	defer finish()

	b, cancel, brokerDone := startBroker(t, nil)
	addr := b.Addr()

	ctx := context.Background()
	admin := client.NewAdmin(addr)
	if _, err := admin.CreateTopic(ctx, "jobs", 3); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Produce 100 keyed messages.
	producer := client.NewProducer(addr, client.WithProducerLogger(quietLogger()))
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%02d", i%10) // 10 keys spread over 3 partitions
		off, err := producer.Produce(ctx, "jobs", []byte(key), []byte(fmt.Sprintf("value-%d", i)))
		if err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
		_ = off
	}

	// Consume all 100 with one group member.
	var mu sync.Mutex
	perPartition := make(map[int32][]uint64)
	seen := make(map[string]bool)
	consumer := client.NewConsumer(addr, "workers", []string{"jobs"},
		func(_ context.Context, m client.Message) error {
			mu.Lock()
			perPartition[m.Partition] = append(perPartition[m.Partition], m.Offset)
			seen[string(m.Value)] = true
			mu.Unlock()
			return nil
		},
		client.WithConsumerLogger(quietLogger()),
		client.WithPollInterval(20*time.Millisecond),
	)
	consCtx, stopConsume := context.WithCancel(ctx)
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(consCtx) }()

	waitFor(t, "100 messages consumed", 30*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 100
	})

	// Per-partition order: offsets arrive strictly +1 (no gaps, no dups,
	// no reorder) since we consumed from offset 0.
	mu.Lock()
	for part, offsets := range perPartition {
		for i, off := range offsets {
			if i > 0 && off != offsets[i-1]+1 {
				t.Fatalf("partition %d: offset %d after %d (out of order)", part, off, offsets[i-1])
			}
		}
	}
	mu.Unlock()

	// Committed offsets catch up to the high-water marks.
	waitFor(t, "offsets committed", 10*time.Second, func() bool {
		st := b.Status()
		for _, topic := range st.Topics {
			if topic.Name != "jobs" {
				continue
			}
			total := uint64(0)
			for _, p := range topic.Partitions {
				g, ok := p.Groups["workers"]
				if !ok {
					return false
				}
				if g.Committed != p.HighWatermark {
					return false
				}
				total += g.Committed
			}
			return total == 100
		}
		return false
	})

	// Clean stop: consumer leaves, then broker shuts down.
	stopConsume()
	if err := <-consumerDone; err != nil {
		t.Fatalf("consumer run: %v", err)
	}
	if err := producer.Close(); err != nil {
		t.Fatalf("producer close: %v", err)
	}
	if err := admin.Close(); err != nil {
		t.Fatalf("admin close: %v", err)
	}
	cancel()
	select {
	case err := <-brokerDone:
		if err != nil {
			t.Fatalf("broker run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("broker did not shut down in time")
	}
}

func TestE2EBatchedProducer(t *testing.T) {
	t.Parallel()
	b, cancel, brokerDone := startBroker(t, nil)
	defer func() {
		cancel()
		<-brokerDone
	}()
	ctx := context.Background()
	admin := client.NewAdmin(addrOf(b))
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, "batch", 2); err != nil {
		t.Fatal(err)
	}
	p := client.NewProducer(addrOf(b),
		client.WithBatching(5*time.Millisecond, 64),
		client.WithProducerLogger(quietLogger()))
	defer p.Close()

	// Fire 50 produces concurrently: they should batch under the hood.
	var wg sync.WaitGroup
	offsets := make(chan uint64, 50)
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			off, err := p.Produce(ctx, "batch", nil, []byte(fmt.Sprintf("m%d", i)))
			if err != nil {
				t.Errorf("batched produce %d: %v", i, err)
				return
			}
			offsets <- off
		}(i)
	}
	wg.Wait()
	close(offsets)
	n := 0
	for range offsets {
		n++
	}
	if n != 50 {
		t.Fatalf("produced %d of 50", n)
	}

	waitFor(t, "hwm 50 across partitions", 10*time.Second, func() bool {
		resp, err := admin.ListTopics(ctx)
		if err != nil {
			return false
		}
		for _, topic := range resp.Topics {
			if topic.Name != "batch" {
				continue
			}
			total := uint64(0)
			for _, pi := range topic.Partitions {
				total += pi.HighWatermark
			}
			return total == 50
		}
		return false
	})
}

func TestE2ERebalanceMovesPartitions(t *testing.T) {
	t.Parallel()
	b, cancel, brokerDone := startBroker(t, func(c *broker.Config) {
		c.SessionTimeout = 2 * time.Second
	})
	defer func() {
		cancel()
		<-brokerDone
	}()
	ctx := context.Background()
	admin := client.NewAdmin(addrOf(b))
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, "jobs", 3); err != nil {
		t.Fatal(err)
	}
	producer := client.NewProducer(addrOf(b), client.WithProducerLogger(quietLogger()))
	defer producer.Close()

	var mu sync.Mutex
	handledBy := make(map[string]int) // memberID → messages handled
	newConsumer := func(memberID string) (*client.Consumer, context.CancelFunc, chan error) {
		c := client.NewConsumer(addrOf(b), "workers", []string{"jobs"},
			func(_ context.Context, m client.Message) error {
				mu.Lock()
				handledBy[memberID]++
				mu.Unlock()
				return nil
			},
			client.WithConsumerLogger(quietLogger()),
			client.WithMemberID(memberID),
			client.WithHeartbeatEvery(300*time.Millisecond),
			client.WithPollInterval(20*time.Millisecond),
		)
		ctx2, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx2) }()
		return c, stop, done
	}

	_, stop1, done1 := newConsumer("member-1")
	_, stop2, done2 := newConsumer("member-2")

	// Produce keyless messages: round-robin spreads them across all 3
	// partitions, and the range assignor split (2/1) guarantees both
	// members own at least one partition.
	for i := 0; i < 60; i++ {
		if _, err := producer.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "both members handle messages after rebalance", 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handledBy["member-1"] > 0 && handledBy["member-2"] > 0 &&
			handledBy["member-1"]+handledBy["member-2"] >= 60
	})

	// member-2 leaves: member-1 must take over all partitions.
	stop2()
	if err := <-done2; err != nil {
		t.Fatalf("consumer 2: %v", err)
	}
	mark := 0
	mu.Lock()
	mark = handledBy["member-1"] + handledBy["member-2"]
	mu.Unlock()
	for i := 60; i < 90; i++ {
		if _, err := producer.Produce(ctx, "jobs", nil, []byte(fmt.Sprintf("x%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "member-1 handles everything after member-2 left", 20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handledBy["member-1"]+handledBy["member-2"] >= mark+30
	})
	mu.Lock()
	m2Total := handledBy["member-2"]
	mu.Unlock()
	// member-2 handled nothing after leaving: total for m2 stays what it
	// was before the extra 30 (all new ones went to member-1).
	waitFor(t, "member-2 stopped receiving", 5*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return handledBy["member-2"] == m2Total
	})

	stop1()
	if err := <-done1; err != nil {
		t.Fatalf("consumer 1: %v", err)
	}
}

// TestE2ENoRebalanceStorm reproduces the production rebalance storm:
// 5 consumers in one group on a 12-partition topic. Once everyone has
// joined, the group generation must stay constant (polled every 100ms
// for 3s). Then one member leaves: the generation bumps exactly once
// and stability resumes. Before the fix, Join bumped the generation on
// every re-join, so one stale heartbeat set off an endless cascade of
// re-joins and the generation climbed several times per second.
func TestE2ENoRebalanceStorm(t *testing.T) {
	t.Parallel()
	b, cancel, brokerDone := startBroker(t, nil)
	defer func() {
		cancel()
		<-brokerDone
	}()
	ctx := context.Background()
	admin := client.NewAdmin(addrOf(b))
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, "jobs", 12); err != nil {
		t.Fatal(err)
	}

	const numConsumers = 5
	stops := make([]context.CancelFunc, 0, numConsumers)
	dones := make([]chan error, 0, numConsumers)
	for i := 0; i < numConsumers; i++ {
		c := client.NewConsumer(addrOf(b), "workers", []string{"jobs"},
			func(_ context.Context, _ client.Message) error { return nil },
			client.WithConsumerLogger(quietLogger()),
			client.WithMemberID(fmt.Sprintf("member-%d", i)),
			client.WithHeartbeatEvery(200*time.Millisecond),
			client.WithPollInterval(20*time.Millisecond),
		)
		ctx2, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.Run(ctx2) }()
		stops = append(stops, stop)
		dones = append(dones, done)
	}

	generation := func() (int32, int) {
		for _, g := range b.Status().Groups {
			if g.ID == "workers" {
				return g.Generation, g.Members
			}
		}
		return 0, 0
	}
	// assertStable fails if the generation moves within d.
	assertStable := func(from int32, d time.Duration) {
		t.Helper()
		deadline := time.Now().Add(d)
		for time.Now().Before(deadline) {
			if gen, _ := generation(); gen != from {
				t.Fatalf("rebalance storm: generation moved %d → %d during steady state", from, gen)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	waitFor(t, "all 5 members joined", 15*time.Second, func() bool {
		_, n := generation()
		return n == numConsumers
	})
	gen0, _ := generation()
	assertStable(gen0, 3*time.Second)

	// One member leaves: exactly one bump, then the group settles again.
	stops[numConsumers-1]()
	if err := <-dones[numConsumers-1]; err != nil {
		t.Fatalf("consumer %d: %v", numConsumers-1, err)
	}
	waitFor(t, "membership drops to 4", 10*time.Second, func() bool {
		_, n := generation()
		return n == numConsumers-1
	})
	gen1, _ := generation()
	if gen1 != gen0+1 {
		t.Fatalf("one leave should bump the generation exactly once: %d → %d", gen0, gen1)
	}
	assertStable(gen1, 2*time.Second)

	for i := 0; i < numConsumers-1; i++ {
		stops[i]()
	}
	for i := 0; i < numConsumers-1; i++ {
		if err := <-dones[i]; err != nil {
			t.Fatalf("consumer %d: %v", i, err)
		}
	}
}

func addrOf(b *broker.Broker) string { return b.Addr() }
