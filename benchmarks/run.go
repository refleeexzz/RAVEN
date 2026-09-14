// Package benchmarks is RAVEN's reproducible broker benchmark harness.
// It backs both the Go benchmarks in broker_bench_test.go (go test -bench)
// and the standalone runner in cmd/benchrun, which measures throughput AND
// latency percentiles per scenario and prints the results table that feeds
// docs/benchmarks.md.
//
// Every scenario runs against a real in-process broker over loopback TCP —
// the same full stack production traffic crosses: frame codec → dispatch →
// per-partition writer → segment append → (optional) fsync.
//
// What Go benchmarks measure well (msg/s, allocs via -benchmem) stays in
// the _test.go file; what they cannot (P50/P95/P99 latency of a whole
// scenario, CPU) is measured here with an explicit latency recorder.
// CPU utilization is NOT measurable from inside the benchmark process on
// Windows in a portable way, so the runner reports it as n/a instead of
// inventing a number.
package benchmarks

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
)

// FsyncMode selects the broker durability policy under test. It maps onto
// the two broker knobs: BROKER_FSYNC_MS (background flush cadence) and
// BROKER_FSYNC_RECORDS (flush after N appended records, whichever first).
type FsyncMode string

const (
	// FsyncAlways fsyncs after every record batch (FsyncRecords=1).
	FsyncAlways FsyncMode = "always"
	// FsyncPeriodic is the production default: 100 ms or 256 records.
	FsyncPeriodic FsyncMode = "periodic"
	// FsyncDisabled never fsyncs on the write path (data still flushed at
	// close): the pure append-speed ceiling.
	FsyncDisabled FsyncMode = "disabled"
)

// brokerConfig builds the broker config for one scenario. dataDir must be
// a fresh temp dir per scenario so recovery never pollutes measurements.
func brokerConfig(dataDir string, mode FsyncMode) broker.Config {
	cfg := broker.Config{
		TCPAddr:           "127.0.0.1:0",
		DataDir:           dataDir,
		DefaultPartitions: 3,
		SessionTimeout:    10 * time.Second,
	}
	switch mode {
	case FsyncAlways:
		cfg.FsyncEvery = time.Hour // record-driven only
		cfg.FsyncRecords = 1
	case FsyncDisabled:
		cfg.FsyncEvery = time.Hour
		cfg.FsyncRecords = 1 << 30 // effectively never on the write path
	default: // FsyncPeriodic
		cfg.FsyncEvery = 100 * time.Millisecond
		cfg.FsyncRecords = 256
	}
	return cfg
}

// QuietLogger keeps benchmark output free of broker logs.
func QuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// StartBroker boots a broker on a loopback ephemeral port. The returned
// stop func shuts it down and waits for a clean exit.
func StartBroker(dir string, mode FsyncMode) (addr string, stop func(), err error) {
	br, err := broker.New(brokerConfig(dir, mode), QuietLogger(), nil)
	if err != nil {
		return "", nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- br.Run(ctx) }()

	deadline := time.Now().Add(10 * time.Second)
	for br.Addr() == "" {
		if time.Now().After(deadline) {
			cancel()
			<-done
			return "", nil, fmt.Errorf("broker did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return br.Addr(), func() { cancel(); <-done }, nil
}

// CreateTopic is a small helper shared by scenarios.
func CreateTopic(ctx context.Context, addr, topic string, partitions int32) error {
	admin := client.NewAdmin(addr)
	defer func() { _ = admin.Close() }()
	_, err := admin.CreateTopic(ctx, topic, partitions)
	return err
}

// ---------------------------------------------------------------------------
// Latency recorder
// ---------------------------------------------------------------------------

// latencyRecorder collects per-call durations with one heap-allocated
// slice per worker goroutine, so the hot path needs no locks and no
// atomics. Each shard owns its slice header through a stable pointer, so
// appends never race and never invalidate anything the recorder holds;
// the recorder merges shards only after every worker has finished.
type latencyRecorder struct {
	mu     sync.Mutex
	shards []*[]time.Duration
}

// shard returns this worker's private recorder. Call once per goroutine.
func (r *latencyRecorder) shard() *latencyShard {
	buf := make([]time.Duration, 0, 4096)
	r.mu.Lock()
	r.shards = append(r.shards, &buf)
	r.mu.Unlock()
	return &latencyShard{buf: &buf}
}

type latencyShard struct {
	buf *[]time.Duration
}

func (s *latencyShard) observe(d time.Duration) { *s.buf = append(*s.buf, d) }

// percentiles merges all shards and computes p50/p95/p99 in microseconds.
// Callers must run it after all workers stopped observing.
func (r *latencyRecorder) percentiles() (p50, p95, p99 float64, n int) {
	total := 0
	for _, s := range r.shards {
		total += len(*s)
	}
	if total == 0 {
		return 0, 0, 0, 0
	}
	all := make([]time.Duration, 0, total)
	for _, s := range r.shards {
		all = append(all, (*s)...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	pick := func(q float64) float64 {
		idx := int(q * float64(len(all)))
		if idx >= len(all) {
			idx = len(all) - 1
		}
		return float64(all[idx].Microseconds())
	}
	return pick(0.50), pick(0.95), pick(0.99), total
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

// Result is one scenario's measurement. Latencies are microseconds.
type Result struct {
	Scenario  string
	Messages  int
	Duration  time.Duration
	MsgPerSec float64
	P50Us     float64
	P95Us     float64
	P99Us     float64
	// AllocBytesPerOp is the process-wide heap allocation delta divided by
	// messages — approximate by construction (background broker work
	// included); the Go benchmarks' -benchmem numbers are the precise ones.
	AllocBytesPerOp float64
	// CPU is reported as "n/a": per-scenario CPU% is not measurable from
	// inside the benchmark process portably. Honest beats invented.
	CPU string
}

// ProduceScenario measures producer throughput and latency.
type ProduceScenario struct {
	Name      string
	Producers int
	SizeBytes int
	Fsync     FsyncMode
	Count     int // total messages across all producers
}

// RunProduce executes one produce scenario against a fresh broker.
func RunProduce(ctx context.Context, dir string, sc ProduceScenario) (Result, error) {
	addr, stop, err := StartBroker(dir, sc.Fsync)
	if err != nil {
		return Result{}, err
	}
	defer stop()
	if err := CreateTopic(ctx, addr, "bench", 3); err != nil {
		return Result{}, fmt.Errorf("create topic: %w", err)
	}

	value := make([]byte, sc.SizeBytes)
	if _, err := rand.Read(value); err != nil {
		return Result{}, err
	}

	// One client per producer, mirroring real deployments (one connection
	// per producer process).
	producers := make([]*client.Producer, sc.Producers)
	for i := range producers {
		producers[i] = client.NewProducer(addr, client.WithProducerLogger(QuietLogger()))
	}
	defer func() {
		for _, p := range producers {
			_ = p.Close()
		}
	}()

	var rec latencyRecorder
	var counter atomic.Int64
	var wg sync.WaitGroup
	var firstErr atomic.Value

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	start := time.Now()
	for w := 0; w < sc.Producers; w++ {
		wg.Add(1)
		go func(p *client.Producer) {
			defer wg.Done()
			shard := rec.shard()
			for {
				i := counter.Add(1)
				if i > int64(sc.Count) {
					return
				}
				t0 := hiresNow()
				_, err := p.Produce(ctx, "bench", nil, value)
				shard.observe(hiresNow() - t0)
				if err != nil {
					firstErr.Store(err)
					return
				}
			}
		}(producers[w])
	}
	wg.Wait()
	elapsed := time.Since(start)
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	if e := firstErr.Load(); e != nil {
		return Result{}, fmt.Errorf("produce failed: %v", e)
	}
	p50, p95, p99, n := rec.percentiles()
	return Result{
		Scenario:        sc.Name,
		Messages:        n,
		Duration:        elapsed,
		MsgPerSec:       float64(n) / elapsed.Seconds(),
		P50Us:           p50,
		P95Us:           p95,
		P99Us:           p99,
		AllocBytesPerOp: float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / float64(max(n, 1)),
		CPU:             "n/a",
	}, nil
}

// ConsumeScenario measures consumer throughput: the topic is pre-filled
// with Count messages (producers are benchmarked separately), then N group
// consumers drain it and the wall time to deliver everything is measured.
// Pre-filling matters: a live feeder producing 1-by-1 through the batching
// client caps at ~1/flush-interval msg/s and the measurement would be the
// feeder, not the consumers.
type ConsumeScenario struct {
	Name      string
	Consumers int // group members; the topic gets the same partition count
	SizeBytes int
	Fsync     FsyncMode
	Count     int // messages to pre-fill and drain
}

// RunConsume executes one consume scenario against a fresh broker.
func RunConsume(ctx context.Context, dir string, sc ConsumeScenario) (Result, error) {
	addr, stop, err := StartBroker(dir, sc.Fsync)
	if err != nil {
		return Result{}, err
	}
	defer stop()
	partitions := int32(max(sc.Consumers, 1))
	if err := CreateTopic(ctx, addr, "bench", partitions); err != nil {
		return Result{}, fmt.Errorf("create topic: %w", err)
	}

	// Pre-fill the backlog with parallel plain (non-batched) producers.
	value := make([]byte, sc.SizeBytes)
	if _, err := rand.Read(value); err != nil {
		return Result{}, err
	}
	if err := prefill(ctx, addr, "bench", value, sc.Count); err != nil {
		return Result{}, err
	}

	var delivered atomic.Int64
	firstDelivery := make(chan struct{})
	var firstOnce sync.Once

	handler := func(_ context.Context, _ client.Message) error {
		firstOnce.Do(func() { close(firstDelivery) })
		delivered.Add(1)
		return nil
	}

	consumeCtx, stopConsume := context.WithCancel(ctx)
	defer stopConsume()
	consWg := sync.WaitGroup{}
	for i := 0; i < sc.Consumers; i++ {
		c := client.NewConsumer(addr, "bench-group", []string{"bench"}, handler,
			client.WithConsumerLogger(QuietLogger()),
			client.WithFetchLimits(500, 8<<20),
			client.WithPollInterval(5*time.Millisecond))
		consWg.Add(1)
		go func() {
			defer consWg.Done()
			_ = c.Run(consumeCtx)
		}()
	}

	// Wait for the group to actually deliver, then measure the drain. No
	// counter reset: exactly Count messages exist, and the first delivery
	// (which opened the gate) is one of them — resetting would wait for
	// one message more than the backlog holds.
	select {
	case <-firstDelivery:
	case <-time.After(30 * time.Second):
		stopConsume()
		consWg.Wait()
		return Result{}, fmt.Errorf("consumers delivered nothing in 30s")
	}
	start := time.Now()
	for delivered.Load() < int64(sc.Count) {
		time.Sleep(2 * time.Millisecond)
	}
	elapsed := time.Since(start)
	stopConsume()
	consWg.Wait()

	// Consume latency per message is not recorded per-call (the handler is
	// batch-driven); percentiles stay 0 and the docs table reads n/a.
	n := int(delivered.Load())
	return Result{
		Scenario:  sc.Name,
		Messages:  n,
		Duration:  elapsed,
		MsgPerSec: float64(n) / elapsed.Seconds(),
		CPU:       "n/a",
	}, nil
}

// prefill writes count copies of value to topic using 8 parallel
// non-batched producers (fast: tens of thousands msg/s on loopback).
func prefill(ctx context.Context, addr, topic string, value []byte, count int) error {
	const workers = 8
	prods := make([]*client.Producer, workers)
	for i := range prods {
		prods[i] = client.NewProducer(addr, client.WithProducerLogger(QuietLogger()))
	}
	defer func() {
		for _, p := range prods {
			_ = p.Close()
		}
	}()
	var counter atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(p *client.Producer) {
			defer wg.Done()
			for {
				if counter.Add(1) > int64(count) {
					return
				}
				if _, err := p.Produce(ctx, topic, nil, value); err != nil {
					firstErr.Store(err)
					return
				}
			}
		}(prods[w])
	}
	wg.Wait()
	if e := firstErr.Load(); e != nil {
		return fmt.Errorf("prefill: %v", e)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Default (laptop-trimmed) scenario sets
// ---------------------------------------------------------------------------

// ProduceScenarios is the roadmap matrix trimmed to finish in minutes on a
// laptop: the full 4 sizes × {1,10,100} producers, with message counts
// scaled down for the big payloads. fsync=periodic (production default).
func ProduceScenarios() []ProduceScenario {
	type row struct {
		name      string
		producers int
		count128  int
		count1K   int
		count10K  int
		count100K int
	}
	rows := []row{
		{"1 producer", 1, 20000, 20000, 10000, 2000},
		{"10 producers", 10, 40000, 40000, 20000, 4000},
		{"100 producers", 100, 60000, 60000, 30000, 6000},
	}
	sizes := []struct {
		label string
		bytes int
		pick  func(row) int
	}{
		{"128B", 128, func(r row) int { return r.count128 }},
		{"1KB", 1024, func(r row) int { return r.count1K }},
		{"10KB", 10 * 1024, func(r row) int { return r.count10K }},
		{"100KB", 100 * 1024, func(r row) int { return r.count100K }},
	}
	var out []ProduceScenario
	for _, s := range sizes {
		for _, r := range rows {
			out = append(out, ProduceScenario{
				Name:      fmt.Sprintf("produce %s x %s", r.name, s.label),
				Producers: r.producers,
				SizeBytes: s.bytes,
				Fsync:     FsyncPeriodic,
				Count:     s.pick(r),
			})
		}
	}
	return out
}

// DurabilityScenarios isolates the fsync policy at 1 KB with 1 producer.
func DurabilityScenarios() []ProduceScenario {
	return []ProduceScenario{
		{Name: "produce 1KB fsync=always", Producers: 1, SizeBytes: 1024, Fsync: FsyncAlways, Count: 2000},
		{Name: "produce 1KB fsync=periodic", Producers: 1, SizeBytes: 1024, Fsync: FsyncPeriodic, Count: 20000},
		{Name: "produce 1KB fsync=disabled", Producers: 1, SizeBytes: 1024, Fsync: FsyncDisabled, Count: 20000},
	}
}

// ConsumeScenarios: {1,10,100} consumers draining 1 KB messages.
func ConsumeScenarios() []ConsumeScenario {
	return []ConsumeScenario{
		{Name: "consume 1 consumer x 1KB", Consumers: 1, SizeBytes: 1024, Fsync: FsyncPeriodic, Count: 20000},
		{Name: "consume 10 consumers x 1KB", Consumers: 10, SizeBytes: 1024, Fsync: FsyncPeriodic, Count: 30000},
		{Name: "consume 100 consumers x 1KB", Consumers: 100, SizeBytes: 1024, Fsync: FsyncPeriodic, Count: 30000},
	}
}

// Markdown renders results as the docs/benchmarks.md table rows.
func Markdown(results []Result) string {
	out := "| Scenario | Messages | Duration | Msg/s | P50 (µs) | P95 (µs) | P99 (µs) | CPU | ~Alloc B/op |\n"
	out += "|----------|----------|----------|-------|----------|----------|----------|-----|-------------|\n"
	for _, r := range results {
		p50, p95, p99 := "n/a", "n/a", "n/a"
		if r.P99Us > 0 { // consume scenarios record no per-message latency
			p50 = fmt.Sprintf("%.0f", r.P50Us)
			p95 = fmt.Sprintf("%.0f", r.P95Us)
			p99 = fmt.Sprintf("%.0f", r.P99Us)
		}
		alloc := "n/a"
		if r.AllocBytesPerOp > 0 {
			alloc = fmt.Sprintf("%.0f", r.AllocBytesPerOp)
		}
		out += fmt.Sprintf("| %s | %d | %.2fs | %.0f | %s | %s | %s | %s | %s |\n",
			r.Scenario, r.Messages, r.Duration.Seconds(), r.MsgPerSec, p50, p95, p99, r.CPU, alloc)
	}
	return out
}
