package benchmarks

import (
	"context"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
)

// Go benchmarks over the roadmap matrix: producers {1,10,100} x sizes
// {128B,1KB,10KB,100KB} x fsync modes {always,periodic,disabled}, and
// consumers {1,10,100}. Every benchmark boots a real broker on loopback
// (127.0.0.1:0, fresh temp data dir) so the number is the full stack:
// TCP frame -> dispatch -> partition writer -> segment append.
//
// Parallelism is a fixed worker pool (not b.RunParallel): SetParallelism
// scales with GOMAXPROCS, which would make "10 producers" a lie on a
// 16-core laptop. A shared atomic counter hands out exactly b.N produces
// across exactly P goroutines.
//
// Run:  go test -run=^$ -bench=. -benchmem ./benchmarks/
// Trim: go test -run=^$ -bench='Produce/1KB' -benchmem ./benchmarks/

func benchProduce(b *testing.B, producers, size int, mode FsyncMode) {
	addr, stop, err := StartBroker(b.TempDir(), mode)
	if err != nil {
		b.Fatal(err)
	}
	defer stop()
	ctx := context.Background()
	if err := CreateTopic(ctx, addr, "bench", 3); err != nil {
		b.Fatal(err)
	}
	prods := make([]*client.Producer, producers)
	for i := range prods {
		prods[i] = client.NewProducer(addr, client.WithProducerLogger(QuietLogger()))
	}
	defer func() {
		for _, p := range prods {
			_ = p.Close()
		}
	}()
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		b.Fatal(err)
	}

	var counter atomic.Int64
	var firstErr atomic.Value
	b.SetBytes(int64(size))
	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < producers; w++ {
		wg.Add(1)
		go func(p *client.Producer) {
			defer wg.Done()
			for {
				if counter.Add(1) > int64(b.N) {
					return
				}
				if _, err := p.Produce(ctx, "bench", nil, value); err != nil {
					firstErr.Store(err)
					return
				}
			}
		}(prods[w])
	}
	wg.Wait()
	b.StopTimer()
	if e := firstErr.Load(); e != nil {
		b.Fatalf("produce failed: %v", e)
	}
}

func BenchmarkProduce(b *testing.B) {
	sizes := []struct {
		name string
		n    int
	}{
		{"128B", 128},
		{"1KB", 1024},
		{"10KB", 10 * 1024},
		{"100KB", 100 * 1024},
	}
	for _, producers := range []int{1, 10, 100} {
		for _, s := range sizes {
			b.Run(fmt.Sprintf("%s/p=%d", s.name, producers), func(b *testing.B) {
				benchProduce(b, producers, s.n, FsyncPeriodic)
			})
		}
	}
}

func BenchmarkProduceDurability(b *testing.B) {
	for _, mode := range []FsyncMode{FsyncAlways, FsyncPeriodic, FsyncDisabled} {
		b.Run("fsync="+string(mode), func(b *testing.B) {
			benchProduce(b, 1, 1024, mode)
		})
	}
}

func benchConsume(b *testing.B, consumers, size int, mode FsyncMode) {
	addr, stop, err := StartBroker(b.TempDir(), mode)
	if err != nil {
		b.Fatal(err)
	}
	defer stop()
	ctx := context.Background()
	if err := CreateTopic(ctx, addr, "bench", int32(max(consumers, 1))); err != nil {
		b.Fatal(err)
	}
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		b.Fatal(err)
	}
	// Pre-fill exactly b.N messages: a live feeder producing 1-by-1 would
	// be the bottleneck (~1/flush-interval msg/s) and the benchmark would
	// measure the producer, not the consumers.
	if err := prefill(ctx, addr, "bench", value, b.N); err != nil {
		b.Fatal(err)
	}

	var delivered atomic.Int64
	handler := func(_ context.Context, _ client.Message) error {
		delivered.Add(1)
		return nil
	}
	consumeCtx, stopConsume := context.WithCancel(ctx)
	defer stopConsume()
	var consWg sync.WaitGroup
	for i := 0; i < consumers; i++ {
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

	b.SetBytes(int64(size))
	b.ResetTimer()
	for delivered.Load() < int64(b.N) {
		time.Sleep(2 * time.Millisecond)
	}
	b.StopTimer()
	stopConsume()
	consWg.Wait()
}

func BenchmarkConsume(b *testing.B) {
	for _, consumers := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("1KB/c=%d", consumers), func(b *testing.B) {
			benchConsume(b, consumers, 1024, FsyncPeriodic)
		})
	}
}
