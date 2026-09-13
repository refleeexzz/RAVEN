package broker_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
)

// BenchmarkProduce measures single-record produce throughput with 1 KiB
// values through the full stack: TCP frame → dispatch → partition
// writer → append. Fsync policy is the production default (100 ms or
// 256 records, whichever first).
func BenchmarkProduce(b *testing.B) {
	cfg := broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        b.TempDir(),
		FsyncEvery:     100 * time.Millisecond,
		FsyncRecords:   256,
		SessionTimeout: 10 * time.Second,
	}
	br, err := broker.New(cfg, quietLogger(), nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- br.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	deadline := time.Now().Add(10 * time.Second)
	for br.Addr() == "" {
		if time.Now().After(deadline) {
			b.Fatal("broker did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}

	admin := client.NewAdmin(br.Addr())
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, "bench", 3); err != nil {
		b.Fatal(err)
	}

	p := client.NewProducer(br.Addr(), client.WithProducerLogger(quietLogger()))
	defer p.Close()

	value := make([]byte, 1024)
	if _, err := rand.Read(value); err != nil {
		b.Fatal(err)
	}
	key := []byte("bench-key")

	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Produce(ctx, "bench", key, value); err != nil {
			b.Fatalf("produce %d: %v", i, err)
		}
	}
}

// BenchmarkProduceBatched is the same measurement through the
// micro-batching producer (flush every 5 ms or 64 messages), which
// amortizes frame and request overhead.
func BenchmarkProduceBatched(b *testing.B) {
	cfg := broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        b.TempDir(),
		FsyncEvery:     100 * time.Millisecond,
		FsyncRecords:   256,
		SessionTimeout: 10 * time.Second,
	}
	br, err := broker.New(cfg, quietLogger(), nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- br.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	deadline := time.Now().Add(10 * time.Second)
	for br.Addr() == "" {
		if time.Now().After(deadline) {
			b.Fatal("broker did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}

	admin := client.NewAdmin(br.Addr())
	defer admin.Close()
	if _, err := admin.CreateTopic(ctx, "bench", 3); err != nil {
		b.Fatal(err)
	}

	p := client.NewProducer(br.Addr(),
		client.WithBatching(5*time.Millisecond, 64),
		client.WithProducerLogger(quietLogger()))
	defer p.Close()

	value := make([]byte, 1024)
	if _, err := rand.Read(value); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := p.Produce(ctx, "bench", nil, value); err != nil {
				b.Fatalf("produce: %v", err)
			}
		}
	})
}
