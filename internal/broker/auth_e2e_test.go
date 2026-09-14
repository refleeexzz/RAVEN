package broker_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
)

func sha256HexE(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func testAPIKeys() []broker.APIKey {
	return []broker.APIKey{
		{ID: "admin", SecretSHA256: sha256HexE("s3cr3t-admin"), Admin: true},
		{ID: "worker", SecretSHA256: sha256HexE("s3cr3t-worker"), TopicsRead: []string{"jobs"}, TopicsWrite: []string{"jobs"}},
	}
}

func TestBrokerAuthE2E(t *testing.T) {
	t.Parallel()
	b, cancel, done := startBroker(t, func(cfg *broker.Config) {
		cfg.APIKeys = testAPIKeys()
	})
	defer func() {
		cancel()
		<-done
	}()

	// Admin key: topic admin works.
	admin := client.NewAdmin(b.Addr(), client.WithAdminAuth("admin", "s3cr3t-admin"))
	defer admin.Close()
	if _, err := admin.CreateTopic(context.Background(), "jobs", 1); err != nil {
		t.Fatalf("admin create topic: %v", err)
	}

	// Worker key: produce works.
	p := client.NewProducer(b.Addr(), client.WithProducerAuth("worker", "s3cr3t-worker"))
	defer p.Close()
	if _, err := p.Produce(context.Background(), "jobs", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("worker produce: %v", err)
	}
}

func TestBrokerAuthRejectsBadSecret(t *testing.T) {
	t.Parallel()
	b, cancel, done := startBroker(t, func(cfg *broker.Config) {
		cfg.APIKeys = testAPIKeys()
	})
	defer func() {
		cancel()
		<-done
	}()

	// Wrong secret: typed UNAUTHENTICATED, fail fast (no retry storm).
	p := client.NewProducer(b.Addr(), client.WithProducerAuth("worker", "wrong"))
	defer p.Close()
	ctx, cancelCtx := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelCtx()
	_, err := p.Produce(ctx, "jobs", nil, []byte("v"))
	if err == nil {
		t.Fatal("produce with a bad secret succeeded")
	}
	if !client.IsUnauthenticated(err) {
		t.Fatalf("err %v (%T), want typed UNAUTHENTICATED", err, err)
	}
}

func TestBrokerAuthRejectsNoAuthClient(t *testing.T) {
	t.Parallel()
	b, cancel, done := startBroker(t, func(cfg *broker.Config) {
		cfg.APIKeys = testAPIKeys()
	})
	defer func() {
		cancel()
		<-done
	}()

	// A client without credentials gets UNAUTHENTICATED on its first
	// real op — the clear error old clients see on an auth-enabled
	// broker.
	admin := client.NewAdmin(b.Addr())
	defer admin.Close()
	_, err := admin.ListTopics(context.Background())
	if err == nil {
		t.Fatal("unauthenticated client listed topics")
	}
	if !client.IsUnauthenticated(err) {
		t.Fatalf("err %v, want typed UNAUTHENTICATED", err)
	}
}

func TestBrokerAuthOpenModeCompat(t *testing.T) {
	t.Parallel()
	// No keys: open mode. A client configured WITH auth must still work
	// (the broker answers AUTH with UNKNOWN_OPCODE, the client proceeds).
	b, cancel, done := startBroker(t, nil)
	defer func() {
		cancel()
		<-done
	}()

	admin := client.NewAdmin(b.Addr(), client.WithAdminAuth("anything", "whatever"))
	defer admin.Close()
	if _, err := admin.CreateTopic(context.Background(), "open", 1); err != nil {
		t.Fatalf("auth-configured client failed against open broker: %v", err)
	}
}

// regAdapter adapts *prometheus.Registry (Register returns error) to
// the broker's CollectorRegistrar interface.
type regAdapter struct{ r *prometheus.Registry }

func (a regAdapter) Register(cs ...prometheus.Collector) {
	for _, c := range cs {
		a.r.MustRegister(c)
	}
}

func TestBrokerAuthFailureMetric(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	cfg := broker.Config{
		TCPAddr:        "127.0.0.1:0",
		DataDir:        t.TempDir(),
		FsyncEvery:     20 * time.Millisecond,
		SessionTimeout: 5 * time.Second,
		APIKeys:        testAPIKeys(),
	}
	b, err := broker.New(cfg, quietLogger(), regAdapter{reg})
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	waitFor(t, "broker listening", 5*time.Second, func() bool { return b.Addr() != "" })

	// Three bad AUTHs, then one good op (proof the broker is up).
	p := client.NewProducer(b.Addr(), client.WithProducerAuth("worker", "wrong"))
	for i := 0; i < 3; i++ {
		cctx, ccancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = p.Produce(cctx, "jobs", nil, []byte("v"))
		ccancel()
	}
	p.Close()

	got := 0.0
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == "raven_broker_auth_failures_total" && len(mf.Metric) > 0 {
			got = mf.Metric[0].GetCounter().GetValue()
		}
	}
	if got < 3 {
		t.Fatalf("raven_broker_auth_failures_total = %v, want >= 3", got)
	}
}
