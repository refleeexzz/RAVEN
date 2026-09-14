package broker_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/broker/client"
	"github.com/refleeexzz/RAVEN/internal/broker/testcert"
)

// clientTLSConfig trusts the bundle CA (what a real client operator
// would pin).
func clientTLSConfig(t *testing.T, b *testcert.Bundle) *tls.Config {
	t.Helper()
	return &tls.Config{RootCAs: b.CAPool(t), ServerName: "localhost"}
}

func TestBrokerTLSProduceFetchE2E(t *testing.T) {
	t.Parallel()
	bundle := testcert.New(t)
	dir := t.TempDir()
	certFile, keyFile := bundle.WriteServerFiles(t, dir)

	b, cancel, done := startBroker(t, func(cfg *broker.Config) {
		cfg.TLSCertFile = certFile
		cfg.TLSKeyFile = keyFile
	})
	defer func() {
		cancel()
		<-done
	}()

	admin := client.NewAdmin(b.Addr(), client.WithAdminTLS(clientTLSConfig(t, bundle)))
	defer admin.Close()
	if _, err := admin.CreateTopic(context.Background(), "secure", 1); err != nil {
		t.Fatalf("create topic over TLS: %v", err)
	}

	p := client.NewProducer(b.Addr(), client.WithProducerTLS(clientTLSConfig(t, bundle)))
	defer p.Close()
	off, err := p.Produce(context.Background(), "secure", []byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("produce over TLS: %v", err)
	}
	if off != 0 {
		t.Fatalf("first offset %d, want 0", off)
	}
}

func TestBrokerTLSRejectsPlaintextClient(t *testing.T) {
	t.Parallel()
	bundle := testcert.New(t)
	dir := t.TempDir()
	certFile, keyFile := bundle.WriteServerFiles(t, dir)

	b, cancel, done := startBroker(t, func(cfg *broker.Config) {
		cfg.TLSCertFile = certFile
		cfg.TLSKeyFile = keyFile
	})
	defer func() {
		cancel()
		<-done
	}()

	// A plaintext producer can never complete a TLS handshake; with a
	// short caller context the error must surface promptly.
	admin := client.NewAdmin(b.Addr())
	defer admin.Close()
	ctx, cancelCtx := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelCtx()
	if _, err := admin.ListTopics(ctx); err == nil {
		t.Fatal("plaintext client listed topics on a TLS broker")
	}
}

func TestBrokerMTLSClientCertE2E(t *testing.T) {
	t.Parallel()
	bundle := testcert.New(t)
	dir := t.TempDir()
	certFile, keyFile := bundle.WriteServerFiles(t, dir)
	caFile := bundle.WriteClientCAFile(t, dir)

	b, cancel, done := startBroker(t, func(cfg *broker.Config) {
		cfg.TLSCertFile = certFile
		cfg.TLSKeyFile = keyFile
		cfg.TLSClientCAFile = caFile
	})
	defer func() {
		cancel()
		<-done
	}()

	// With a client certificate: full access.
	good := clientTLSConfig(t, bundle)
	good.Certificates = []tls.Certificate{bundle.ClientCert(t)}
	admin := client.NewAdmin(b.Addr(), client.WithAdminTLS(good))
	defer admin.Close()
	if _, err := admin.CreateTopic(context.Background(), "mtls", 1); err != nil {
		t.Fatalf("create topic over mTLS: %v", err)
	}

	// Without one: the handshake is rejected, no topic gets created.
	bare := client.NewAdmin(b.Addr(), client.WithAdminTLS(clientTLSConfig(t, bundle)))
	defer bare.Close()
	ctx, cancelCtx := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelCtx()
	if _, err := bare.CreateTopic(ctx, "nope", 1); err == nil {
		t.Fatal("cert-less client created a topic on an mTLS broker")
	}
}
