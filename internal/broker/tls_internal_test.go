package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/testcert"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func leafSerial(t *testing.T, cert *tls.Certificate) int64 {
	t.Helper()
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	return leaf.SerialNumber.Int64()
}

func TestCertReloaderServesRotatedCert(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := testcert.New(t)
	certFile, keyFile := b.WriteServerFiles(t, dir)

	rel, err := newCertReloader(certFile, keyFile, quietLog())
	if err != nil {
		t.Fatalf("newCertReloader: %v", err)
	}
	cert, err := rel.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if got := leafSerial(t, cert); got != 2 {
		t.Fatalf("initial serial %d, want 2", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rel.run(ctx, 20*time.Millisecond)

	// Rotate on disk; the reloader must swap the serving cert without a
	// restart.
	b.Rotate(t, 42, certFile, keyFile)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cert, err = rel.GetCertificate(nil)
		if err != nil {
			t.Fatalf("GetCertificate: %v", err)
		}
		if leafSerial(t, cert) == 42 {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("reloader never picked up the rotated certificate")
}

func TestCertReloaderKeepsOldCertOnBadRotation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := testcert.New(t)
	certFile, keyFile := b.WriteServerFiles(t, dir)

	rel, err := newCertReloader(certFile, keyFile, quietLog())
	if err != nil {
		t.Fatalf("newCertReloader: %v", err)
	}

	// A half-written rotation (garbage cert, bumped mtime) must not
	// replace the serving certificate.
	if err := os.WriteFile(certFile, []byte("not a pem"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(certFile, time.Now().Add(time.Hour), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rel.run(ctx, 20*time.Millisecond)
	time.Sleep(150 * time.Millisecond) // a few ticks

	cert, err := rel.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	if got := leafSerial(t, cert); got != 2 {
		t.Fatalf("serving serial %d after bad rotation, want 2 (unchanged)", got)
	}
}

func TestBuildServerTLSConfigOffByDefault(t *testing.T) {
	t.Parallel()
	cfg, rel, err := buildServerTLSConfig(Config{}, quietLog())
	if err != nil || cfg != nil || rel != nil {
		t.Fatalf("TLS off: got (%v, %v, %v), want all nil", cfg, rel, err)
	}
}

func TestBuildServerTLSConfigRequiresPair(t *testing.T) {
	t.Parallel()
	if _, _, err := buildServerTLSConfig(Config{TLSCertFile: "only.crt"}, quietLog()); err == nil {
		t.Fatal("cert without key must fail")
	}
	if _, _, err := buildServerTLSConfig(Config{TLSKeyFile: "only.key"}, quietLog()); err == nil {
		t.Fatal("key without cert must fail")
	}
}

func TestBuildServerTLSConfigPolicy(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := testcert.New(t)
	certFile, keyFile := b.WriteServerFiles(t, dir)
	caFile := b.WriteClientCAFile(t, dir)

	cfg, rel, err := buildServerTLSConfig(Config{
		TLSCertFile: certFile, TLSKeyFile: keyFile, TLSClientCAFile: caFile,
	}, quietLog())
	if err != nil {
		t.Fatalf("buildServerTLSConfig: %v", err)
	}
	if rel == nil {
		t.Fatal("reloader is nil with TLS on")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion 0x%04x, want TLS 1.2 floor", cfg.MinVersion)
	}
	if cfg.GetCertificate == nil {
		t.Fatal("GetCertificate not wired to the reloader")
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Fatalf("ClientAuth %v, want RequireAndVerifyClientCert with a client CA", cfg.ClientAuth)
	}
	if cfg.ClientCAs == nil {
		t.Fatal("ClientCAs pool missing")
	}
}

func TestBuildServerTLSConfigBadFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	b := testcert.New(t)
	certFile, keyFile := b.WriteServerFiles(t, dir)

	if _, _, err := buildServerTLSConfig(Config{TLSCertFile: certFile, TLSKeyFile: "missing.key"}, quietLog()); err == nil {
		t.Fatal("missing key file must fail the boot")
	}
	// A client CA file with no PEM certificates must fail the boot —
	// silently starting without mTLS would be the worst outcome.
	garbage := filepath.Join(dir, "garbage-ca.crt")
	if err := os.WriteFile(garbage, []byte("not a pem"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildServerTLSConfig(Config{
		TLSCertFile: certFile, TLSKeyFile: keyFile, TLSClientCAFile: garbage,
	}, quietLog()); err == nil {
		t.Fatal("client CA file without PEM certificates must fail the boot")
	}
}
