package jobs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// writeTestCertPair generates a self-signed ECDSA certificate and writes
// cert.pem/key.pem into dir, returning both paths. Good enough to exercise
// the TLS-config file loading; no handshake happens here.
func writeTestCertPair(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "raven-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certFile = filepath.Join(dir, "ca.crt")
	keyFile = filepath.Join(dir, "ca.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// TestBrokerSecurityOpenMode pins the compatibility contract: an empty
// config yields a nil BrokerSecurity whose every method is a no-op, so
// services without any BROKER_* env behave exactly as before.
func TestBrokerSecurityOpenMode(t *testing.T) {
	t.Parallel()

	sec, err := NewBrokerSecurity(BrokerSecurityConfig{})
	if err != nil {
		t.Fatalf("NewBrokerSecurity (empty): %v", err)
	}
	if sec != nil {
		t.Fatalf("NewBrokerSecurity (empty): got %v, want nil (open mode)", sec)
	}

	// Nil receiver = open mode everywhere.
	if sec.AuthEnabled() {
		t.Error("AuthEnabled: got true, want false in open mode")
	}
	if sec.TLSEnabled() {
		t.Error("TLSEnabled: got true, want false in open mode")
	}
	if got := sec.ProducerOptions(); len(got) != 0 {
		t.Errorf("ProducerOptions: got %d options, want 0 in open mode", len(got))
	}
	if got := sec.ConsumerOptions(); len(got) != 0 {
		t.Errorf("ConsumerOptions: got %d options, want 0 in open mode", len(got))
	}
	if got := sec.AdminOptions(); len(got) != 0 {
		t.Errorf("AdminOptions: got %d options, want 0 in open mode", len(got))
	}
	if mode := sec.Mode(); !strings.Contains(mode, "open") {
		t.Errorf("Mode: got %q, want an open-mode description", mode)
	}
}

// TestBrokerSecurityValidation runs the misconfiguration matrix: every
// half-specified knob must be a hard boot error (fail closed), never a
// silent downgrade.
func TestBrokerSecurityValidation(t *testing.T) {
	t.Parallel()

	certFile, keyFile := writeTestCertPair(t, t.TempDir())

	cases := []struct {
		name string
		cfg  BrokerSecurityConfig
	}{
		{"secret without id", BrokerSecurityConfig{APIKeySecret: "s3cr3t"}},
		{"id without secret", BrokerSecurityConfig{APIKeyID: "worker"}},
		{"client key without cert", BrokerSecurityConfig{TLSCAFile: certFile, TLSClientKeyFile: keyFile}},
		{"client cert without key", BrokerSecurityConfig{TLSCAFile: certFile, TLSClientCertFile: certFile}},
		{"client cert without CA", BrokerSecurityConfig{TLSClientCertFile: certFile, TLSClientKeyFile: keyFile}},
		{"missing CA file", BrokerSecurityConfig{TLSCAFile: filepath.Join(t.TempDir(), "nope.crt")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sec, err := NewBrokerSecurity(tc.cfg)
			if err == nil {
				t.Fatalf("NewBrokerSecurity (%s): got nil error (sec=%v), want a config error", tc.name, sec)
			}
		})
	}

	t.Run("CA file without PEM", func(t *testing.T) {
		t.Parallel()
		bad := filepath.Join(t.TempDir(), "bad.crt")
		if err := os.WriteFile(bad, []byte("not a pem bundle"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewBrokerSecurity(BrokerSecurityConfig{TLSCAFile: bad}); err == nil {
			t.Fatal("NewBrokerSecurity (unparseable CA): got nil error, want a config error")
		}
	})
}

// TestBrokerSecurityOptions checks the happy paths: auth-only, TLS-only and
// auth+mTLS map onto the right client options, and Mode never leaks the
// secret.
func TestBrokerSecurityOptions(t *testing.T) {
	t.Parallel()

	authOnly, err := NewBrokerSecurity(BrokerSecurityConfig{APIKeyID: "worker", APIKeySecret: "s3cr3t-worker"})
	if err != nil {
		t.Fatalf("NewBrokerSecurity (auth only): %v", err)
	}
	if !authOnly.AuthEnabled() || authOnly.TLSEnabled() {
		t.Errorf("auth only: AuthEnabled=%v TLSEnabled=%v, want true/false", authOnly.AuthEnabled(), authOnly.TLSEnabled())
	}
	if got := authOnly.ProducerOptions(); len(got) != 1 {
		t.Errorf("auth only ProducerOptions: got %d, want 1", len(got))
	}
	if got := authOnly.ConsumerOptions(); len(got) != 1 {
		t.Errorf("auth only ConsumerOptions: got %d, want 1", len(got))
	}
	if got := authOnly.AdminOptions(); len(got) != 1 {
		t.Errorf("auth only AdminOptions: got %d, want 1", len(got))
	}
	if mode := authOnly.Mode(); !strings.Contains(mode, `"worker"`) || strings.Contains(mode, "s3cr3t") {
		t.Errorf("auth only Mode: got %q, want key id present and secret absent", mode)
	}

	certFile, keyFile := writeTestCertPair(t, t.TempDir())

	tlsOnly, err := NewBrokerSecurity(BrokerSecurityConfig{TLSCAFile: certFile, TLSServerName: "broker.internal"})
	if err != nil {
		t.Fatalf("NewBrokerSecurity (TLS only): %v", err)
	}
	if tlsOnly.AuthEnabled() || !tlsOnly.TLSEnabled() {
		t.Errorf("TLS only: AuthEnabled=%v TLSEnabled=%v, want false/true", tlsOnly.AuthEnabled(), tlsOnly.TLSEnabled())
	}
	if got := tlsOnly.ProducerOptions(); len(got) != 1 {
		t.Errorf("TLS only ProducerOptions: got %d, want 1", len(got))
	}

	full, err := NewBrokerSecurity(BrokerSecurityConfig{
		APIKeyID: "worker", APIKeySecret: "s3cr3t-worker",
		TLSCAFile: certFile, TLSClientCertFile: certFile, TLSClientKeyFile: keyFile,
	})
	if err != nil {
		t.Fatalf("NewBrokerSecurity (auth + mTLS): %v", err)
	}
	if got := full.ProducerOptions(); len(got) != 2 {
		t.Errorf("auth+mTLS ProducerOptions: got %d, want 2 (TLS + auth)", len(got))
	}
	if got := full.ConsumerOptions(); len(got) != 2 {
		t.Errorf("auth+mTLS ConsumerOptions: got %d, want 2 (TLS + auth)", len(got))
	}
	if got := full.AdminOptions(); len(got) != 2 {
		t.Errorf("auth+mTLS AdminOptions: got %d, want 2 (TLS + auth)", len(got))
	}
	if mode := full.Mode(); !strings.Contains(mode, "mTLS") {
		t.Errorf("auth+mTLS Mode: got %q, want mTLS mentioned", mode)
	}
}

// TestExplainBrokerError pins the boot-log contract: the broker's typed auth
// failures become actionable messages naming the env vars to fix, while
// unrelated errors pass through untouched.
func TestExplainBrokerError(t *testing.T) {
	t.Parallel()

	unauth := ExplainBrokerError("ensure topics", &protocol.Error{Code: protocol.CodeUnauthenticated, Message: "bad credentials"})
	if unauth == nil || !strings.Contains(unauth.Error(), "UNAUTHENTICATED") ||
		!strings.Contains(unauth.Error(), "BROKER_API_KEY_ID") {
		t.Errorf("unauthenticated: got %v, want UNAUTHENTICATED + env var hint", unauth)
	}

	denied := ExplainBrokerError("ensure topics", &protocol.Error{Code: protocol.CodeUnauthorized, Message: "no admin grant"})
	if denied == nil || !strings.Contains(denied.Error(), "UNAUTHORIZED") ||
		!strings.Contains(denied.Error(), "BROKER_API_KEYS") {
		t.Errorf("unauthorized: got %v, want UNAUTHORIZED + grants hint", denied)
	}

	other := &protocol.Error{Code: protocol.CodeTopicExists, Message: "exists"}
	if got := ExplainBrokerError("ensure topics", other); got != other {
		t.Errorf("passthrough: got %v, want the original error", got)
	}
}
