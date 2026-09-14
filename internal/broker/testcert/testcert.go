// Package testcert generates throwaway TLS credentials for broker
// tests: a self-signed CA plus server and client certificates signed
// by it, all in memory (or written to a temp dir for hot-reload
// tests). ECDSA P-256 keys keep generation fast enough for parallel
// tests. Nothing here is suitable for production: fixed validity,
// no revocation, no CN policy.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Bundle is one CA plus the certificates it signed.
type Bundle struct {
	CACertPEM     []byte
	ServerCertPEM []byte
	ServerKeyPEM  []byte
	ClientCertPEM []byte
	ClientKeyPEM  []byte

	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
}

// New builds a CA, a server cert (SAN: localhost / 127.0.0.1) and a
// client cert, each valid for one day.
func New(t *testing.T) *Bundle {
	t.Helper()
	caKey := mustKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "raven-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	b := &Bundle{
		CACertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		caCert:    caCert,
		caKey:     caKey,
	}
	b.ServerCertPEM, b.ServerKeyPEM = b.issue(t, 2, "raven-test-server",
		x509.ExtKeyUsageServerAuth, []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	b.ClientCertPEM, b.ClientKeyPEM = b.issue(t, 3, "raven-test-client",
		x509.ExtKeyUsageClientAuth, nil, nil)
	return b
}

// issue signs one end-entity certificate with the bundle CA.
func (b *Bundle) issue(t *testing.T, serial int64, cn string, usage x509.ExtKeyUsage,
	dnsNames []string, ips []net.IP) (certPEM, keyPEM []byte) {
	t.Helper()
	key := mustKey(t)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
		DNSNames:     dnsNames,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, b.caCert, &key.PublicKey, b.caKey)
	if err != nil {
		t.Fatalf("create %s cert: %v", cn, err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal %s key: %v", cn, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func mustKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return key
}

// ServerCert parses the bundle's server certificate pair.
func (b *Bundle) ServerCert(t *testing.T) tls.Certificate {
	t.Helper()
	cert, err := tls.X509KeyPair(b.ServerCertPEM, b.ServerKeyPEM)
	if err != nil {
		t.Fatalf("parse server pair: %v", err)
	}
	return cert
}

// ClientCert parses the bundle's client certificate pair.
func (b *Bundle) ClientCert(t *testing.T) tls.Certificate {
	t.Helper()
	cert, err := tls.X509KeyPair(b.ClientCertPEM, b.ClientKeyPEM)
	if err != nil {
		t.Fatalf("parse client pair: %v", err)
	}
	return cert
}

// CAPool returns a pool trusting only the bundle CA.
func (b *Bundle) CAPool(t *testing.T) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b.CACertPEM) {
		t.Fatal("CA PEM did not parse")
	}
	return pool
}

// WriteServerFiles writes the server cert and key into dir and returns
// their paths (feeds BROKER_TLS_CERT_FILE / BROKER_TLS_KEY_FILE).
func (b *Bundle) WriteServerFiles(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, b.ServerCertPEM, 0o644); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, b.ServerKeyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

// WriteClientCAFile writes the CA PEM (feeds BROKER_TLS_CLIENT_CA_FILE).
func (b *Bundle) WriteClientCAFile(t *testing.T, dir string) string {
	t.Helper()
	p := filepath.Join(dir, "client-ca.crt")
	if err := os.WriteFile(p, b.CACertPEM, 0o644); err != nil {
		t.Fatalf("write client CA: %v", err)
	}
	return p
}

// Rotate issues a fresh server certificate (new serial and key) and
// rewrites certFile/keyFile in place, simulating an operator rotation.
// The file mtimes are bumped so mtime-based reloaders notice.
func (b *Bundle) Rotate(t *testing.T, serial int64, certFile, keyFile string) {
	t.Helper()
	certPEM, keyPEM := b.issue(t, serial, "raven-test-server",
		x509.ExtKeyUsageServerAuth, []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})
	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		t.Fatalf("rotate cert: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		t.Fatalf("rotate key: %v", err)
	}
	when := time.Now().Add(time.Duration(serial) * time.Second)
	for _, p := range []string{certFile, keyFile} {
		if err := os.Chtimes(p, when, when); err != nil {
			t.Fatalf("bump mtime: %v", err)
		}
	}
}
