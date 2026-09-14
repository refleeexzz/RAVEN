package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// certReloader hot-reloads the broker's TLS certificate. A background
// goroutine stats the cert/key files every interval and swaps the
// in-memory pair (guarded by an RWMutex) when either mtime changes;
// tls.Config hands the current pair out through GetCertificate, so new
// handshakes pick up a rotation without a restart. Live connections
// are untouched — they already finished their handshake.
//
// Why polling and not stat-per-handshake: handshakes are the hot path
// and two syscalls per handshake buy nothing — rotations are
// operator-paced (seconds are plenty), while a broken file at handshake
// time would fail new connections instead of just keeping the old cert.
type certReloader struct {
	certFile string
	keyFile  string
	log      *slog.Logger

	mu       sync.RWMutex
	cert     *tls.Certificate
	modTimes [2]time.Time // cert, key
}

// newCertReloader loads the initial pair (fail fast at boot: a broker
// configured for TLS must not start plaintext or start with a bad
// cert) and returns the reloader. Call run to start watching.
func newCertReloader(certFile, keyFile string, log *slog.Logger) (*certReloader, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile, log: log}
	if err := r.reload(); err != nil {
		return nil, err
	}
	return r, nil
}

// reload swaps the pair atomically. Callers outside boot must treat an
// error as "keep the current certificate".
func (r *certReloader) reload() error {
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return fmt.Errorf("load key pair: %w", err)
	}
	fiCert, err := os.Stat(r.certFile)
	if err != nil {
		return fmt.Errorf("stat cert: %w", err)
	}
	fiKey, err := os.Stat(r.keyFile)
	if err != nil {
		return fmt.Errorf("stat key: %w", err)
	}
	r.mu.Lock()
	r.cert = &cert
	r.modTimes = [2]time.Time{fiCert.ModTime(), fiKey.ModTime()}
	r.mu.Unlock()
	return nil
}

// GetCertificate is the tls.Config.GetCertificate callback.
func (r *certReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.cert == nil {
		return nil, errors.New("broker: no TLS certificate loaded")
	}
	return r.cert, nil
}

// run polls the files until ctx is done. A failed reload (half-written
// rotation, bad PEM) keeps the serving certificate and logs a warning —
// the next tick retries, so a healed rotation loads without a restart.
func (r *certReloader) run(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.mu.RLock()
			cur := r.modTimes
			r.mu.RUnlock()
			fiCert, errCert := os.Stat(r.certFile)
			fiKey, errKey := os.Stat(r.keyFile)
			if errCert != nil || errKey != nil {
				continue // files briefly gone mid-rotation; try next tick
			}
			if fiCert.ModTime().Equal(cur[0]) && fiKey.ModTime().Equal(cur[1]) {
				continue
			}
			if err := r.reload(); err != nil {
				r.log.Warn("TLS certificate reload failed; keeping the previous certificate",
					slog.String("cert_file", r.certFile), slog.Any("err", err))
				continue
			}
			r.log.Info("TLS certificate reloaded", slog.String("cert_file", r.certFile))
		}
	}
}

// buildServerTLSConfig turns the Config TLS knobs into a *tls.Config
// for the client listener. It returns (nil, nil, nil) when TLS is off,
// so the server stays plaintext by default (dev compatibility).
func buildServerTLSConfig(cfg Config, log *slog.Logger) (*tls.Config, *certReloader, error) {
	if cfg.TLSCertFile == "" && cfg.TLSKeyFile == "" {
		return nil, nil, nil
	}
	if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
		return nil, nil, errors.New("broker TLS: BROKER_TLS_CERT_FILE and BROKER_TLS_KEY_FILE must be set together")
	}
	rel, err := newCertReloader(cfg.TLSCertFile, cfg.TLSKeyFile, log)
	if err != nil {
		return nil, nil, fmt.Errorf("broker TLS: %w", err)
	}
	tc := &tls.Config{
		// TLS 1.2 floor; 1.3 is negotiated whenever the client offers it
		// (Go prefers the highest mutual version by default). TLS 1.3
		// suites are fixed by the runtime, so CipherSuites below only
		// constrains 1.2 — pinned to ECDHE AEAD suites (PFS + AEAD).
		MinVersion: tls.VersionTLS12,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
		},
		GetCertificate: rel.GetCertificate,
	}
	if cfg.TLSClientCAFile != "" {
		pem, err := os.ReadFile(cfg.TLSClientCAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("broker mTLS: read client CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, nil, fmt.Errorf("broker mTLS: %s contains no PEM certificates", cfg.TLSClientCAFile)
		}
		tc.ClientCAs = pool
		tc.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tc, rel, nil
}
