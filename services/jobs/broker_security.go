// broker_security.go wires the broker client's opt-in protection layers
// (docs/broker-security.md) into the jobs and worker services: API-key
// authentication and TLS/mTLS. Everything is driven by env config and the
// zero value is OPEN MODE — no AUTH frame, plaintext connection, byte-for-byte
// the pre-security behavior — so local dev and the existing test suite keep
// working untouched.
package jobs

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/refleeexzz/RAVEN/internal/broker/client"
)

// BrokerSecurityConfig carries the raw client-side broker protection knobs.
// cmd/jobs and cmd/worker fill it from the environment variables documented
// in docs/contracts/ports-and-env.md.
type BrokerSecurityConfig struct {
	// APIKeyID/APIKeySecret (BROKER_API_KEY_ID / BROKER_API_KEY_SECRET) are
	// the API key presented in the AUTH frame right after every connect.
	// Both empty = open mode (no AUTH frame). Setting only one is a config
	// error.
	APIKeyID     string
	APIKeySecret string

	// TLSCAFile (BROKER_TLS_CA_FILE) is the PEM bundle that trusts the
	// broker's certificate. Empty = plaintext. TLSServerName
	// (BROKER_TLS_SERVER_NAME) pins the expected server name when it differs
	// from the dial host (e.g. IP dial with a DNS-named cert).
	TLSCAFile     string
	TLSServerName string

	// TLSClientCertFile/TLSClientKeyFile (BROKER_TLS_CLIENT_CERT_FILE /
	// BROKER_TLS_CLIENT_KEY_FILE) present a client certificate for mTLS.
	// Both empty = plain TLS; setting only one is a config error.
	TLSClientCertFile string
	TLSClientKeyFile  string
}

// BrokerSecurity is the validated, immutable form of BrokerSecurityConfig:
// the TLS config is built once at boot and shared by every broker client the
// service creates (producer, consumers, admin, health checker). A nil
// *BrokerSecurity is valid and means open mode.
type BrokerSecurity struct {
	keyID  string
	secret string
	tlsCfg *tls.Config // nil = plaintext
	mtls   bool
}

// NewBrokerSecurity validates cfg and pre-builds the TLS config. It returns
// (nil, nil) for a completely empty config — open mode. Any inconsistency
// (half a key pair, unreadable CA, ...) is a hard error so a typo in
// security config fails the boot instead of silently running unprotected:
// the same fail-closed rule the broker applies to BROKER_API_KEYS.
func NewBrokerSecurity(cfg BrokerSecurityConfig) (*BrokerSecurity, error) {
	if cfg == (BrokerSecurityConfig{}) {
		return nil, nil // open mode
	}

	switch {
	case cfg.APIKeyID == "" && cfg.APIKeySecret != "":
		return nil, fmt.Errorf("BROKER_API_KEY_SECRET is set but BROKER_API_KEY_ID is empty: set both or neither")
	case cfg.APIKeyID != "" && cfg.APIKeySecret == "":
		return nil, fmt.Errorf("BROKER_API_KEY_ID %q is set but BROKER_API_KEY_SECRET is empty: set both or neither", cfg.APIKeyID)
	}

	switch {
	case cfg.TLSClientCertFile == "" && cfg.TLSClientKeyFile != "":
		return nil, fmt.Errorf("BROKER_TLS_CLIENT_KEY_FILE is set but BROKER_TLS_CLIENT_CERT_FILE is empty: set both or neither")
	case cfg.TLSClientCertFile != "" && cfg.TLSClientKeyFile == "":
		return nil, fmt.Errorf("BROKER_TLS_CLIENT_CERT_FILE is set but BROKER_TLS_CLIENT_KEY_FILE is empty: set both or neither")
	}
	if cfg.TLSClientCertFile != "" && cfg.TLSCAFile == "" {
		return nil, fmt.Errorf("BROKER_TLS_CLIENT_CERT_FILE requires BROKER_TLS_CA_FILE: mTLS is TLS plus a client certificate")
	}

	sec := &BrokerSecurity{keyID: cfg.APIKeyID, secret: cfg.APIKeySecret}

	if cfg.TLSCAFile != "" {
		tlsCfg, err := buildBrokerTLSConfig(cfg)
		if err != nil {
			return nil, err
		}
		sec.tlsCfg = tlsCfg
		sec.mtls = cfg.TLSClientCertFile != ""
	}
	return sec, nil
}

// buildBrokerTLSConfig loads the CA bundle and, for mTLS, the client key
// pair. The floor is TLS 1.2, mirroring the broker's own policy
// (internal/broker/tls.go).
func buildBrokerTLSConfig(cfg BrokerSecurityConfig) (*tls.Config, error) {
	pem, err := os.ReadFile(cfg.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read BROKER_TLS_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("BROKER_TLS_CA_FILE %q contains no PEM certificates", cfg.TLSCAFile)
	}
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		ServerName: cfg.TLSServerName, // empty = dial host (client default)
		MinVersion: tls.VersionTLS12,
	}
	if cfg.TLSClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSClientCertFile, cfg.TLSClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load BROKER_TLS_CLIENT_CERT_FILE/BROKER_TLS_CLIENT_KEY_FILE: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// Mode describes the effective posture in one short string for boot logs.
// Never includes the secret.
func (s *BrokerSecurity) Mode() string {
	if s == nil {
		return "open (plaintext, no auth)"
	}
	mode := "plaintext"
	if s.tlsCfg != nil && s.mtls {
		mode = "mTLS"
	} else if s.tlsCfg != nil {
		mode = "TLS"
	}
	if s.keyID != "" {
		return fmt.Sprintf("%s + api-key auth (id %q)", mode, s.keyID)
	}
	return mode + ", no auth"
}

// AuthEnabled reports whether an API key id is configured. A key id against
// an open-mode broker is harmless (the client logs and proceeds).
func (s *BrokerSecurity) AuthEnabled() bool { return s != nil && s.keyID != "" }

// TLSEnabled reports whether the connection is upgraded to TLS.
func (s *BrokerSecurity) TLSEnabled() bool { return s != nil && s.tlsCfg != nil }

// ProducerOptions maps the security settings onto client.ProducerOptions.
// Nil receiver = open mode = no options.
func (s *BrokerSecurity) ProducerOptions() []client.ProducerOption {
	if s == nil {
		return nil
	}
	var opts []client.ProducerOption
	if s.tlsCfg != nil {
		opts = append(opts, client.WithProducerTLS(s.tlsCfg))
	}
	if s.keyID != "" {
		opts = append(opts, client.WithProducerAuth(s.keyID, s.secret))
	}
	return opts
}

// ConsumerOptions maps the security settings onto client.ConsumerOptions.
// Nil receiver = open mode = no options.
func (s *BrokerSecurity) ConsumerOptions() []client.ConsumerOption {
	if s == nil {
		return nil
	}
	var opts []client.ConsumerOption
	if s.tlsCfg != nil {
		opts = append(opts, client.WithConsumerTLS(s.tlsCfg))
	}
	if s.keyID != "" {
		opts = append(opts, client.WithConsumerAuth(s.keyID, s.secret))
	}
	return opts
}

// AdminOptions maps the security settings onto client.AdminOptions. Nil
// receiver = open mode = no options. Note the broker requires an admin-flagged
// key for CreateTopic/ListTopics when auth is on.
func (s *BrokerSecurity) AdminOptions() []client.AdminOption {
	if s == nil {
		return nil
	}
	var opts []client.AdminOption
	if s.tlsCfg != nil {
		opts = append(opts, client.WithAdminTLS(s.tlsCfg))
	}
	if s.keyID != "" {
		opts = append(opts, client.WithAdminAuth(s.keyID, s.secret))
	}
	return opts
}

// ExplainBrokerError decorates the broker.s typed auth failures with an
// actionable hint, so a misconfigured key fails the boot with a readable
// message instead of a bare wire code. Open mode never produces these codes,
// so open-mode behavior is untouched.
func ExplainBrokerError(op string, err error) error {
	switch {
	case client.IsUnauthenticated(err):
		return fmt.Errorf("%s: broker rejected the API key (UNAUTHENTICATED) — "+
			"check BROKER_API_KEY_ID/BROKER_API_KEY_SECRET against the broker's BROKER_API_KEYS: %w", op, err)
	case client.IsUnauthorized(err):
		return fmt.Errorf("%s: the broker API key lacks the grant for this operation (UNAUTHORIZED) — "+
			"fix topics_read/topics_write/admin for the key id in the broker's BROKER_API_KEYS; retrying will not help: %w", op, err)
	default:
		return err
	}
}
