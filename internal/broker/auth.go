package broker

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/refleeexzz/RAVEN/internal/broker/server"
)

// APIKey is one static credential, bootstrapped from the
// BROKER_API_KEYS env var (JSON array). It exists to get authentication
// running without a database; the SaaS phase replaces it with a
// DB-backed store behind the same server.Authenticator interface.
//
//	[
//	  {"id":"worker","secret_sha256":"<hex of sha256(secret)>",
//	   "topics_read":["jobs.*"],"topics_write":["jobs.*"]},
//	  {"id":"admin","secret_sha256":"...","admin":true}
//	]
//
// The plaintext secret never touches the broker: operators store only
// its SHA-256 (hex). The client sends the secret itself inside the
// AUTH frame (protect it with TLS on real networks) and the broker
// hashes and compares in constant time.
type APIKey struct {
	ID           string   `json:"id"`
	SecretSHA256 string   `json:"secret_sha256"`
	TopicsRead   []string `json:"topics_read,omitempty"`
	TopicsWrite  []string `json:"topics_write,omitempty"`
	Admin        bool     `json:"admin,omitempty"`
}

// parseAPIKeys decodes and validates the BROKER_API_KEYS JSON. Every
// entry is checked strictly — a malformed security config must fail
// the boot (fail closed), never be silently ignored.
func parseAPIKeys(raw string) ([]APIKey, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var keys []APIKey
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	seen := make(map[string]struct{}, len(keys))
	for i, k := range keys {
		if k.ID == "" {
			return nil, fmt.Errorf("key #%d: id is required", i)
		}
		if _, dup := seen[k.ID]; dup {
			return nil, fmt.Errorf("key %q: duplicate id", k.ID)
		}
		seen[k.ID] = struct{}{}
		h, err := hex.DecodeString(k.SecretSHA256)
		if err != nil || len(h) != sha256.Size {
			return nil, fmt.Errorf("key %q: secret_sha256 must be %d lowercase hex chars (sha256 of the secret)", k.ID, sha256.Size*2)
		}
		for _, pat := range append(append([]string{}, k.TopicsRead...), k.TopicsWrite...) {
			if pat == "" {
				return nil, fmt.Errorf("key %q: empty topic pattern", k.ID)
			}
		}
		if k.Admin && (len(k.TopicsRead) > 0 || len(k.TopicsWrite) > 0) {
			return nil, fmt.Errorf("key %q: admin already implies every grant; drop topics_read/topics_write", k.ID)
		}
	}
	return keys, nil
}

// staticAuthenticator implements server.Authenticator over the
// configured API keys. Secrets are compared in constant time:
// sha256(secret) vs the stored hash via subtle.ConstantTimeCompare,
// and unknown ids compare against a fixed dummy hash so the timing
// does not reveal which ids exist.
type staticAuthenticator struct {
	keys map[string]apiKeyEntry
}

type apiKeyEntry struct {
	hash      [sha256.Size]byte
	principal *server.Principal
}

// dummyHash is compared against when the id is unknown. It is the
// sha256 of a fixed, public string — its only job is to make the
// unknown-id path run the same constant-time compare as the known-id
// path.
var dummyHash = sha256.Sum256([]byte("raven-broker-auth-dummy"))

func newStaticAuthenticator(keys []APIKey) (*staticAuthenticator, error) {
	a := &staticAuthenticator{keys: make(map[string]apiKeyEntry, len(keys))}
	for _, k := range keys {
		h, err := hex.DecodeString(k.SecretSHA256)
		if err != nil || len(h) != sha256.Size {
			return nil, fmt.Errorf("key %q: bad secret_sha256", k.ID)
		}
		var hash [sha256.Size]byte
		copy(hash[:], h)
		a.keys[k.ID] = apiKeyEntry{
			hash: hash,
			principal: &server.Principal{
				ID:          k.ID,
				TopicsRead:  k.TopicsRead,
				TopicsWrite: k.TopicsWrite,
				Admin:       k.Admin,
			},
		}
	}
	return a, nil
}

// Authenticate checks id+secret. It always runs the same compare, so
// "unknown id" and "wrong secret" are indistinguishable by timing.
func (a *staticAuthenticator) Authenticate(_ context.Context, id, secret string) (*server.Principal, error) {
	sum := sha256.Sum256([]byte(secret))
	e, ok := a.keys[id]
	hash := dummyHash
	if ok {
		hash = e.hash
	}
	if subtle.ConstantTimeCompare(sum[:], hash[:]) != 1 || !ok {
		return nil, server.ErrBadCredentials
	}
	return e.principal, nil
}
