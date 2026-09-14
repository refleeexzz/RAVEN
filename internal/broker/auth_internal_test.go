package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/server"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestParseAPIKeysValid(t *testing.T) {
	t.Parallel()
	raw := fmt.Sprintf(`[
		{"id":"worker","secret_sha256":%q,"topics_read":["jobs"],"topics_write":["jobs"]},
		{"id":"reader","secret_sha256":%q,"topics_read":["*"]},
		{"id":"admin","secret_sha256":%q,"admin":true}
	]`, sha256Hex("w"), sha256Hex("r"), sha256Hex("a"))
	keys, err := parseAPIKeys(raw)
	if err != nil {
		t.Fatalf("parseAPIKeys: %v", err)
	}
	if len(keys) != 3 || keys[0].ID != "worker" || !keys[2].Admin {
		t.Fatalf("parsed keys wrong: %+v", keys)
	}
}

func TestParseAPIKeysEmptyMeansOpenMode(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", "   ", "\n\t"} {
		keys, err := parseAPIKeys(raw)
		if err != nil || keys != nil {
			t.Fatalf("empty input %q: got (%v, %v), want (nil, nil)", raw, keys, err)
		}
	}
}

func TestParseAPIKeysFailsClosed(t *testing.T) {
	t.Parallel()
	valid := sha256Hex("x")
	cases := map[string]string{
		"not json":        `{`,
		"missing id":      fmt.Sprintf(`[{"secret_sha256":%q}]`, valid),
		"duplicate id":    fmt.Sprintf(`[{"id":"a","secret_sha256":%q},{"id":"a","secret_sha256":%q}]`, valid, valid),
		"bad hex":         `[{"id":"a","secret_sha256":"zz"}]`,
		"short hash":      `[{"id":"a","secret_sha256":"abcd"}]`,
		"empty pattern":   fmt.Sprintf(`[{"id":"a","secret_sha256":%q,"topics_read":[""]}]`, valid),
		"admin w/ topics": fmt.Sprintf(`[{"id":"a","secret_sha256":%q,"admin":true,"topics_read":["jobs"]}]`, valid),
	}
	for name, raw := range cases {
		if _, err := parseAPIKeys(raw); err == nil {
			t.Fatalf("%s: accepted, want error", name)
		}
	}
}

func TestStaticAuthenticator(t *testing.T) {
	t.Parallel()
	keys := []APIKey{
		{ID: "worker", SecretSHA256: sha256Hex("s3cr3t"), TopicsRead: []string{"jobs"}, TopicsWrite: []string{"jobs"}},
		{ID: "admin", SecretSHA256: sha256Hex("adm1n"), Admin: true},
	}
	a, err := newStaticAuthenticator(keys)
	if err != nil {
		t.Fatalf("newStaticAuthenticator: %v", err)
	}

	p, err := a.Authenticate(context.Background(), "worker", "s3cr3t")
	if err != nil || p == nil {
		t.Fatalf("good creds rejected: %v", err)
	}
	if p.ID != "worker" || len(p.TopicsWrite) != 1 || p.Admin {
		t.Fatalf("principal wrong: %+v", p)
	}

	for _, c := range [][2]string{{"worker", "wrong"}, {"nobody", "s3cr3t"}, {"admin", "s3cr3t"}} {
		p, err := a.Authenticate(context.Background(), c[0], c[1])
		if p != nil || !errors.Is(err, server.ErrBadCredentials) {
			t.Fatalf("%v: got (%v, %v), want (nil, ErrBadCredentials)", c, p, err)
		}
	}
}

// TestAuthenticateConstantTimeShape is a coarse timing smoke test: the
// unknown-id path must not shortcut the sha256+compare work (an early
// return would leak which key ids exist). The bound is deliberately
// generous (20x on medians) — it catches structural early-returns, not
// nanoseconds, and cannot flake on a loaded CI box.
func TestAuthenticateConstantTimeShape(t *testing.T) {
	t.Parallel()
	keys := []APIKey{{ID: "worker", SecretSHA256: sha256Hex("s3cr3t")}}
	a, err := newStaticAuthenticator(keys)
	if err != nil {
		t.Fatal(err)
	}
	median := func(id, secret string) time.Duration {
		ds := make([]time.Duration, 0, 101)
		for i := 0; i < 101; i++ {
			start := time.Now()
			_, _ = a.Authenticate(context.Background(), id, secret)
			ds = append(ds, time.Since(start))
		}
		return ds[len(ds)/2]
	}
	known := median("worker", "wrong-secret")
	unknown := median("nobody", "wrong-secret")
	if known > 0 && (unknown > 20*known || known > 20*unknown) {
		t.Fatalf("timing skew suggests an early return: known-id %v, unknown-id %v", known, unknown)
	}
}

func TestConfigFromEnvAPIKeysFailClosed(t *testing.T) {
	// Not parallel: env manipulation.
	t.Setenv("BROKER_API_KEYS", `[{"id":"a","secret_sha256":"not-hex"}]`)
	cfg := ConfigFromEnv()
	if cfg.apiKeysErr == nil {
		t.Fatal("invalid BROKER_API_KEYS did not record an error")
	}
	if _, err := New(cfg, quietLog(), nil); err == nil {
		t.Fatal("broker booted with an invalid BROKER_API_KEYS (must fail closed)")
	}
}

func TestConfigFromEnvAPIKeysValid(t *testing.T) {
	t.Setenv("BROKER_API_KEYS", fmt.Sprintf(`[{"id":"w","secret_sha256":%q,"topics_write":["*"]}]`, sha256Hex("s")))
	cfg := ConfigFromEnv()
	if cfg.apiKeysErr != nil {
		t.Fatalf("valid BROKER_API_KEYS recorded error: %v", cfg.apiKeysErr)
	}
	if len(cfg.APIKeys) != 1 || cfg.APIKeys[0].ID != "w" {
		t.Fatalf("keys not loaded: %+v", cfg.APIKeys)
	}
}
