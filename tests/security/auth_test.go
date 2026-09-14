// Package security holds the pre-release offensive-security regression
// suite for the RAVEN identity layer (services/auth, services/users,
// internal/auth). Every test here attacks a real code path and asserts the
// attack fails. This file needs no infrastructure and runs in the default
// go test ./...; tests that need Postgres/Redis live in
// auth_integration_test.go behind the integration build tag.
//
// Findings are registered in docs/security/auth.md (AUTH-xx).
package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/proto"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	authsvc "github.com/refleeexzz/RAVEN/services/auth"
)

const authSecSecret = "security-suite-secret-not-for-prod"

// authSecLog keeps the suite output clean; servers under attack log warnings
// by design (reuse detection, fail-open denylist).
func authSecLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newAuthSecServer builds an auth Server with no Postgres and no Redis. Only
// the pure-token RPCs (ValidateToken, RevokeToken) are safe to call on it.
func newAuthSecServer(t *testing.T) *authsvc.Server {
	t.Helper()
	m := authsvc.NewServiceMetrics(metrics.New("auth-sec-unit"))
	return authsvc.NewServer(nil, nil, authSecLog(), authSecSecret, bcrypt.MinCost, m)
}

// ---------------------------------------------------------------------------
// JWT algorithm confusion (AUTH-05: tested, not vulnerable)
// ---------------------------------------------------------------------------

// craftAuthSecJWT builds a JWT by hand so we can produce shapes the jwt library
// refuses to mint (alg=none, missing exp).
func craftAuthSecJWT(t *testing.T, header, payload map[string]any, sign func(signingString string) string) string {
	t.Helper()
	rawHeader, err := json.Marshal(header)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	h := base64.RawURLEncoding.EncodeToString(rawHeader)
	p := base64.RawURLEncoding.EncodeToString(rawPayload)
	return h + "." + p + "." + sign(h+"."+p)
}

func TestSecure_JWTAlgorithmConfusion(t *testing.T) {
	t.Parallel()
	now := time.Now()
	payload := map[string]any{
		"sub":   "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a",
		"email": "attacker@example.com",
		"roles": []string{"ADMIN"},
		"perms": []string{"admin:*"},
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	}

	// alg=none: no signature at all.
	algNone := craftAuthSecJWT(t, map[string]any{"alg": "none", "typ": "JWT"}, payload,
		func(string) string { return "" })

	// alg=RS256: signed with a real RSA key the attacker controls. The
	// classic confusion attack hopes the server verifies RS256 using its
	// HMAC secret as the RSA public key.
	rsaKey, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	rsToken, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"sub": "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a", "roles": []string{"ADMIN"},
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(),
	}).SignedString(rsaKey)
	if err != nil {
		t.Fatalf("sign RS256: %v", err)
	}

	// alg=ES256: same idea with ECDSA.
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	esToken, err := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"sub": "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a", "roles": []string{"ADMIN"},
		"exp": now.Add(time.Hour).Unix(), "iat": now.Unix(),
	}).SignedString(ecKey)
	if err != nil {
		t.Fatalf("sign ES256: %v", err)
	}

	// alg=HS384: HMAC family but not the pinned HS256 — must be rejected
	// even when signed with the real secret.
	hs384, err := jwt.NewWithClaims(jwt.SigningMethodHS384, jwt.MapClaims{
		"sub": "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a", "exp": now.Add(time.Hour).Unix(),
	}).SignedString([]byte(authSecSecret))
	if err != nil {
		t.Fatalf("sign HS384: %v", err)
	}

	tokens := map[string]string{
		"alg=none": algNone,
		"alg=RS256 (attacker key, HMAC secret as public key)": rsToken,
		"alg=ES256":                  esToken,
		"alg=HS384 (HMAC downgrade)": hs384,
	}
	for name, tok := range tokens {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := ravenauth.ParseAccessToken(authSecSecret, tok); err == nil {
				t.Fatal("ParseAccessToken accepted a forged token")
			} else if errors.KindOf(err) != errors.KindUnauthorized {
				t.Errorf("kind: got %v, want KindUnauthorized", errors.KindOf(err))
			}

			// Service level: ValidateToken must report the forgery as
			// invalid (not error, not valid).
			resp, err := newAuthSecServer(t).ValidateToken(t.Context(),
				&genauth.ValidateTokenRequest{AccessToken: tok})
			if err != nil {
				t.Fatalf("ValidateToken: %v", err)
			}
			if resp.GetValid() {
				t.Error("ValidateToken accepted a forged token")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// JWT claim tampering + expiration (AUTH-05: tested, not vulnerable)
// ---------------------------------------------------------------------------

func TestSecure_JWTClaimTampering(t *testing.T) {
	t.Parallel()

	valid, err := ravenauth.MintAccessToken(authSecSecret, ravenauth.Claims{
		Email: "user@example.com",
		Roles: []string{ravenauth.RoleUser},
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a",
		},
	})
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}
	parts := strings.Split(valid, ".")

	// Swap in an admin payload but keep the original signature.
	forgedPayload := base64.RawURLEncoding.EncodeToString([]byte(
		`{"email":"user@example.com","roles":["ADMIN"],"perms":["admin:*"],` +
			`"sub":"018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a","exp":9999999999}`))
	tampered := parts[0] + "." + forgedPayload + "." + parts[2]

	if _, err := ravenauth.ParseAccessToken(authSecSecret, tampered); err == nil {
		t.Fatal("ParseAccessToken accepted a token with a tampered payload")
	}

	// Re-signing the forged payload with a guessed/common secret must fail
	// as long as the real secret is not guessable.
	resigned, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub": "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a", "perms": []string{"admin:*"},
		"exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte("dev-only-secret-change-me"))
	if err != nil {
		t.Fatalf("sign forged: %v", err)
	}
	if _, err := ravenauth.ParseAccessToken(authSecSecret, resigned); err == nil {
		t.Fatal("ParseAccessToken accepted a token signed with the wrong secret")
	}
}

func TestSecure_JWTExpirationBypass(t *testing.T) {
	t.Parallel()
	now := time.Now()

	// exp missing entirely (WithExpirationRequired must reject).
	noExp := craftAuthSecJWT(t,
		map[string]any{"alg": "HS256", "typ": "JWT"},
		map[string]any{"sub": "x", "iat": now.Unix()},
		func(signingString string) string {
			tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "x", "iat": now.Unix()})
			signed, err := tok.SignedString([]byte(authSecSecret))
			if err != nil {
				t.Fatalf("sign no-exp: %v", err)
			}
			return strings.Split(signed, ".")[2]
		})
	if _, err := ravenauth.ParseAccessToken(authSecSecret, noExp); err == nil {
		t.Fatal("ParseAccessToken accepted a token without exp")
	}

	// exp in the past.
	expired, err := ravenauth.MintAccessToken(authSecSecret, ravenauth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "x",
			IssuedAt:  jwt.NewNumericDate(now.Add(-2 * time.Hour)),
			ExpiresAt: jwt.NewNumericDate(now.Add(-time.Hour)),
		},
	})
	if err != nil {
		t.Fatalf("MintAccessToken expired: %v", err)
	}
	if _, err := ravenauth.ParseAccessToken(authSecSecret, expired); err == nil {
		t.Fatal("ParseAccessToken accepted an expired token")
	} else if errors.CodeOf(err) != "token_expired" {
		t.Errorf("code: got %q, want token_expired", errors.CodeOf(err))
	}

	// A token minted 1 second ago must still validate — no clock-skew
	// false positives on fresh tokens (jwt/v5 applies no leeway).
	fresh, err := ravenauth.MintAccessToken(authSecSecret, ravenauth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "x"},
	})
	if err != nil {
		t.Fatalf("MintAccessToken fresh: %v", err)
	}
	if _, err := ravenauth.ParseAccessToken(authSecSecret, fresh); err != nil {
		t.Errorf("fresh token rejected: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Weak password hashing (AUTH-03: REAL FINDING — cost floor)
// ---------------------------------------------------------------------------

// TestSecure_BcryptCostFloor pins the contract that no configured cost —
// including AUTH_BCRYPT_COST values like 4..9 — can produce hashes weaker
// than the production floor of 10. Zero/negative (unset/garbage env) still
// falls back to the production default of 12.
func TestSecure_BcryptCostFloor(t *testing.T) {
	t.Parallel()

	cases := []struct {
		configured int
		wantCost   int
	}{
		{configured: 0, wantCost: ravenauth.DefaultBcryptCost},  // unset env
		{configured: -5, wantCost: ravenauth.DefaultBcryptCost}, // garbage env
		{configured: bcrypt.MinCost, wantCost: 10},              // 4: test override
		{configured: 5, wantCost: 10},                           // weak misconfig
		{configured: 9, wantCost: 10},                           // weak misconfig
		{configured: 10, wantCost: 10},                          // floor, unchanged
		{configured: 12, wantCost: 12},                          // default, unchanged
	}
	for _, tc := range cases {
		hash, err := ravenauth.HashPassword("password-123", tc.configured)
		if err != nil {
			t.Fatalf("HashPassword(cost=%d): %v", tc.configured, err)
		}
		got, err := bcrypt.Cost([]byte(hash))
		if err != nil {
			t.Fatalf("bcrypt.Cost: %v", err)
		}
		if got != tc.wantCost {
			t.Errorf("configured cost %d produced hash cost %d, want %d",
				tc.configured, got, tc.wantCost)
		}
		if got < 10 {
			t.Errorf("configured cost %d produced a hash weaker than the floor (cost %d)",
				tc.configured, got)
		}
	}
}

// ---------------------------------------------------------------------------
// Refresh token entropy (AUTH-06: tested, not vulnerable)
// ---------------------------------------------------------------------------

// TestSecure_RefreshTokenHashing pins the at-rest contract: the database
// only ever sees sha256(token) as hex, never the token itself. Entropy and
// uniqueness of freshly minted tokens are asserted over the wire in the
// integration suite (TestSecure_RefreshRotationConcurrency).
func TestSecure_RefreshTokenHashing(t *testing.T) {
	t.Parallel()

	tok := strings.Repeat("a", 43) // shape of a real 32-byte base64url token
	h1 := authsvc.HashRefreshToken(tok)
	h2 := authsvc.HashRefreshToken(tok)
	if h1 != h2 {
		t.Error("hash must be deterministic")
	}
	if len(h1) != 64 {
		t.Errorf("hash: got %d hex chars, want 64 (sha256)", len(h1))
	}
	if strings.Contains(h1, tok) {
		t.Error("hash must not contain the token")
	}
	// A one-bit difference in the token must flip the whole hash (no
	// prefix/extension games that would let an attacker narrow the space).
	if authsvc.HashRefreshToken(strings.Repeat("a", 42)+"b") == h1 {
		t.Error("near-identical tokens must hash differently")
	}
}

// ---------------------------------------------------------------------------
// Mass assignment surface (AUTH-07: not vulnerable by construction)
// ---------------------------------------------------------------------------

// TestSecure_ProtoMassAssignmentSurface pins the request messages to exactly
// their intended fields. If anyone ever adds roles/id/created_at/deleted to
// a public request message, this test screams before the audit does.
func TestSecure_ProtoMassAssignmentSurface(t *testing.T) {
	t.Parallel()

	want := map[proto.Message][]string{
		&genauth.RegisterRequest{}:    {"email", "password", "display_name"},
		&genauth.LoginRequest{}:       {"email", "password"},
		&genauth.RefreshRequest{}:     {"refresh_token"},
		&genusers.CreateUserRequest{}: {"email", "password", "display_name"},
		&genusers.UpdateUserRequest{}: {"id", "display_name", "bio", "avatar_url"},
		&genusers.DeleteUserRequest{}: {"id"},
	}
	banned := map[string]bool{
		"roles": true, "permissions": true, "perms": true,
		"created_at": true, "updated_at": true, "deleted": true, "deleted_at": true,
		"password_hash": true, "is_admin": true,
	}
	for msg, fields := range want {
		name := string(msg.ProtoReflect().Descriptor().FullName())
		fd := msg.ProtoReflect().Descriptor().Fields()
		var got []string
		for i := range fd.Len() {
			got = append(got, string(fd.Get(i).Name()))
		}
		if len(got) != len(fields) {
			t.Errorf("%s fields: got %v, want exactly %v", name, got, fields)
			continue
		}
		for i, f := range fields {
			if got[i] != f {
				t.Errorf("%s field %d: got %q, want %q", name, i, got[i], f)
			}
			if banned[got[i]] {
				t.Errorf("%s exposes mass-assignable field %q", name, got[i])
			}
		}
	}

	// proto3 decoding drops unknown fields, so even if a client sends
	// {"roles":["ADMIN"]} it cannot land in the message — there is no field
	// to land in. This test guards the contract going forward.
}

// ---------------------------------------------------------------------------
// Revocation denylist fail-open (AUTH-08: documented trade-off, behavior pinned)
// ---------------------------------------------------------------------------

// TestSecure_DenylistFailOpen documents the accepted trade-off: when Redis
// is down, ValidateToken accepts cryptographically valid tokens (bounded by
// the 15-minute access-token TTL) instead of taking the platform down.
func TestSecure_DenylistFailOpen(t *testing.T) {
	t.Parallel()

	tok, err := ravenauth.MintAccessToken(authSecSecret, ravenauth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user-1"},
	})
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}

	// Redis client pointed at a closed port: every command errors fast.
	dead := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: 0})
	defer dead.Close()
	m := authsvc.NewServiceMetrics(metrics.New("auth-sec-failopen"))
	srv := authsvc.NewServer(nil, dead, authSecLog(), authSecSecret, bcrypt.MinCost, m)

	resp, err := srv.ValidateToken(t.Context(), &genauth.ValidateTokenRequest{AccessToken: tok})
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if !resp.GetValid() {
		t.Error("fail-open contract broken: valid token rejected while Redis is down")
	}
}

// TestSecure_RevokeWithoutRedis pins the degraded-mode contract: revocation
// without Redis reports success (no-op) instead of erroring — the exposure
// window is the remaining token TTL, documented in AUTH-08.
func TestSecure_RevokeWithoutRedis(t *testing.T) {
	t.Parallel()

	tok, err := ravenauth.MintAccessToken(authSecSecret, ravenauth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: "user-1"},
	})
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}
	resp, err := newAuthSecServer(t).RevokeToken(t.Context(),
		&genauth.RevokeTokenRequest{AccessToken: tok})
	if err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
	if !resp.GetOk() {
		t.Error("RevokeToken without Redis must be a successful no-op")
	}
}
