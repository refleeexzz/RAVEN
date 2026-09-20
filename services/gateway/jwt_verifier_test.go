package gateway

import (
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
)

func mintLoginToken(t *testing.T, secret, subject string) string {
	t.Helper()
	token, err := ravenauth.MintAccessToken(secret, ravenauth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: subject},
	})
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}
	return token
}

// TestParseLoginTokenRotationWindow proves the gateway's verifier wiring: a
// login token signed with JWT_SECRET_PREVIOUS still attributes the audited
// login to the user during the rotation window, and the metric tracks the
// fallback use.
func TestParseLoginTokenRotationWindow(t *testing.T) {
	t.Parallel()

	counter := ravenauth.NewPreviousSecretUsedCounter()
	verifier, err := ravenauth.NewVerifier("gw-new-secret", "gw-retiring-secret", counter)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	s := &server{jwtVerifier: verifier}

	oldToken := mintLoginToken(t, "gw-retiring-secret", "user-before-rotation")
	claims, ok := s.parseLoginToken(oldToken)
	if !ok || claims.Subject != "user-before-rotation" {
		t.Errorf("parseLoginToken (previous-secret token): got ok=%v sub=%q, want ok=true sub=user-before-rotation",
			ok, claims.Subject)
	}
	if got := testutil.ToFloat64(counter); got != 1 {
		t.Errorf("previous-secret counter: got %v, want 1", got)
	}

	newToken := mintLoginToken(t, "gw-new-secret", "user-after-rotation")
	claims, ok = s.parseLoginToken(newToken)
	if !ok || claims.Subject != "user-after-rotation" {
		t.Errorf("parseLoginToken (primary token): got ok=%v sub=%q, want ok=true sub=user-after-rotation",
			ok, claims.Subject)
	}
	if got := testutil.ToFloat64(counter); got != 1 {
		t.Errorf("previous-secret counter after primary parse: got %v, want 1 (unchanged)", got)
	}

	stranger := mintLoginToken(t, "gw-unknown-secret", "intruder")
	if _, ok = s.parseLoginToken(stranger); ok {
		t.Error("parseLoginToken (unknown secret): got ok=true, want false")
	}
}

// TestParseLoginTokenSingleSecretFallback pins the pre-rotation behavior for
// servers built without a verifier (the audit test rig constructs the server
// struct directly): the raw jwtSecret parse keeps working, and with no
// secret at all nothing validates.
func TestParseLoginTokenSingleSecretFallback(t *testing.T) {
	t.Parallel()

	token := mintLoginToken(t, "audit-test-secret", "user-1")

	s := &server{jwtSecret: "audit-test-secret"}
	claims, ok := s.parseLoginToken(token)
	if !ok || claims.Subject != "user-1" {
		t.Errorf("parseLoginToken (raw-secret fallback): got ok=%v sub=%q, want ok=true sub=user-1",
			ok, claims.Subject)
	}

	bare := &server{}
	if _, ok = bare.parseLoginToken(token); ok {
		t.Error("parseLoginToken (no secret configured): got ok=true, want false")
	}
}
