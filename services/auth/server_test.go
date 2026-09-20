package auth

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"golang.org/x/crypto/bcrypt"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

func serverTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func mintServiceToken(t *testing.T, secret, subject string) string {
	t.Helper()
	token, err := ravenauth.MintAccessToken(secret, ravenauth.Claims{
		RegisteredClaims: jwt.RegisteredClaims{Subject: subject},
	})
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}
	return token
}

// TestServerWithVerifierRotationWindow proves the wiring the service Run
// path uses: a Verifier built with JWT_SECRET + JWT_SECRET_PREVIOUS keeps
// validating tokens signed with the previous secret, counts them on the
// registered rotation metric, and keeps signing with the primary only.
func TestServerWithVerifierRotationWindow(t *testing.T) {
	t.Parallel()

	m := NewServiceMetrics(metrics.New("auth-test-rotation"))
	verifier, err := ravenauth.NewVerifier("new-primary-secret", "retiring-secret", m.previousSecretUsed)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if !verifier.HasPrevious() {
		t.Fatal("HasPrevious: got false, want true inside a rotation window")
	}
	srv := NewServerWithVerifier(nil, nil, serverTestLogger(), verifier, bcrypt.MinCost, m)

	// A token minted just before the rotation (signed with the previous
	// secret) still validates — and the window metric must move.
	oldToken := mintServiceToken(t, "retiring-secret", "user-before-rotation")
	resp, err := srv.ValidateToken(context.Background(), &genauth.ValidateTokenRequest{AccessToken: oldToken})
	if err != nil {
		t.Fatalf("ValidateToken (previous-secret token): %v", err)
	}
	if !resp.GetValid() || resp.GetUserId() != "user-before-rotation" {
		t.Errorf("ValidateToken (previous-secret token): got valid=%v user=%q, want valid=true user=user-before-rotation",
			resp.GetValid(), resp.GetUserId())
	}
	if got := testutil.ToFloat64(m.previousSecretUsed); got != 1 {
		t.Errorf("previous-secret counter: got %v, want 1", got)
	}

	// Tokens signed with the primary validate without touching the metric.
	newToken := mintServiceToken(t, "new-primary-secret", "user-after-rotation")
	resp, err = srv.ValidateToken(context.Background(), &genauth.ValidateTokenRequest{AccessToken: newToken})
	if err != nil {
		t.Fatalf("ValidateToken (primary token): %v", err)
	}
	if !resp.GetValid() {
		t.Error("ValidateToken (primary token): got valid=false, want true")
	}
	if got := testutil.ToFloat64(m.previousSecretUsed); got != 1 {
		t.Errorf("previous-secret counter after primary parse: got %v, want 1 (unchanged)", got)
	}

	// A token signed with an unrelated secret is rejected.
	stranger := mintServiceToken(t, "some-other-secret", "intruder")
	resp, err = srv.ValidateToken(context.Background(), &genauth.ValidateTokenRequest{AccessToken: stranger})
	if err != nil {
		t.Fatalf("ValidateToken (unknown secret): %v", err)
	}
	if resp.GetValid() {
		t.Error("ValidateToken (unknown secret): got valid=true, want false")
	}
}

// TestNewServerSingleSecretParity pins the zero-behavior-change contract of
// the backward-compatible constructor: with no JWT_SECRET_PREVIOUS in play,
// tokens validate against the one secret and the rotation metric never
// moves.
func TestNewServerSingleSecretParity(t *testing.T) {
	t.Parallel()

	m := NewServiceMetrics(metrics.New("auth-test-single"))
	srv := NewServer(nil, nil, serverTestLogger(), "the-only-secret", bcrypt.MinCost, m)

	token := mintServiceToken(t, "the-only-secret", "user-1")
	resp, err := srv.ValidateToken(context.Background(), &genauth.ValidateTokenRequest{AccessToken: token})
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if !resp.GetValid() || resp.GetUserId() != "user-1" {
		t.Errorf("ValidateToken: got valid=%v user=%q, want valid=true user=user-1",
			resp.GetValid(), resp.GetUserId())
	}
	if got := testutil.ToFloat64(m.previousSecretUsed); got != 0 {
		t.Errorf("previous-secret counter: got %v, want 0 without a rotation window", got)
	}

	stranger := mintServiceToken(t, "wrong-secret", "user-1")
	resp, err = srv.ValidateToken(context.Background(), &genauth.ValidateTokenRequest{AccessToken: stranger})
	if err != nil {
		t.Fatalf("ValidateToken (wrong secret): %v", err)
	}
	if resp.GetValid() {
		t.Error("ValidateToken (wrong secret): got valid=true, want false")
	}
}
