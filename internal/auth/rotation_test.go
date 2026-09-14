package auth

import (
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

const (
	testPrimarySecret  = "primary-secret-rotation-test"
	testPreviousSecret = "previous-secret-rotation-test"
)

func verifierClaims() Claims {
	return Claims{
		Email:            "rotate@example.com",
		Roles:            []string{RoleUser},
		RegisteredClaims: jwt.RegisteredClaims{Subject: "rotate-subject"},
	}
}

func TestNewVerifierRequiresPrimary(t *testing.T) {
	t.Parallel()

	_, err := NewVerifier("", testPreviousSecret, nil)
	if err == nil {
		t.Fatal("expected an error for empty primary secret, got nil")
	}
	if kind := errors.KindOf(err); kind != errors.KindInvalid {
		t.Errorf("kind: got %v, want KindInvalid", kind)
	}
	if code := errors.CodeOf(err); code != "jwt_secret_missing" {
		t.Errorf("code: got %q, want %q", code, "jwt_secret_missing")
	}
}

func TestVerifierParsesTokenSignedWithPrevious(t *testing.T) {
	t.Parallel()

	token, err := MintAccessToken(testPreviousSecret, verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}

	v, err := NewVerifier(testPrimarySecret, testPreviousSecret, nil)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	got, err := v.ParseAccessToken(token)
	if err != nil {
		t.Fatalf("ParseAccessToken rejected a token signed with the previous secret: %v", err)
	}
	if got.Subject != "rotate-subject" {
		t.Errorf("subject: got %q, want %q", got.Subject, "rotate-subject")
	}
}

func TestVerifierRejectsUnknownSecret(t *testing.T) {
	t.Parallel()

	token, err := MintAccessToken("some-random-secret", verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}

	v, err := NewVerifier(testPrimarySecret, testPreviousSecret, nil)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	_, err = v.ParseAccessToken(token)
	if err == nil {
		t.Fatal("ParseAccessToken accepted a token signed with an unknown secret")
	}
	if kind := errors.KindOf(err); kind != errors.KindUnauthorized {
		t.Errorf("kind: got %v, want KindUnauthorized", kind)
	}
	if code := errors.CodeOf(err); code != "token_invalid" {
		t.Errorf("code: got %q, want %q", code, "token_invalid")
	}
}

func TestVerifierSignsWithPrimaryOnly(t *testing.T) {
	t.Parallel()

	v, err := NewVerifier(testPrimarySecret, testPreviousSecret, nil)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	token, err := v.MintAccessToken(verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}

	if _, err := ParseAccessToken(testPrimarySecret, token); err != nil {
		t.Errorf("token minted by the Verifier does not validate against the primary secret: %v", err)
	}
	if _, err := ParseAccessToken(testPreviousSecret, token); err == nil {
		t.Error("token minted by the Verifier validates against the previous secret — signing must always use the primary")
	}
}

// TestVerifierRotationWindowSimulation walks the full rotation lifecycle:
// mint before rotation → rotate (old becomes previous, new becomes primary)
// → old tokens keep validating during the window → close the window → old
// tokens are rejected, tokens minted after rotation keep working.
func TestVerifierRotationWindowSimulation(t *testing.T) {
	t.Parallel()

	oldSecret := "old-secret-about-to-retire"
	newSecret := "fresh-secret-just-rotated-in"

	// Before rotation: single-secret verifier mints the tokens users hold.
	before, err := NewVerifier(oldSecret, "", nil)
	if err != nil {
		t.Fatalf("NewVerifier (before): %v", err)
	}
	preRotationToken, err := before.MintAccessToken(verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken (pre-rotation): %v", err)
	}

	// Rotation: JWT_SECRET_PREVIOUS=old, JWT_SECRET=new.
	counter := NewPreviousSecretUsedCounter()
	during, err := NewVerifier(newSecret, oldSecret, counter)
	if err != nil {
		t.Fatalf("NewVerifier (during): %v", err)
	}
	if !during.HasPrevious() {
		t.Fatal("HasPrevious: got false, want true inside a rotation window")
	}

	if _, err := during.ParseAccessToken(preRotationToken); err != nil {
		t.Fatalf("pre-rotation token rejected inside the window: %v", err)
	}
	if got := testutil.ToFloat64(counter); got != 1 {
		t.Errorf("previous-secret counter: got %v, want 1", got)
	}

	// Tokens minted after rotation use the new secret and must NOT trip the
	// previous-secret path.
	postRotationToken, err := during.MintAccessToken(verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken (post-rotation): %v", err)
	}
	if _, err := during.ParseAccessToken(postRotationToken); err != nil {
		t.Fatalf("post-rotation token rejected: %v", err)
	}
	if got := testutil.ToFloat64(counter); got != 1 {
		t.Errorf("previous-secret counter after primary parse: got %v, want 1 (unchanged)", got)
	}

	// Window closed: previous removed.
	after, err := NewVerifier(newSecret, "", counter)
	if err != nil {
		t.Fatalf("NewVerifier (after): %v", err)
	}
	if after.HasPrevious() {
		t.Fatal("HasPrevious: got true, want false after the window closed")
	}
	if _, err := after.ParseAccessToken(preRotationToken); err == nil {
		t.Error("pre-rotation token still validates after the window closed")
	}
	if _, err := after.ParseAccessToken(postRotationToken); err != nil {
		t.Errorf("post-rotation token rejected after the window closed: %v", err)
	}
	if got := testutil.ToFloat64(counter); got != 1 {
		t.Errorf("previous-secret counter after failed parses: got %v, want 1 (unchanged)", got)
	}
}

func TestVerifierExpiredPreviousTokenReportsExpired(t *testing.T) {
	t.Parallel()

	expired, err := MintAccessToken(testPreviousSecret, Claims{
		Email: "stale@example.com",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "stale-id",
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		},
	})
	if err != nil {
		t.Fatalf("MintAccessToken (expired): %v", err)
	}

	v, err := NewVerifier(testPrimarySecret, testPreviousSecret, nil)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	_, err = v.ParseAccessToken(expired)
	if err == nil {
		t.Fatal("expected an error for an expired token, got nil")
	}
	// Expired must win over invalid-signature so clients refresh instead of
	// re-authenticating.
	if code := errors.CodeOf(err); code != "token_expired" {
		t.Errorf("code: got %q, want %q", code, "token_expired")
	}
}

func TestVerifierWithoutPreviousBehavesLikeSingleSecret(t *testing.T) {
	t.Parallel()

	counter := NewPreviousSecretUsedCounter()
	v, err := NewVerifier(testPrimarySecret, "", counter)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if v.HasPrevious() {
		t.Error("HasPrevious: got true, want false with an empty previous secret")
	}

	token, err := v.MintAccessToken(verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}
	if _, err := v.ParseAccessToken(token); err != nil {
		t.Errorf("ParseAccessToken: %v", err)
	}
	if got := testutil.ToFloat64(counter); got != 0 {
		t.Errorf("previous-secret counter: got %v, want 0 without a previous secret", got)
	}
}

func TestVerifierSameSecretBothSlotsIsNotAWindow(t *testing.T) {
	t.Parallel()

	counter := NewPreviousSecretUsedCounter()
	v, err := NewVerifier(testPrimarySecret, testPrimarySecret, counter)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if v.HasPrevious() {
		t.Error("HasPrevious: got true, want false when previous == primary")
	}

	token, err := MintAccessToken(testPrimarySecret, verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}
	if _, err := v.ParseAccessToken(token); err != nil {
		t.Errorf("ParseAccessToken: %v", err)
	}
	if got := testutil.ToFloat64(counter); got != 0 {
		t.Errorf("previous-secret counter: got %v, want 0 (primary path must not count)", got)
	}
}

// TestVerifierConcurrentUse hammers Parse/Mint from many goroutines so
// `go test -race` can prove the Verifier is safe to share — services hold
// one Verifier per process and every request goroutine hits it.
func TestVerifierConcurrentUse(t *testing.T) {
	t.Parallel()

	counter := NewPreviousSecretUsedCounter()
	v, err := NewVerifier(testPrimarySecret, testPreviousSecret, counter)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	prevToken, err := MintAccessToken(testPreviousSecret, verifierClaims())
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}

	const workers = 16
	const iterations = 50

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				token, err := v.MintAccessToken(verifierClaims())
				if err != nil {
					t.Errorf("MintAccessToken: %v", err)
					return
				}
				if _, err := v.ParseAccessToken(token); err != nil {
					t.Errorf("ParseAccessToken (primary): %v", err)
					return
				}
				if _, err := v.ParseAccessToken(prevToken); err != nil {
					t.Errorf("ParseAccessToken (previous): %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got, want := testutil.ToFloat64(counter), float64(workers*iterations); got != want {
		t.Errorf("previous-secret counter: got %v, want %v", got, want)
	}
}
