// rotation.go adds the dual-secret verification window used when rotating
// JWT_SECRET without dropping live sessions.
//
// The model is the classic primary/previous pair:
//
//   - signing ALWAYS uses the primary secret (JWT_SECRET) — new tokens must
//     stop depending on the old secret the moment rotation starts;
//   - verification tries the primary first, then the previous one
//     (JWT_SECRET_PREVIOUS) — tokens minted just before the rotation keep
//     validating until they expire naturally (≤ AccessTokenTTL, 15 min).
//
// A Verifier is immutable after construction and safe for concurrent use.
// See docs/security/rotation.md for the full operational runbook.
package auth

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// NewPreviousSecretUsedCounter returns the standard collector for the
// rotation window: raven_auth_jwt_previous_secret_used_total. It increments
// every time a token validates ONLY against the previous secret — while the
// rate is above zero, tokens signed with the old secret are still in flight
// and the rotation window must stay open. Once the rate sits at ~0 for at
// least AccessTokenTTL, JWT_SECRET_PREVIOUS can be removed.
//
// The owning service registers the returned counter in its metrics registry
// (nil is a valid argument to NewVerifier for callers without metrics).
func NewPreviousSecretUsedCounter() prometheus.Counter {
	return prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "raven",
		Subsystem: "auth",
		Name:      "jwt_previous_secret_used_total",
		Help: "Access tokens that validated only against JWT_SECRET_PREVIOUS. " +
			"Falls to zero when the JWT rotation window can be closed.",
	})
}

// Verifier validates access tokens against a primary secret and, during a
// rotation window, a previous secret. Signing always uses the primary.
type Verifier struct {
	primary  string
	previous string             // empty outside a rotation window
	prevUsed prometheus.Counter // nil disables counting
}

// NewVerifier builds a Verifier. primary is required: rotating into an empty
// secret would silently invalidate every token, so it is a hard error
// (KindInvalid, code jwt_secret_missing — same contract as MintAccessToken).
// previous may be empty (normal single-secret operation); passing previous
// equal to primary is normalized to "no window". prevUsed should come from
// NewPreviousSecretUsedCounter and may be nil.
func NewVerifier(primary, previous string, prevUsed prometheus.Counter) (*Verifier, error) {
	if primary == "" {
		return nil, errors.E(errors.KindInvalid, "jwt_secret_missing",
			"JWT primary secret must not be empty", nil)
	}
	if previous == primary {
		// Same value in both slots is not a rotation window — skipping the
		// second parse attempt keeps the hot path at one HMAC.
		previous = ""
	}
	return &Verifier{primary: primary, previous: previous, prevUsed: prevUsed}, nil
}

// MintAccessToken signs c with the PRIMARY secret — always. A Verifier never
// emits tokens that depend on the previous secret, so the window can close
// as soon as old tokens expire.
func (v *Verifier) MintAccessToken(c Claims) (string, error) {
	return MintAccessToken(v.primary, c)
}

// ParseAccessToken validates token against the primary secret first and,
// only if that fails and a previous secret is configured, against the
// previous one. A successful previous-secret parse increments the
// previous-secret-used counter.
//
// Error semantics match the package-level ParseAccessToken exactly
// (KindUnauthorized, codes token_expired / token_invalid), with one
// refinement: a token that is expired AND signed with the previous secret
// reports token_expired, so clients refresh instead of treating it as
// invalid.
func (v *Verifier) ParseAccessToken(token string) (Claims, error) {
	claims, primaryErr := ParseAccessToken(v.primary, token)
	if primaryErr == nil {
		return claims, nil
	}
	if v.previous == "" {
		return Claims{}, primaryErr
	}
	claims, prevErr := ParseAccessToken(v.previous, token)
	if prevErr != nil {
		if errors.CodeOf(prevErr) == "token_expired" {
			return Claims{}, prevErr
		}
		// The primary secret is the source of truth; surface its error.
		return Claims{}, primaryErr
	}
	if v.prevUsed != nil {
		v.prevUsed.Inc()
	}
	return claims, nil
}

// HasPrevious reports whether the verifier is inside a rotation window.
func (v *Verifier) HasPrevious() bool {
	return v.previous != ""
}
