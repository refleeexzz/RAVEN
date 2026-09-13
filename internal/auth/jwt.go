// Package auth holds the identity primitives shared by RAVEN services:
// JWT access-token minting/parsing, RBAC permission checks, and the input
// validation + password hashing rules that both the auth service (register/
// login) and the users service (create user) must apply identically.
package auth

import (
	stderrors "errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// AccessTokenTTL is the fixed lifetime of a minted access token. Short on
// purpose: revocation of short-lived tokens is best-effort (see the Redis
// denylist in the auth service) and refreshes are cheap.
const AccessTokenTTL = 15 * time.Minute

// Claims is the JWT payload carried by every RAVEN access token. It embeds
// the registered claims (sub, jti, iat, exp) and adds the identity fields
// downstream services need to authorise requests without a database lookup.
type Claims struct {
	Email string   `json:"email"`
	Roles []string `json:"roles,omitempty"`
	Perms []string `json:"perms,omitempty"`
	jwt.RegisteredClaims
}

// MintAccessToken signs c as an HS256 JWT. Zero-valued IssuedAt, ExpiresAt
// and ID are defaulted (now, now+AccessTokenTTL, random uuid) so tests can
// still mint deliberately broken tokens by setting them explicitly.
func MintAccessToken(secret string, c Claims) (string, error) {
	if secret == "" {
		return "", errors.E(errors.KindInvalid, "jwt_secret_missing",
			"JWT secret must not be empty", nil)
	}
	now := time.Now()
	if c.IssuedAt == nil {
		c.IssuedAt = jwt.NewNumericDate(now)
	}
	if c.ExpiresAt == nil {
		c.ExpiresAt = jwt.NewNumericDate(now.Add(AccessTokenTTL))
	}
	if c.ID == "" {
		c.ID = uuid.NewString()
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, c)
	signed, err := token.SignedString([]byte(secret))
	if err != nil {
		return "", fmt.Errorf("auth: sign access token: %w", err)
	}
	return signed, nil
}

// ParseAccessToken validates the signature, algorithm and expiry of token
// and returns its typed claims. Errors are KindUnauthorized so transports
// can map them to 401 / Unauthenticated without inspecting internals.
func ParseAccessToken(secret, token string) (Claims, error) {
	var claims Claims
	_, err := jwt.ParseWithClaims(token, &claims,
		func(t *jwt.Token) (any, error) {
			if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
			}
			return []byte(secret), nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		if stderrors.Is(err, jwt.ErrTokenExpired) {
			return Claims{}, errors.E(errors.KindUnauthorized, "token_expired",
				"access token has expired", err)
		}
		return Claims{}, errors.E(errors.KindUnauthorized, "token_invalid",
			"access token is invalid", err)
	}
	return claims, nil
}
