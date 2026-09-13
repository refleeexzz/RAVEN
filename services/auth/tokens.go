package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"time"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// RefreshTokenTTL is the lifetime of a refresh session.
const RefreshTokenTTL = 7 * 24 * time.Hour

// refreshTokenBytes is the entropy of a freshly minted refresh token.
const refreshTokenBytes = 32

// newRefreshToken returns a fresh opaque refresh token and the sha256 hash
// that is stored in the sessions table. Only the hash ever touches the
// database, so a database leak does not leak usable tokens.
func newRefreshToken() (token, tokenHash string, err error) {
	buf := make([]byte, refreshTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", "", errors.E(errors.KindUnknown, "token_entropy_failed",
			"could not generate a refresh token", err)
	}
	token = base64.RawURLEncoding.EncodeToString(buf)
	return token, HashRefreshToken(token), nil
}

// HashRefreshToken computes the at-rest form of a refresh token.
func HashRefreshToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}
