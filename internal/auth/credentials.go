package auth

import (
	"regexp"
	"strings"

	"golang.org/x/crypto/bcrypt"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// MinPasswordLength is the platform-wide minimum for user passwords.
const MinPasswordLength = 8

// DefaultBcryptCost is the production bcrypt work factor. Tests override it
// via the AUTH_BCRYPT_COST env var, but never below MinBcryptCost.
const DefaultBcryptCost = 12

// MinBcryptCost is the security floor for the bcrypt work factor. A
// misconfigured env var (e.g. AUTH_BCRYPT_COST=5) can never push hashing
// below it. Cost 10 is still fast enough for tests (~50 ms) while staying
// outside trivially-brute-forceable territory.
const MinBcryptCost = 10

// emailRe is a deliberately simple email shape check: one @, non-empty
// local and domain parts, a dot in the domain, no whitespace. Full RFC 5322
// validation is a solved problem we do not need — citext uniqueness and a
// confirmation flow (out of scope) matter more than esoteric edge cases.
var emailRe = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

// ValidateEmail returns a KindInvalid error when email is not acceptable.
func ValidateEmail(email string) error {
	trimmed := strings.TrimSpace(email)
	if trimmed == "" {
		return errors.E(errors.KindInvalid, "email_required", "email is required", nil)
	}
	if len(trimmed) > 254 || !emailRe.MatchString(trimmed) {
		return errors.E(errors.KindInvalid, "email_invalid", "email address is not valid", nil)
	}
	return nil
}

// ValidatePassword returns a KindInvalid error when password is too weak.
func ValidatePassword(password string) error {
	if password == "" {
		return errors.E(errors.KindInvalid, "password_required", "password is required", nil)
	}
	if len(password) < MinPasswordLength {
		return errors.E(errors.KindInvalid, "password_too_short",
			"password must be at least 8 characters", nil)
	}
	// bcrypt only reads the first 72 bytes; reject longer inputs explicitly
	// instead of silently truncating, which surprises users.
	if len(password) > 72 {
		return errors.E(errors.KindInvalid, "password_too_long",
			"password must be at most 72 characters", nil)
	}
	return nil
}

// HashPassword hashes password with bcrypt at the given cost. Two clamps
// protect production from misconfiguration: costs below bcrypt.MinCost
// (unset/garbage env) fall back to DefaultBcryptCost, and costs between
// bcrypt.MinCost and MinBcryptCost (explicitly configured weak values like
// AUTH_BCRYPT_COST=5) are raised to the MinBcryptCost floor.
func HashPassword(password string, cost int) (string, error) {
	if cost < bcrypt.MinCost {
		cost = DefaultBcryptCost
	} else if cost < MinBcryptCost {
		cost = MinBcryptCost
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), cost)
	if err != nil {
		return "", errors.E(errors.KindUnknown, "password_hash_failed",
			"could not hash the password", err)
	}
	return string(hash), nil
}

// CheckPassword compares a bcrypt hash with a candidate password. It returns
// nil on match; callers should treat every non-nil result as "invalid
// credentials" and not distinguish the cause.
func CheckPassword(hash, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
}
