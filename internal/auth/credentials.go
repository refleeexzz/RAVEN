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
// (bcrypt.MinCost) via the AUTH_BCRYPT_COST env var to stay fast.
const DefaultBcryptCost = 12

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

// HashPassword hashes password with bcrypt at the given cost. Costs below
// bcrypt.MinCost are clamped up to DefaultBcryptCost so a misconfigured env
// var can never weaken production hashing.
func HashPassword(password string, cost int) (string, error) {
	if cost < bcrypt.MinCost {
		cost = DefaultBcryptCost
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
