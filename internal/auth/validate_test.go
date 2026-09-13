package auth

import (
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

func TestValidateEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		email    string
		wantCode string // empty means valid
	}{
		{name: "plain", email: "alice@example.com"},
		{name: "subdomain and plus", email: "a.b+tag@mail.example.co.uk"},
		{name: "empty", email: "", wantCode: "email_required"},
		{name: "whitespace only", email: "   ", wantCode: "email_required"},
		{name: "missing at", email: "alice.example.com", wantCode: "email_invalid"},
		{name: "missing domain", email: "alice@", wantCode: "email_invalid"},
		{name: "missing tld dot", email: "alice@localhost", wantCode: "email_invalid"},
		{name: "space inside", email: "ali ce@example.com", wantCode: "email_invalid"},
		{name: "too long", email: strings.Repeat("a", 250) + "@x.io", wantCode: "email_invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateEmail(tt.email)
			if tt.wantCode == "" {
				if err != nil {
					t.Fatalf("ValidateEmail(%q) = %v, want nil", tt.email, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateEmail(%q) = nil, want code %q", tt.email, tt.wantCode)
			}
			if kind := errors.KindOf(err); kind != errors.KindInvalid {
				t.Errorf("kind: got %v, want KindInvalid", kind)
			}
			if code := errors.CodeOf(err); code != tt.wantCode {
				t.Errorf("code: got %q, want %q", code, tt.wantCode)
			}
		})
	}
}

func TestValidatePassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantCode string // empty means valid
	}{
		{name: "minimum length", password: "12345678"},
		{name: "long passphrase", password: strings.Repeat("x", 72)},
		{name: "empty", password: "", wantCode: "password_required"},
		{name: "too short", password: "1234567", wantCode: "password_too_short"},
		{name: "over bcrypt limit", password: strings.Repeat("x", 73), wantCode: "password_too_long"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidatePassword(tt.password)
			if tt.wantCode == "" {
				if err != nil {
					t.Fatalf("ValidatePassword: %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidatePassword = nil, want code %q", tt.wantCode)
			}
			if code := errors.CodeOf(err); code != tt.wantCode {
				t.Errorf("code: got %q, want %q", code, tt.wantCode)
			}
		})
	}
}

func TestHashAndCheckPassword(t *testing.T) {
	t.Parallel()

	hash, err := HashPassword("correct horse battery staple", bcrypt.MinCost)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if hash == "correct horse battery staple" || !strings.HasPrefix(hash, "$2") {
		t.Fatalf("hash does not look like bcrypt: %q", hash)
	}
	if err := CheckPassword(hash, "correct horse battery staple"); err != nil {
		t.Errorf("CheckPassword with correct password: %v", err)
	}
	if err := CheckPassword(hash, "wrong password"); err == nil {
		t.Error("CheckPassword with wrong password: want error, got nil")
	}
}

func TestHashPasswordClampsLowCost(t *testing.T) {
	t.Parallel()

	hash, err := HashPassword("some password", 0)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	cost, err := bcrypt.Cost([]byte(hash))
	if err != nil {
		t.Fatalf("bcrypt.Cost: %v", err)
	}
	if cost != DefaultBcryptCost {
		t.Errorf("cost: got %d, want %d (clamped)", cost, DefaultBcryptCost)
	}
}
