package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

const testSecret = "test-secret-do-not-use-in-prod"

func TestMintParseRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		claims Claims
	}{
		{
			name: "full claims",
			claims: Claims{
				Email: "alice@example.com",
				Roles: []string{RoleUser},
				Perms: []string{PermUsersRead, PermJobsCreate},
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "018f3d3c-9d5a-7c1e-b2a0-9f0e1d2c3b4a",
				},
			},
		},
		{
			name: "no roles or perms",
			claims: Claims{
				Email: "bob@example.com",
				RegisteredClaims: jwt.RegisteredClaims{
					Subject: "bob-id",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			token, err := MintAccessToken(testSecret, tt.claims)
			if err != nil {
				t.Fatalf("MintAccessToken: %v", err)
			}

			got, err := ParseAccessToken(testSecret, token)
			if err != nil {
				t.Fatalf("ParseAccessToken: %v", err)
			}

			if got.Subject != tt.claims.Subject {
				t.Errorf("subject: got %q, want %q", got.Subject, tt.claims.Subject)
			}
			if got.Email != tt.claims.Email {
				t.Errorf("email: got %q, want %q", got.Email, tt.claims.Email)
			}
			if got.ID == "" {
				t.Error("jti: expected a generated id, got empty")
			}
			if got.ExpiresAt == nil || time.Until(got.ExpiresAt.Time) <= 0 {
				t.Error("exp: expected a future expiry")
			}
			if ttl := time.Until(got.ExpiresAt.Time); ttl > AccessTokenTTL {
				t.Errorf("exp: ttl %v exceeds AccessTokenTTL %v", ttl, AccessTokenTTL)
			}
			if len(got.Roles) != len(tt.claims.Roles) {
				t.Errorf("roles: got %v, want %v", got.Roles, tt.claims.Roles)
			}
			if len(got.Perms) != len(tt.claims.Perms) {
				t.Errorf("perms: got %v, want %v", got.Perms, tt.claims.Perms)
			}
		})
	}
}

func TestParseAccessTokenRejects(t *testing.T) {
	t.Parallel()

	valid, err := MintAccessToken(testSecret, Claims{
		Email:            "carol@example.com",
		RegisteredClaims: jwt.RegisteredClaims{Subject: "carol-id"},
	})
	if err != nil {
		t.Fatalf("MintAccessToken: %v", err)
	}

	expired, err := MintAccessToken(testSecret, Claims{
		Email: "dave@example.com",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "dave-id",
			IssuedAt:  jwt.NewNumericDate(time.Now().Add(-time.Hour)),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)),
		},
	})
	if err != nil {
		t.Fatalf("MintAccessToken (expired): %v", err)
	}

	parts := strings.Split(valid, ".")
	tampered := parts[0] + "." + parts[1] + ".AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

	tests := []struct {
		name     string
		secret   string
		token    string
		wantCode string
	}{
		{name: "expired token", secret: testSecret, token: expired, wantCode: "token_expired"},
		{name: "wrong secret", secret: "another-secret", token: valid, wantCode: "token_invalid"},
		{name: "tampered signature", secret: testSecret, token: tampered, wantCode: "token_invalid"},
		{name: "garbage token", secret: testSecret, token: "not-a-jwt", wantCode: "token_invalid"},
		{name: "empty token", secret: testSecret, token: "", wantCode: "token_invalid"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ParseAccessToken(tt.secret, tt.token)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if kind := errors.KindOf(err); kind != errors.KindUnauthorized {
				t.Errorf("kind: got %v, want KindUnauthorized", kind)
			}
			if code := errors.CodeOf(err); code != tt.wantCode {
				t.Errorf("code: got %q, want %q", code, tt.wantCode)
			}
		})
	}
}

func TestMintAccessTokenRequiresSecret(t *testing.T) {
	t.Parallel()
	if _, err := MintAccessToken("", Claims{}); err == nil {
		t.Fatal("expected an error for empty secret, got nil")
	}
}
