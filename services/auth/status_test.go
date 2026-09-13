package auth

import (
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

func TestToStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
		wantMsg  string
	}{
		{
			name:     "invalid",
			err:      errors.E(errors.KindInvalid, "email_invalid", "email address is not valid", nil),
			wantCode: codes.InvalidArgument,
			wantMsg:  "email_invalid: email address is not valid",
		},
		{
			name:     "unauthorized",
			err:      errors.E(errors.KindUnauthorized, "invalid_credentials", "invalid email or password", nil),
			wantCode: codes.Unauthenticated,
			wantMsg:  "invalid_credentials: invalid email or password",
		},
		{
			name:     "forbidden",
			err:      errors.E(errors.KindForbidden, "forbidden", "not allowed", nil),
			wantCode: codes.PermissionDenied,
			wantMsg:  "forbidden: not allowed",
		},
		{
			name:     "not found",
			err:      errors.E(errors.KindNotFound, "user_not_found", "user does not exist", nil),
			wantCode: codes.NotFound,
			wantMsg:  "user_not_found: user does not exist",
		},
		{
			name:     "conflict",
			err:      errors.E(errors.KindConflict, "email_taken", "a user with this email already exists", nil),
			wantCode: codes.AlreadyExists,
			wantMsg:  "email_taken: a user with this email already exists",
		},
		{
			name:     "unavailable",
			err:      errors.E(errors.KindUnavailable, "database_unreachable", "postgres did not answer a ping", nil),
			wantCode: codes.Unavailable,
			wantMsg:  "database_unreachable: postgres did not answer a ping",
		},
		{
			name:     "wrapped cause is not leaked",
			err:      errors.E(errors.KindUnknown, "internal", "internal error", fmt.Errorf("pq: connection reset by 10.0.0.1")),
			wantCode: codes.Internal,
			wantMsg:  "internal: internal error",
		},
		{
			name:     "foreign error becomes generic internal",
			err:      fmt.Errorf("raw driver error with secrets"),
			wantCode: codes.Internal,
			wantMsg:  "internal error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := toStatus(tt.err)
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("toStatus returned a non-status error: %v", err)
			}
			if st.Code() != tt.wantCode {
				t.Errorf("code: got %v, want %v", st.Code(), tt.wantCode)
			}
			if st.Message() != tt.wantMsg {
				t.Errorf("message: got %q, want %q", st.Message(), tt.wantMsg)
			}
		})
	}

	if err := toStatus(nil); err != nil {
		t.Errorf("toStatus(nil) = %v, want nil", err)
	}
}

func TestHashRefreshTokenStable(t *testing.T) {
	t.Parallel()
	a := HashRefreshToken("some-token")
	b := HashRefreshToken("some-token")
	c := HashRefreshToken("other-token")
	if a != b {
		t.Error("same token must hash identically")
	}
	if a == c {
		t.Error("different tokens must hash differently")
	}
	if len(a) != 64 {
		t.Errorf("sha256 hex length: got %d, want 64", len(a))
	}
}

func TestNewRefreshToken(t *testing.T) {
	t.Parallel()
	tok1, hash1, err := newRefreshToken()
	if err != nil {
		t.Fatalf("newRefreshToken: %v", err)
	}
	tok2, _, err := newRefreshToken()
	if err != nil {
		t.Fatalf("newRefreshToken: %v", err)
	}
	if tok1 == tok2 {
		t.Error("two minted tokens must differ")
	}
	if hash1 != HashRefreshToken(tok1) {
		t.Error("hash must match HashRefreshToken(token)")
	}
}
