package users

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

func TestNormalizePage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		in           *gencommon.PageRequest
		wantPage     int
		wantPageSize int
	}{
		{name: "nil request", in: nil, wantPage: 1, wantPageSize: 20},
		{name: "zero values", in: &gencommon.PageRequest{}, wantPage: 1, wantPageSize: 20},
		{name: "explicit page", in: &gencommon.PageRequest{Page: 3, PageSize: 10}, wantPage: 3, wantPageSize: 10},
		{name: "negative page", in: &gencommon.PageRequest{Page: -2, PageSize: 10}, wantPage: 1, wantPageSize: 10},
		{name: "oversized page size capped", in: &gencommon.PageRequest{Page: 1, PageSize: 500}, wantPage: 1, wantPageSize: 100},
		{name: "exactly max", in: &gencommon.PageRequest{Page: 1, PageSize: 100}, wantPage: 1, wantPageSize: 100},
		{name: "negative page size", in: &gencommon.PageRequest{Page: 1, PageSize: -5}, wantPage: 1, wantPageSize: 20},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			page, pageSize := normalizePage(tt.in)
			if page != tt.wantPage || pageSize != tt.wantPageSize {
				t.Errorf("normalizePage = (%d, %d), want (%d, %d)",
					page, pageSize, tt.wantPage, tt.wantPageSize)
			}
		})
	}
}

func TestEmailPatternEscapesLikeMetachars(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty matches everything", in: "", want: "%%"},
		{name: "plain substring", in: "alice", want: "%alice%"},
		{name: "percent escaped", in: "100%", want: `%100\%%`},
		{name: "underscore escaped", in: "a_b", want: `%a\_b%`},
		{name: "backslash escaped", in: `a\b`, want: `%a\\b%`},
		{name: "combined", in: `_a%b\c`, want: `%\_a\%b\\c%`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := emailPattern(tt.in); got != tt.want {
				t.Errorf("emailPattern(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestToStatus(t *testing.T) {
	t.Parallel()

	// The mapper is duplicated with services/auth by design; this table
	// guards the copy from drifting.
	tests := []struct {
		name     string
		err      error
		wantCode codes.Code
	}{
		{name: "invalid", err: errors.E(errors.KindInvalid, "user_id_invalid", "user id must be a uuid", nil), wantCode: codes.InvalidArgument},
		{name: "not found", err: errors.E(errors.KindNotFound, "user_not_found", "user does not exist", nil), wantCode: codes.NotFound},
		{name: "conflict", err: errors.E(errors.KindConflict, "email_taken", "a user with this email already exists", nil), wantCode: codes.AlreadyExists},
		{name: "unknown", err: errors.E(errors.KindUnknown, "user_list_failed", "could not list users", nil), wantCode: codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st, ok := status.FromError(toStatus(tt.err))
			if !ok {
				t.Fatal("toStatus returned a non-status error")
			}
			if st.Code() != tt.wantCode {
				t.Errorf("code: got %v, want %v", st.Code(), tt.wantCode)
			}
		})
	}
}
