package gateway

import (
	"encoding/json"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/raven/platform/pkg/errors"
	"github.com/raven/platform/pkg/logger"
)

// TestKindToHTTPStatus pins the kind → HTTP status mapping from the spec.
func TestKindToHTTPStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind errors.Kind
		want int
	}{
		{errors.KindInvalid, http.StatusBadRequest},
		{errors.KindUnauthorized, http.StatusUnauthorized},
		{errors.KindForbidden, http.StatusForbidden},
		{errors.KindNotFound, http.StatusNotFound},
		{errors.KindConflict, http.StatusConflict},
		{errors.KindRateLimited, http.StatusTooManyRequests},
		{errors.KindUnavailable, http.StatusServiceUnavailable},
		{errors.KindUnknown, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		if got := httpStatusForKind(tt.kind); got != tt.want {
			t.Errorf("kind %d: got HTTP %d, want %d", tt.kind, got, tt.want)
		}
	}
}

// TestGRPCCodeToKindRoundtrip proves that the gRPC status mapping upstreams
// use (services/auth toStatus) round-trips back to the same kind here.
func TestGRPCCodeToKindRoundtrip(t *testing.T) {
	t.Parallel()

	// kind ↔ its canonical gRPC code, mirroring toStatus in services/auth.
	tests := []struct {
		kind errors.Kind
		code codes.Code
	}{
		{errors.KindInvalid, codes.InvalidArgument},
		{errors.KindUnauthorized, codes.Unauthenticated},
		{errors.KindForbidden, codes.PermissionDenied},
		{errors.KindNotFound, codes.NotFound},
		{errors.KindConflict, codes.AlreadyExists},
		{errors.KindRateLimited, codes.ResourceExhausted},
		{errors.KindUnavailable, codes.Unavailable},
	}
	for _, tt := range tests {
		if got := kindFromGRPCCode(tt.code); got != tt.kind {
			t.Errorf("code %v: got kind %v, want %v", tt.code, got, tt.kind)
		}
		// Full roundtrip: kind → canonical HTTP status, code → same kind.
		if httpStatusForKind(kindFromGRPCCode(tt.code)) != httpStatusForKind(tt.kind) {
			t.Errorf("kind %v did not survive the gRPC roundtrip", tt.kind)
		}
	}

	// Codes outside the mapping collapse to Unknown → 500.
	for _, c := range []codes.Code{codes.Internal, codes.Unknown, codes.DataLoss, codes.Unimplemented} {
		if got := kindFromGRPCCode(c); got != errors.KindUnknown {
			t.Errorf("code %v: got kind %v, want Unknown", c, got)
		}
	}
	// A deadline talking to an upstream looks "unavailable" to the client.
	if got := kindFromGRPCCode(codes.DeadlineExceeded); got != errors.KindUnavailable {
		t.Errorf("DeadlineExceeded: got kind %v, want Unavailable", got)
	}
}

// TestFromGRPCErrorKeepsCodeAndMessage checks the "<code>: message" parsing.
func TestFromGRPCErrorKeepsCodeAndMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		wantKind    errors.Kind
		wantCode    string
		wantMessage string
	}{
		{
			name:        "prefixed message keeps stable code",
			err:         status.Error(codes.NotFound, "user_not_found: user does not exist"),
			wantKind:    errors.KindNotFound,
			wantCode:    "user_not_found",
			wantMessage: "user does not exist",
		},
		{
			name:        "unprefixed message gets the default code and safe text",
			err:         status.Error(codes.Unavailable, "connection refused"),
			wantKind:    errors.KindUnavailable,
			wantCode:    "unavailable",
			wantMessage: "service temporarily unavailable",
		},
		{
			name:        "internal errors never leak their message",
			err:         status.Error(codes.Internal, "pq: duplicate key violates constraint"),
			wantKind:    errors.KindUnknown,
			wantCode:    "internal",
			wantMessage: "internal error",
		},
		{
			name:        "plain go error is a safe 500",
			err:         stderrors.New("boom"),
			wantKind:    errors.KindUnknown,
			wantCode:    "internal",
			wantMessage: "internal error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ae := fromGRPCError(tt.err)
			if ae.Kind != tt.wantKind {
				t.Errorf("kind: got %v, want %v", ae.Kind, tt.wantKind)
			}
			if ae.Code != tt.wantCode {
				t.Errorf("code: got %q, want %q", ae.Code, tt.wantCode)
			}
			if ae.Message != tt.wantMessage {
				t.Errorf("message: got %q, want %q", ae.Message, tt.wantMessage)
			}
		})
	}
}

// TestNormalizeErrorPassthrough: AppErrors (e.g. from the breaker) pass
// through untouched.
func TestNormalizeErrorPassthrough(t *testing.T) {
	t.Parallel()

	orig := errors.E(errors.KindRateLimited, "rate_limited", "slow down", nil)
	got := normalizeError(orig)
	if got.Kind != errors.KindRateLimited || got.Code != "rate_limited" || got.Message != "slow down" {
		t.Errorf("normalizeError mangled the AppError: %+v", got)
	}
}

// TestWriteErrorEnvelope pins the wire shape:
// {"error":{"code","message","request_id"}} with the right HTTP status.
func TestWriteErrorEnvelope(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	req = req.WithContext(logger.WithRequestID(req.Context(), "req-123"))
	rec := httptest.NewRecorder()

	writeError(rec, req, errors.E(errors.KindForbidden, "forbidden", "missing permission users:read", nil))

	if rec.Code != http.StatusForbidden {
		t.Errorf("status: got %d, want 403", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q, want application/json", ct)
	}

	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("envelope is not the documented shape: %v (body: %s)", err, rec.Body.String())
	}
	if env.Error.Code != "forbidden" {
		t.Errorf("code: got %q, want forbidden", env.Error.Code)
	}
	if env.Error.Message != "missing permission users:read" {
		t.Errorf("message: got %q", env.Error.Message)
	}
	if env.Error.RequestID != "req-123" {
		t.Errorf("request_id: got %q, want req-123", env.Error.RequestID)
	}
}

// TestWriteErrorFromGRPCNotFound is the full path: an upstream gRPC error
// becomes a 404 envelope with the upstream's stable code.
func TestWriteErrorFromGRPCNotFound(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/api/users/123", nil)
	rec := httptest.NewRecorder()

	writeError(rec, req, status.Error(codes.NotFound, "user_not_found: user does not exist"))

	if rec.Code != http.StatusNotFound {
		t.Errorf("status: got %d, want 404", rec.Code)
	}
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Error.Code != "user_not_found" || env.Error.Message != "user does not exist" {
		t.Errorf("envelope: got %+v", env.Error)
	}
}
