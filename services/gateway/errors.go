// errors.go maps between the three error vocabularies the gateway touches:
// pkg/errors kinds (our internal language), gRPC status codes (what the
// upstreams speak) and HTTP statuses + the JSON envelope (what clients see).
package gateway

import (
	stderrors "errors"
	"net/http"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/logger"
)

// httpStatusForKind maps a platform error kind onto an HTTP status.
func httpStatusForKind(k errors.Kind) int {
	switch k {
	case errors.KindInvalid:
		return http.StatusBadRequest
	case errors.KindUnauthorized:
		return http.StatusUnauthorized
	case errors.KindForbidden:
		return http.StatusForbidden
	case errors.KindNotFound:
		return http.StatusNotFound
	case errors.KindConflict:
		return http.StatusConflict
	case errors.KindRateLimited:
		return http.StatusTooManyRequests
	case errors.KindUnavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

// kindFromGRPCCode maps a gRPC status code back onto a platform kind. It is
// the mirror of the toStatus mapping every upstream service uses.
func kindFromGRPCCode(c codes.Code) errors.Kind {
	switch c {
	case codes.InvalidArgument:
		return errors.KindInvalid
	case codes.Unauthenticated:
		return errors.KindUnauthorized
	case codes.PermissionDenied:
		return errors.KindForbidden
	case codes.NotFound:
		return errors.KindNotFound
	case codes.AlreadyExists:
		return errors.KindConflict
	case codes.ResourceExhausted:
		return errors.KindRateLimited
	case codes.Unavailable, codes.DeadlineExceeded:
		return errors.KindUnavailable
	default:
		return errors.KindUnknown
	}
}

// defaultCodeForKind is the stable machine code used when an error does not
// carry one (plain Go errors, gRPC errors without our "<code>: msg" shape).
func defaultCodeForKind(k errors.Kind) string {
	switch k {
	case errors.KindInvalid:
		return "invalid"
	case errors.KindUnauthorized:
		return "unauthorized"
	case errors.KindForbidden:
		return "forbidden"
	case errors.KindNotFound:
		return "not_found"
	case errors.KindConflict:
		return "conflict"
	case errors.KindRateLimited:
		return "rate_limited"
	case errors.KindUnavailable:
		return "unavailable"
	default:
		return "internal"
	}
}

// defaultMessageForKind is the generic client-safe text used when an
// upstream error has no "<code>: message" prefix. Raw gRPC messages can
// contain dial errors and internal addresses, so they are never forwarded.
func defaultMessageForKind(k errors.Kind) string {
	switch k {
	case errors.KindInvalid:
		return "invalid request"
	case errors.KindUnauthorized:
		return "authentication required"
	case errors.KindForbidden:
		return "access denied"
	case errors.KindNotFound:
		return "resource not found"
	case errors.KindConflict:
		return "conflict"
	case errors.KindRateLimited:
		return "too many requests"
	case errors.KindUnavailable:
		return "service temporarily unavailable"
	default:
		return "internal error"
	}
}

// grpcMessagePrefix matches the "<code>: " prefix our services put in gRPC
// status messages (see toStatus in services/auth). Codes are snake_case.
var grpcMessagePrefix = regexp.MustCompile(`^[a-z0-9_]+: `)

// fromGRPCError converts an upstream gRPC error into an *errors.AppError,
// keeping the stable "<code>: message" pair when the upstream sent one.
func fromGRPCError(err error) *errors.AppError {
	st, ok := status.FromError(err)
	if !ok {
		return &errors.AppError{
			Kind:    errors.KindUnknown,
			Code:    "internal",
			Message: "internal error",
			Err:     err,
		}
	}
	kind := kindFromGRPCCode(st.Code())
	code := defaultCodeForKind(kind)
	message := defaultMessageForKind(kind)
	// Only messages in our "<code>: message" shape are known client-safe and
	// get forwarded; everything else keeps the generic text above.
	if loc := grpcMessagePrefix.FindString(st.Message()); loc != "" {
		if kind != errors.KindUnknown {
			code = strings.TrimSuffix(loc, ": ")
			message = strings.TrimPrefix(st.Message(), loc)
		}
	}
	return &errors.AppError{Kind: kind, Code: code, Message: message, Err: err}
}

// normalizeError turns any error the gateway produces or receives into an
// *errors.AppError. AppErrors pass through untouched; gRPC status errors get
// mapped; anything else becomes a safe 500.
func normalizeError(err error) *errors.AppError {
	var ae *errors.AppError
	if stderrors.As(err, &ae) {
		return ae
	}
	return fromGRPCError(err)
}

// errorEnvelope is the JSON body of every gateway error response:
// {"error": {"code": "...", "message": "...", "request_id": "..."}}.
type errorEnvelope struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

// writeError renders err as the JSON envelope with the right HTTP status.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	ae := normalizeError(err)
	var env errorEnvelope
	env.Error.Code = ae.Code
	env.Error.Message = ae.Message
	env.Error.RequestID = logger.RequestID(r.Context())
	writeJSON(w, httpStatusForKind(ae.Kind), env)
}
