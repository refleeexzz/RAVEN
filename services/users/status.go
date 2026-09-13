package users

import (
	stderrors "errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/raven/platform/pkg/errors"
)

// toStatus maps the platform error vocabulary onto gRPC status codes. The
// client-visible message is "<code>: <message>"; the wrapped cause is never
// exposed. Unknown errors become Internal with a generic message.
//
// This intentionally duplicates the tiny mapper in services/auth: the
// mapping is a per-service transport concern, and 25 lines of duplication
// beats a shared package that couples services together.
func toStatus(err error) error {
	if err == nil {
		return nil
	}

	code := codes.Internal
	switch errors.KindOf(err) {
	case errors.KindInvalid:
		code = codes.InvalidArgument
	case errors.KindUnauthorized:
		code = codes.Unauthenticated
	case errors.KindForbidden:
		code = codes.PermissionDenied
	case errors.KindNotFound:
		code = codes.NotFound
	case errors.KindConflict:
		code = codes.AlreadyExists
	case errors.KindRateLimited:
		code = codes.ResourceExhausted
	case errors.KindUnavailable:
		code = codes.Unavailable
	case errors.KindUnknown:
		code = codes.Internal
	}

	var ae *errors.AppError
	if stderrors.As(err, &ae) {
		return status.Error(code, ae.Code+": "+ae.Message)
	}
	return status.Error(codes.Internal, "internal error")
}
