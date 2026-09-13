// Package errors defines the small error vocabulary shared by all RAVEN
// services. The rule: wrap with context at each layer, inspect with
// errors.Is / errors.As only at boundaries (HTTP handlers, gRPC status
// mappers, retry loops).
package errors

import (
	"errors"
	"fmt"
)

// Kind classifies an error so transports can map it to a status code
// without string matching.
type Kind int

const (
	KindUnknown      Kind = iota // 500
	KindInvalid                  // 400 — bad input, failed validation
	KindUnauthorized             // 401 — missing or bad credentials
	KindForbidden                // 403 — authenticated but not allowed
	KindNotFound                 // 404
	KindConflict                 // 409 — duplicate, version mismatch
	KindRateLimited              // 429
	KindUnavailable              // 503 — dependency down, circuit open
)

// AppError is the single error type that travels across layers.
// Message is safe to show to clients; Err keeps the internal cause.
type AppError struct {
	Kind    Kind
	Code    string // stable machine code, e.g. "user_not_found"
	Message string // human-readable, client-safe
	Err     error  // wrapped cause, never serialized to clients
}

func (e *AppError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap exposes the cause for errors.Is / errors.As.
func (e *AppError) Unwrap() error { return e.Err }

// E builds an *AppError. Pass a nil cause for leaf errors.
//
//	return errors.E(errors.KindNotFound, "user_not_found", "user does not exist", err)
func E(kind Kind, code, message string, cause error) error {
	return &AppError{Kind: kind, Code: code, Message: message, Err: cause}
}

// KindOf extracts the Kind from err, defaulting to KindUnknown.
func KindOf(err error) Kind {
	var ae *AppError
	if errors.As(err, &ae) {
		return ae.Kind
	}
	return KindUnknown
}

// CodeOf extracts the stable machine code, or "internal".
func CodeOf(err error) string {
	var ae *AppError
	if errors.As(err, &ae) {
		return ae.Code
	}
	return "internal"
}

// MessageOf extracts the client-safe message.
func MessageOf(err error) string {
	var ae *AppError
	if errors.As(err, &ae) {
		return ae.Message
	}
	return "internal error"
}
