package raven

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Error is the typed error every API failure surfaces as. It mirrors the
// gateway's error envelope:
//
//	{"error":{"code":"job_not_found","message":"job does not exist","request_id":"0f7b2c..."}}
//
// Code is a stable snake_case machine string (e.g. "job_not_found",
// "rate_limited"). Message is client-safe prose. RequestID matches the
// request_id field in the gateway's JSON logs — quote it when reporting
// issues. StatusCode is the HTTP status of the response.
type Error struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
	RequestID  string `json:"request_id"`
}

// Error implements the error interface.
func (e *Error) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("raven: %s: %s (status %d, request %s)", e.Code, e.Message, e.StatusCode, e.RequestID)
	}
	return fmt.Sprintf("raven: %s: %s (status %d)", e.Code, e.Message, e.StatusCode)
}

// IsCode reports whether err is an *Error with the given machine code,
// walking through wrapped errors.
//
//	if raven.IsCode(err, "job_not_found") { ... }
func IsCode(err error, code string) bool {
	for err != nil {
		if e, ok := err.(*Error); ok {
			return e.Code == code
		}
		// errors.As-compatible chain: only *Error is produced by this SDK,
		// so a plain unwrap is enough.
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// errorEnvelope is the wire shape of an error response.
type errorEnvelope struct {
	Err struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

// decodeError builds an *Error from a non-2xx response body. A body that is
// not the standard envelope (a proxy error page, a truncated reply) still
// yields an *Error with code "http_<status>".
func decodeError(statusCode int, body []byte) *Error {
	var env errorEnvelope
	if err := json.Unmarshal(body, &env); err == nil && env.Err.Code != "" {
		return &Error{
			StatusCode: statusCode,
			Code:       env.Err.Code,
			Message:    env.Err.Message,
			RequestID:  env.Err.RequestID,
		}
	}
	return &Error{
		StatusCode: statusCode,
		Code:       fmt.Sprintf("http_%d", statusCode),
		Message:    http.StatusText(statusCode),
	}
}
