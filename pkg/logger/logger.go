// Package logger gives every service the same structured JSON logger.
// It is a thin wrapper over log/slog so we keep zero dependencies and
// still get production-grade output: one JSON object per line, with
// service name, level, request ID and trace ID attached.
package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// New builds a JSON slog.Logger that stamps every record with the
// service name. level accepts "debug", "info", "warn", "error".
// Unknown values fall back to info.
func New(service, level string) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseLevel(level),
	})).With(slog.String("service", service))
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type ctxKey struct{}

// WithRequestID returns a context carrying the request ID. HTTP and gRPC
// middleware put it here so any log line deep in the call stack can be
// correlated with the original request.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKey{}, id)
}

// RequestID extracts the request ID from ctx, or "" when absent.
func RequestID(ctx context.Context) string {
	if id, ok := ctx.Value(ctxKey{}).(string); ok {
		return id
	}
	return ""
}

// WithContext enriches the logger with correlation fields found in ctx
// (request ID today, trace ID once tracing middleware adds it).
func WithContext(ctx context.Context, log *slog.Logger) *slog.Logger {
	if id := RequestID(ctx); id != "" {
		log = log.With(slog.String("request_id", id))
	}
	return log
}
