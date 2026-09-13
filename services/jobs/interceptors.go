package jobs

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/logger"
)

// unaryLoggingInterceptor logs one line per finished RPC. Same shape as the
// auth service interceptors; kept per-service so each binary stays
// self-contained.
func unaryLoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.WithContext(ctx, log).Debug("grpc call",
			slog.String("method", info.FullMethod),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return resp, err
	}
}

// unaryRecoveryInterceptor converts panics into Internal errors.
func unaryRecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("grpc panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", rec),
				)
				err = toStatus(errors.E(errors.KindUnknown, "internal", "internal error", nil))
			}
		}()
		return handler(ctx, req)
	}
}
