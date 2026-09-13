// Command gateway is the RAVEN API gateway binary. It is deliberately thin:
// read env config, install signal handling, hand over to
// services/gateway.Run.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/raven/platform/internal/config"
	"github.com/raven/platform/services/gateway"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := gateway.Config{
		HTTPAddr:       config.Get("HTTP_ADDR", ":8080"),
		AuthGRPCAddr:   config.Get("AUTH_GRPC_ADDR", "localhost:9081"),
		UsersGRPCAddr:  config.Get("USERS_GRPC_ADDR", "localhost:9082"),
		JobsGRPCAddr:   config.Get("JOBS_GRPC_ADDR", "localhost:9083"),
		WSAddr:         config.Get("WS_ADDR", "http://localhost:8084"),
		RedisAddr:      config.Get("REDIS_ADDR", "localhost:6379"),
		JWTSecret:      config.Get("JWT_SECRET", "dev-only-secret-change-me"),
		LogLevel:       config.Get("LOG_LEVEL", "info"),
		OtelEnabled:    config.GetBool("OTEL_ENABLED", false),
		OtelEndpoint:   config.Get("OTEL_ENDPOINT", "localhost:4317"),
		RateLimitRPM:   config.GetInt("RATE_LIMIT_RPM", 100),
		RateLimitBurst: config.GetInt("RATE_LIMIT_BURST", 20),
	}

	if err := gateway.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}
