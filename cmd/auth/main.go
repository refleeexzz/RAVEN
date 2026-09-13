// Command auth is the RAVEN auth service binary. It is deliberately thin:
// read env config, install signal handling, hand over to services/auth.Run.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/raven/platform/internal/config"
	"github.com/raven/platform/services/auth"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := auth.Config{
		GRPCAddr:    config.Get("GRPC_ADDR", ":9081"),
		HTTPAddr:    config.Get("HTTP_ADDR", ":8081"),
		DatabaseURL: config.Get("DATABASE_URL", "postgres://raven:raven@localhost:5432/raven?sslmode=disable"),
		RedisAddr:   config.Get("REDIS_ADDR", "localhost:6379"),
		JWTSecret:   config.Get("JWT_SECRET", "dev-only-secret-change-me"),
		LogLevel:    config.Get("LOG_LEVEL", "info"),
		BcryptCost:  config.GetInt("AUTH_BCRYPT_COST", 12),
	}

	if err := auth.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "auth: %v\n", err)
		os.Exit(1)
	}
}
