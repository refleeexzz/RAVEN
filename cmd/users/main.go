// Command users is the RAVEN users service binary. It is deliberately thin:
// read env config, install signal handling, hand over to services/users.Run.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/raven/platform/internal/config"
	"github.com/raven/platform/services/users"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := users.Config{
		GRPCAddr:     config.Get("GRPC_ADDR", ":9082"),
		HTTPAddr:     config.Get("HTTP_ADDR", ":8082"),
		DatabaseURL:  config.Get("DATABASE_URL", "postgres://raven:raven@localhost:5432/raven?sslmode=disable"),
		LogLevel:     config.Get("LOG_LEVEL", "info"),
		BcryptCost:   config.GetInt("USERS_BCRYPT_COST", 12),
		OtelEndpoint: config.Get("OTEL_ENDPOINT", "localhost:4317"),
		OtelEnabled:  config.GetBool("OTEL_ENABLED", false),
	}

	if err := users.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "users: %v\n", err)
		os.Exit(1)
	}
}
