// Command jobs is the RAVEN jobs service binary. It is deliberately thin:
// read env config, install signal handling, hand over to services/jobs.Run.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/services/jobs"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := jobs.Config{
		GRPCAddr:      config.Get("GRPC_ADDR", ":9083"),
		HTTPAddr:      config.Get("HTTP_ADDR", ":8083"),
		DatabaseURL:   config.Get("DATABASE_URL", "postgres://raven:raven@localhost:5432/raven?sslmode=disable"),
		RedisAddr:     config.Get("REDIS_ADDR", "localhost:6379"),
		BrokerAddr:    config.Get("BROKER_ADDR", "localhost:9100"),
		LogLevel:      config.Get("LOG_LEVEL", "info"),
		SweepInterval: config.GetDuration("JOBS_SWEEP_INTERVAL", 15*time.Second),
		OtelEndpoint:  config.Get("OTEL_ENDPOINT", "localhost:4317"),
		OtelEnabled:   config.GetBool("OTEL_ENABLED", false),
	}

	if err := jobs.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "jobs: %v\n", err)
		os.Exit(1)
	}
}
