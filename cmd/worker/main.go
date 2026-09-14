// Command worker is the RAVEN worker service binary. It is deliberately
// thin: read env config, install signal handling, hand over to
// services/worker.Run.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/services/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := worker.Config{
		HTTPAddr:    config.Get("HTTP_ADDR", ":8085"),
		DatabaseURL: config.Get("DATABASE_URL", "postgres://raven:raven@localhost:5432/raven?sslmode=disable"),
		RedisAddr:   config.Get("REDIS_ADDR", "localhost:6379"),
		BrokerAddr:  config.Get("BROKER_ADDR", "localhost:9100"),
		LogLevel:    config.Get("LOG_LEVEL", "info"),
		Concurrency: config.GetInt("WORKER_CONCURRENCY", 8),
		JobTimeout:  config.GetDuration("WORKER_JOB_TIMEOUT", 30*time.Second),
		// Milliseconds, not a Go duration string: the name says so.
		JobLease:     time.Duration(config.GetInt("WORKER_JOB_LEASE_MS", 30000)) * time.Millisecond,
		OtelEndpoint: config.Get("OTEL_ENDPOINT", "localhost:4317"),
		OtelEnabled:  config.GetBool("OTEL_ENABLED", false),
	}

	if err := worker.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		os.Exit(1)
	}
}
