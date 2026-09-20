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
		// Broker client auth/TLS (docs/broker-security.md). Empty = open mode.
		BrokerAPIKeyID:          config.Get("BROKER_API_KEY_ID", ""),
		BrokerAPIKeySecret:      config.Get("BROKER_API_KEY_SECRET", ""),
		BrokerTLSCAFile:         config.Get("BROKER_TLS_CA_FILE", ""),
		BrokerTLSServerName:     config.Get("BROKER_TLS_SERVER_NAME", ""),
		BrokerTLSClientCertFile: config.Get("BROKER_TLS_CLIENT_CERT_FILE", ""),
		BrokerTLSClientKeyFile:  config.Get("BROKER_TLS_CLIENT_KEY_FILE", ""),
		LogLevel:                config.Get("LOG_LEVEL", "info"),
		Concurrency:             config.GetInt("WORKER_CONCURRENCY", 8),
		JobTimeout:              config.GetDuration("WORKER_JOB_TIMEOUT", 30*time.Second),
		// Milliseconds, not a Go duration string: the name says so.
		JobLease:     time.Duration(config.GetInt("WORKER_JOB_LEASE_MS", 30000)) * time.Millisecond,
		OtelEndpoint: config.Get("OTEL_ENDPOINT", "localhost:4317"),
		OtelEnabled:  config.GetBool("OTEL_ENABLED", false),
		// SSRF escape hatch (JOBS-01): dev/test only, never in production.
		WebhookAllowPrivate: config.GetBool("WORKER_WEBHOOK_ALLOW_PRIVATE", false),
	}

	if err := worker.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", err)
		os.Exit(1)
	}
}
