// Package brokersvc wires the broker service: configuration from env,
// the broker itself, and the ops HTTP server (/health, /ready,
// /metrics, /topics) required by the platform contract.
package brokersvc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker"
	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// Run boots the broker and its ops server, blocking until ctx is
// cancelled (SIGINT/SIGTERM from main). Both servers shut down
// gracefully: the TCP side drains in-flight frames, flushes and fsyncs
// every partition, then closes files.
func Run(ctx context.Context) error {
	log := logger.New("broker", config.Get("LOG_LEVEL", "info"))

	cfg := broker.ConfigFromEnv()
	reg := metrics.New("broker")
	b, err := broker.New(cfg, log, reg)
	if err != nil {
		return err
	}

	healthReg := health.NewRegistry(3 * time.Second)
	healthReg.Register("storage", b.Healthy)

	mux := http.NewServeMux()
	mux.Handle("GET /health", healthReg.Liveness())
	mux.Handle("GET /ready", healthReg.Readiness())
	mux.Handle("GET /metrics", reg.Handler())
	mux.HandleFunc("GET /topics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(b.Status())
	})
	opsHandler := middleware.Chain(mux,
		middleware.RequestID,
		middleware.Logging(log),
		middleware.Recovery(log),
		reg.Middleware,
	)
	opsAddr := config.Get("HTTP_ADDR", ":9101")

	log.Info("broker starting",
		"tcp_addr", cfg.TCPAddr,
		"ops_addr", opsAddr,
		"data_dir", cfg.DataDir,
	)

	// Two blocking servers, one context. Whichever fails first ends the
	// process; a clean signal makes both return nil.
	errCh := make(chan error, 2)
	go func() { errCh <- httpserver.ListenAndServe(ctx, opsAddr, opsHandler) }()
	go func() { errCh <- b.Run(ctx) }()

	first := <-errCh
	second := <-errCh
	for _, err := range []error{first, second} {
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}
