// Command websocket is RAVEN's realtime edge: it terminates WebSocket
// connections (proxied by the gateway at /ws), groups them into rooms and
// scales horizontally through Redis pub/sub fanout. Ports and env vars are
// fixed by docs/contracts/ports-and-env.md: public HTTP+WS on :8084, Redis
// at REDIS_ADDR.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/refleeexzz/RAVEN/internal/config"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/httpserver"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	ws "github.com/refleeexzz/RAVEN/services/websocket"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "websocket: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logger.New("websocket", config.Get("LOG_LEVEL", "info"))

	reg := metrics.New("websocket")
	wm := ws.NewMetrics()
	reg.Register(wm.Collectors()...)

	hub := ws.NewHub(log, wm)

	rdb := redis.NewClient(&redis.Options{
		Addr: config.Get("REDIS_ADDR", "localhost:6379"),
	})
	defer func() { _ = rdb.Close() }()

	fanout := ws.NewFanout(rdb, hub, log)
	presence := ws.NewPresence(rdb, hub, fanout.NodeID(), log)

	healthReg := health.NewRegistry(2 * time.Second)
	healthReg.Register("redis", func(ctx context.Context) error {
		return rdb.Ping(ctx).Err()
	})

	handler := ws.NewHandler(ws.HandlerConfig{
		Hub:            hub,
		Fanout:         fanout,
		Presence:       presence,
		JWTSecret:      config.Get("JWT_SECRET", "dev-only-secret-change-me"),
		AllowAnonymous: config.GetBool("WS_ALLOW_ANONYMOUS", false),
		AllowedOrigins: splitCSV(config.Get("WS_ALLOWED_ORIGINS", "")),
		Logger:         log,
		Metrics:        reg,
		Health:         healthReg,
	})

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return fanout.Run(gctx) })
	g.Go(func() error {
		addr := config.Get("HTTP_ADDR", ":8084")
		log.Info("websocket service listening", slog.String("addr", addr))
		return httpserver.ListenAndServe(gctx, addr, handler)
	})

	// Blocks until SIGINT/SIGTERM (or a startup failure): the HTTP server
	// drains and the Redis subscriptions close, then g.Wait returns.
	err := g.Wait()

	// Drain WebSocket clients: every conn gets a close frame and its pumps
	// exit, bounded by a 10s deadline.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	hub.Shutdown(shutdownCtx)

	return err
}

// splitCSV parses the comma-separated WS_ALLOWED_ORIGINS list.
func splitCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
