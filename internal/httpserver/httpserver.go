// Package httpserver runs an HTTP server with graceful shutdown.
// Every service gets the same behavior: serve until the context is
// cancelled (SIGTERM/SIGINT wired in main), stop accepting connections,
// drain in-flight requests with a deadline, then exit.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// ListenAndServe starts an HTTP server on addr and blocks until ctx is
// cancelled, then shuts down gracefully. It returns nil on a clean stop.
func ListenAndServe(ctx context.Context, addr string, handler http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("http server %s: %w", addr, err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http server %s shutdown: %w", addr, err)
		}
		return <-errCh
	}
}
