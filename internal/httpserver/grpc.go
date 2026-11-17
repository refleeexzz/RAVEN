package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

// serve both listeners until either exits, then drain both before returning.
func ServeWithGRPC(ctx context.Context, srv *grpc.Server, lis net.Listener, httpAddr string, handler http.Handler) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var servers errgroup.Group
	servers.Go(func() error {
		defer cancel()
		err := srv.Serve(lis)
		if err == nil || (ctx.Err() != nil && errors.Is(err, grpc.ErrServerStopped)) {
			return nil
		}
		return fmt.Errorf("grpc serve: %w", err)
	})
	servers.Go(func() error {
		defer cancel()
		return ListenAndServe(ctx, httpAddr, handler)
	})

	<-ctx.Done()
	timer := time.AfterFunc(10*time.Second, srv.Stop)
	defer timer.Stop()
	srv.GracefulStop()
	return servers.Wait()
}
