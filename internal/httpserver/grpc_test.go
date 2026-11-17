package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestServeWithGRPC(t *testing.T) {
	for _, scenario := range []string{"http bind failure", "grpc listener failure", "canceled context"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()
			httpAddr := "127.0.0.1:0"
			switch scenario {
			case "http bind failure":
				httpAddr = lis.Addr().String()
			case "grpc listener failure":
				if err := lis.Close(); err != nil {
					t.Fatal(err)
				}
			case "canceled context":
				cancel()
			}

			done := make(chan error, 1)
			go func() { done <- ServeWithGRPC(ctx, grpc.NewServer(), lis, httpAddr, http.NotFoundHandler()) }()
			select {
			case err := <-done:
				switch scenario {
				case "http bind failure":
					var op *net.OpError
					if !errors.As(err, &op) || op.Op != "listen" {
						t.Fatalf("expected an http listen error, got %v", err)
					}
				case "grpc listener failure":
					if !errors.Is(err, net.ErrClosed) {
						t.Fatalf("expected a closed grpc listener, got %v", err)
					}
				case "canceled context":
					if err != nil {
						t.Fatal(err)
					}
				}
			case <-time.After(5 * time.Second):
				t.Fatal("servers did not shut down")
			}
		})
	}
}
