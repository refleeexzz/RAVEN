// grpc_client.go owns the client side of the gateway↔upstreams boundary:
// one shared connection per upstream, per-call timeouts, bounded retries for
// idempotent RPCs, circuit breaking and the upstream latency histogram.
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

// Resilience parameters, fixed by the gateway spec:
//   - every upstream call gets a 5 s timeout;
//   - only idempotent RPCs (Get/List/Validate) retry, only on Unavailable,
//     at most 2 retries with 100 ms then 250 ms backoff;
//   - the circuit breaker opens after 5 consecutive transport failures.
const (
	upstreamCallTimeout = 5 * time.Second
	upstreamMaxRetries  = 2
)

// retryBackoff is the delay before retry number attempt (0-based): the first
// retry waits 100 ms, the second 250 ms.
func retryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return 100 * time.Millisecond
	}
	return 250 * time.Millisecond
}

// upstream is a dialled backend service with its own circuit breaker.
type upstream struct {
	name    string
	conn    *grpc.ClientConn
	breaker *breaker
	log     *slog.Logger
	metrics *serviceMetrics
}

// dialUpstream builds the shared connection to one backend. The dial is
// lazy (the connection opens on first use), so this only fails on a bad
// address; readiness is reported by the /ready checker instead.
func dialUpstream(name, addr string, log *slog.Logger, m *serviceMetrics) (*upstream, error) {
	if addr == "" {
		return nil, fmt.Errorf("gateway: %s grpc address is empty", name)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return nil, fmt.Errorf("gateway: dial %s at %s: %w", name, addr, err)
	}

	u := &upstream{name: name, conn: conn, log: log, metrics: m}
	u.breaker = newBreaker(name, breakerThreshold, breakerOpenFor, u.onBreakerChange)
	u.metrics.breakerState.WithLabelValues(name).Set(float64(breakerClosed))
	return u, nil
}

// onBreakerChange logs every breaker transition and exports it to the gauge.
func (u *upstream) onBreakerChange(name string, from, to breakerState) {
	u.log.Warn("circuit breaker state change",
		slog.String("upstream", name),
		slog.String("from", from.String()),
		slog.String("to", to.String()),
	)
	u.metrics.breakerState.WithLabelValues(name).Set(float64(to))
}

// breakerFailure reports whether a gRPC result says the upstream is
// unreachable/sick (counts against the breaker) as opposed to an
// application-level answer like NotFound (the upstream is alive).
func breakerFailure(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}

// call runs fn against the upstream with the per-attempt timeout, retrying
// idempotent RPCs on Unavailable and routing everything through the circuit
// breaker. rpc is the short method name ("GetUser") used in metrics.
func (u *upstream) call(ctx context.Context, rpc string, idempotent bool, fn func(ctx context.Context) error) error {
	// Forward the authenticated identity to upstreams. The jobs service
	// reads x-user-id to set the job owner (used for owner-targeted
	// websocket notifications) and x-user-perms for the admin:* bypass;
	// other services simply ignore them. API-key requests forward the key's
	// scopes here exactly like a JWT forwards its permissions.
	if id, ok := IdentityFrom(ctx); ok && id.UserID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "x-user-id", id.UserID)
		if len(id.Perms) > 0 {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-user-perms", strings.Join(id.Perms, ","))
		}
	}

	for attempt := 0; ; attempt++ {
		if err := u.breaker.before(); err != nil {
			return err
		}

		callCtx, cancel := context.WithTimeout(ctx, upstreamCallTimeout)
		start := time.Now()
		err := fn(callCtx)
		cancel()
		code := status.Code(err)
		u.metrics.upstreamDuration.WithLabelValues(u.name, rpc, code.String()).
			Observe(time.Since(start).Seconds())

		if err == nil {
			u.breaker.after(true)
			return nil
		}
		if ctx.Err() != nil {
			// The caller went away (client disconnect, request timeout). Do
			// not blame the upstream and do not retry — nobody is listening.
			return err
		}
		// Application errors prove the upstream answered: success for the
		// breaker. Transport failures count against it.
		u.breaker.after(!breakerFailure(code))

		if !idempotent || code != codes.Unavailable || attempt >= upstreamMaxRetries {
			return err
		}

		delay := retryBackoff(attempt)
		u.log.Debug("retrying upstream call",
			slog.String("upstream", u.name),
			slog.String("rpc", rpc),
			slog.Int("attempt", attempt+1),
			slog.Duration("backoff", delay),
		)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
}
