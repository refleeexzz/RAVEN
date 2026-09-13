// healthagg.go implements GET /api/health/services — one public endpoint the
// console's health grid can poll instead of probing each service directly
// (most ops ports are ClusterIP-only and unreachable from a browser).
//
// Every probe is a REAL reachability check with a short timeout, run in
// parallel: gRPC upstreams get a cheap RPC through the same breaker path as
// real traffic, broker/websocket get an HTTP GET on their ops port, and the
// worker pool is counted from the Redis registry.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
)

// serviceStatus is one row of the console health grid.
type serviceStatus struct {
	Name      string `json:"name"`
	Status    string `json:"status"` // ok | degraded | down
	LatencyMs int64  `json:"latency_ms"`
	Detail    string `json:"detail,omitempty"`
}

// healthPayload is the response body of GET /api/health/services.
type healthPayload struct {
	CheckedAt time.Time       `json:"checked_at"`
	Services  []serviceStatus `json:"services"`
}

// prober checks one service and decides its status directly.
type prober func(ctx context.Context) (status, detail string)

// probeTimeout bounds each individual probe; probes run in parallel so the
// handler itself stays fast even when several services are down.
const probeTimeout = 1500 * time.Millisecond

// healthOrder fixes the display order of the grid.
var healthOrder = []string{"gateway", "auth", "users", "jobs", "broker", "websocket", "worker_pool"}

// healthAgg bundles the probers. probeFns is a field (not package state) so
// tests can inject fakes without touching globals.
type healthAgg struct {
	probers map[string]prober
	log     *slog.Logger
}

// newHealthAggregator builds the real probers from live dependencies.
func newHealthAggregator(cfg Config, authUp, usersUp, jobsUp *upstream, rdb *redis.Client, log *slog.Logger) *healthAgg {
	// grpcAlive probes through the upstream's normal call path (breaker +
	// timeout + metrics): if the breaker is open or the service unreachable,
	// the probe reports down — the same truth real traffic sees.
	grpcAlive := func(u *upstream, rpc string, call func(ctx context.Context) error) prober {
		return func(ctx context.Context) (string, string) {
			err := u.call(ctx, rpc, true, call)
			if err == nil {
				return "ok", "reachable"
			}
			// Application-level answers (Unauthenticated, InvalidArgument...)
			// prove the service answered; only transport failures mean down.
			switch status.Code(err) {
			case codes.Unauthenticated, codes.InvalidArgument, codes.NotFound,
				codes.PermissionDenied, codes.AlreadyExists:
				return "ok", "reachable"
			default:
				return "down", "unreachable"
			}
		}
	}

	httpGet := func(addr, path string, detail func(body []byte) string) prober {
		return func(ctx context.Context) (string, string) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+path, nil)
			if err != nil {
				return "down", "bad address"
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return "down", "unreachable"
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			if resp.StatusCode >= 500 {
				return "down", fmt.Sprintf("status %d", resp.StatusCode)
			}
			return "ok", detail(body)
		}
	}

	return &healthAgg{
		log: log,
		probers: map[string]prober{
			"gateway": func(context.Context) (string, string) { return "ok", "self" },
			"auth": grpcAlive(authUp, "HealthProbe", func(ctx context.Context) error {
				// A deliberately invalid token: Unauthenticated = service alive.
				_, err := genauth.NewAuthServiceClient(authUp.conn).
					ValidateToken(ctx, &genauth.ValidateTokenRequest{AccessToken: "probe"})
				return err
			}),
			"users": grpcAlive(usersUp, "HealthProbe", func(ctx context.Context) error {
				_, err := genusers.NewUserServiceClient(usersUp.conn).
					ListUsers(ctx, &genusers.ListUsersRequest{Page: &gencommon.PageRequest{Page: 1, PageSize: 1}})
				return err
			}),
			"jobs": grpcAlive(jobsUp, "HealthProbe", func(ctx context.Context) error {
				_, err := genjobs.NewJobServiceClient(jobsUp.conn).
					ListJobs(ctx, &genjobs.ListJobsRequest{Page: &gencommon.PageRequest{Page: 1, PageSize: 1}})
				return err
			}),
			"broker": httpGet(cfg.BrokerOpsAddr, "/health", func([]byte) string {
				return "tcp listener up"
			}),
			"websocket": httpGet(cfg.WSAddr, "/debug/stats", func(body []byte) string {
				var stats struct {
					Connections int `json:"connections"`
					Rooms       int `json:"rooms"`
				}
				if json.Unmarshal(body, &stats) == nil {
					return fmt.Sprintf("%d connections, %d rooms", stats.Connections, stats.Rooms)
				}
				return "reachable"
			}),
			"worker_pool": func(ctx context.Context) (string, string) {
				var cursor uint64
				workers := 0
				for {
					keys, next, err := rdb.Scan(ctx, cursor, "worker:*", 100).Result()
					if err != nil {
						return "down", "redis unreachable"
					}
					workers += len(keys)
					if next == 0 {
						break
					}
					cursor = next
				}
				if workers == 0 {
					return "degraded", "no workers registered"
				}
				return "ok", fmt.Sprintf("%d workers", workers)
			},
		},
	}
}

// handler serves GET /api/health/services.
func (h *healthAgg) handler(w http.ResponseWriter, r *http.Request) {
	results := make([]serviceStatus, len(healthOrder))
	var wg sync.WaitGroup
	for i, name := range healthOrder {
		probe, ok := h.probers[name]
		if !ok {
			results[i] = serviceStatus{Name: name, Status: "down", Detail: "no prober"}
			continue
		}
		wg.Add(1)
		go func(i int, name string, p prober) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), probeTimeout)
			defer cancel()
			start := time.Now()
			st, detail := p(ctx)
			lat := time.Since(start).Milliseconds()
			if st == "ok" && lat > 1000 {
				st = "degraded" // answering, but slowly
			}
			results[i] = serviceStatus{Name: name, Status: st, LatencyMs: lat, Detail: detail}
		}(i, name, probe)
	}
	wg.Wait()

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(healthPayload{CheckedAt: time.Now().UTC(), Services: results})
}
