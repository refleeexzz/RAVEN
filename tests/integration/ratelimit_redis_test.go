//go:build integration

package integration

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/gateway"
)

// Distributed rate limiting: two gateway replicas sharing one Redis must
// enforce ONE budget per key (the whole point of RATE_LIMIT_STORE=redis),
// while a memory-store replica keeps its historical per-process budget.
// Runs against the suite's real Redis container; skips without Docker.

// rateLimitFakeAuth mints a distinct user per token so each test gets a
// fresh bucket key in the shared Redis.
type rateLimitFakeAuth struct {
	genauth.UnimplementedAuthServiceServer
	users map[string]string // token -> user id
}

func (f rateLimitFakeAuth) ValidateToken(_ context.Context, req *genauth.ValidateTokenRequest) (*genauth.ValidateTokenResponse, error) {
	if id, ok := f.users[req.GetAccessToken()]; ok {
		return &genauth.ValidateTokenResponse{
			Valid: true, UserId: id, Permissions: []string{"jobs:read"},
		}, nil
	}
	return &genauth.ValidateTokenResponse{Valid: false}, nil
}

// bootRateLimitGateway starts the production gateway with the given
// rate-limit knobs against the suite's Redis.
func bootRateLimitGateway(t *testing.T, authAddr, jobsAddr, redisAddr, store string, rpm, burst int) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpAddr := l.Addr().String()
	_ = l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := gateway.Config{
		HTTPAddr:       httpAddr,
		AuthGRPCAddr:   authAddr,
		UsersGRPCAddr:  "127.0.0.1:1",
		JobsGRPCAddr:   jobsAddr,
		WSAddr:         "http://127.0.0.1:1",
		BrokerOpsAddr:  "http://127.0.0.1:1",
		RedisAddr:      redisAddr,
		JWTSecret:      "integration-secret",
		LogLevel:       "error",
		RateLimitRPM:   rpm,
		RateLimitBurst: burst,
		RateLimitStore: store,
	}
	go func() { _ = gateway.Run(ctx, cfg) }()

	base := "http://" + httpAddr
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := http.Get(base + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return base
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("gateway did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func rateLimitHit(t *testing.T, base, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, base+"/api/jobs", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/jobs: %v", err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestRateLimitSharedAcrossGatewayReplicas(t *testing.T) {
	check(t)

	token := "token-" + uuid.NewString()
	authAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genauth.RegisterAuthServiceServer(s, rateLimitFakeAuth{users: map[string]string{
			token: uuid.NewString(),
		}})
	})
	jobsAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genjobs.RegisterJobServiceServer(s, &keysFakeJobs{})
	})

	redisAddr := env.rdb.Options().Addr
	// rpm 6 = one token per 10s: refill is negligible over the test, so the
	// numbers below are exact, not racy.
	gwA := bootRateLimitGateway(t, authAddr, jobsAddr, redisAddr, "redis", 6, 4)
	gwB := bootRateLimitGateway(t, authAddr, jobsAddr, redisAddr, "redis", 6, 4)

	allowed, limited := 0, 0
	for i := 0; i < 12; i++ {
		gw := gwA
		if i%2 == 1 {
			gw = gwB
		}
		switch rateLimitHit(t, gw, token) {
		case http.StatusOK:
			allowed++
		case http.StatusTooManyRequests:
			limited++
		default:
			t.Fatalf("request %d: unexpected status", i+1)
		}
	}
	if allowed != 4 || limited != 8 {
		t.Fatalf("two replicas: got %d allowed / %d limited of 12, want exactly 4/8 — "+
			"the bucket must be shared through Redis", allowed, limited)
	}

	// Control: a memory-store replica keeps its own per-process budget for
	// the very same user.
	gwMem := bootRateLimitGateway(t, authAddr, jobsAddr, redisAddr, "memory", 6, 4)
	for i := 0; i < 4; i++ {
		if got := rateLimitHit(t, gwMem, token); got != http.StatusOK {
			t.Fatalf("memory replica request %d: got %d, want 200 (own budget)", i+1, got)
		}
	}
	if got := rateLimitHit(t, gwMem, token); got != http.StatusTooManyRequests {
		t.Fatalf("memory replica over budget: got %d, want 429", got)
	}
}
