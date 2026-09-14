//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/refleeexzz/RAVEN/internal/database"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/services/gateway"
)

// API keys end to end: migration 000007 (up AND down), the /api/keys REST
// surface and the `Authorization: ApiKey` scheme, all through the gateway's
// production Run entrypoint against in-process fake gRPC upstreams and a
// real per-test Postgres (000001 + 000007 applied verbatim).

// keysDB is deliveriesDB with migration 000001 + 000007 instead: users (for
// the owner FK) and api_keys.
func keysDB(t *testing.T, name string) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()

	ident := pgx.Identifier{name}.Sanitize()
	if _, err := env.pool.Exec(ctx, `DROP DATABASE IF EXISTS `+ident+` WITH (FORCE)`); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	if _, err := env.pool.Exec(ctx, `CREATE DATABASE `+ident); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}

	base, err := env.pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Path = "/" + name
	dsn := u.String()

	for _, m := range []string{"000001_auth_users.up.sql", "000007_api_keys.up.sql"} {
		if err := applyMigrationFile(ctx, dsn, m); err != nil {
			t.Fatalf("apply %s: %v", m, err)
		}
	}

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, dsn
}

// keysFakeAuth validates exactly one token: "token-owner" → the seeded user.
type keysFakeAuth struct {
	genauth.UnimplementedAuthServiceServer
	ownerID string
}

func (f keysFakeAuth) ValidateToken(_ context.Context, req *genauth.ValidateTokenRequest) (*genauth.ValidateTokenResponse, error) {
	if req.GetAccessToken() == "token-owner" {
		return &genauth.ValidateTokenResponse{
			Valid: true, UserId: f.ownerID, Email: "owner@example.com",
			Permissions: []string{"jobs:create", "jobs:read"},
		}, nil
	}
	return &genauth.ValidateTokenResponse{Valid: false}, nil
}

// keysFakeJobs captures the identity metadata the gateway forwarded.
type keysFakeJobs struct {
	genjobs.UnimplementedJobServiceServer
	lastUserID string
	lastPerms  string
	calls      int
}

func (f *keysFakeJobs) ListJobs(ctx context.Context, _ *genjobs.ListJobsRequest) (*genjobs.ListJobsResponse, error) {
	f.calls++
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		f.lastUserID = strings.Join(md.Get("x-user-id"), ",")
		f.lastPerms = strings.Join(md.Get("x-user-perms"), ",")
	}
	return &genjobs.ListJobsResponse{
		Page: &gencommon.PageResponse{Page: 1, PageSize: 20, Total: 0},
	}, nil
}

func serveKeysGRPC(t *testing.T, register func(s *grpc.Server)) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := grpc.NewServer()
	register(s)
	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)
	return lis.Addr().String()
}

// bootKeysGateway starts the production gateway with the keys database
// configured and generous rate limits.
func bootKeysGateway(t *testing.T, authAddr, jobsAddr, keysDSN string) string {
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
		HTTPAddr:           httpAddr,
		AuthGRPCAddr:       authAddr,
		UsersGRPCAddr:      "127.0.0.1:1", // never called in this suite
		JobsGRPCAddr:       jobsAddr,
		WSAddr:             "http://127.0.0.1:1",
		BrokerOpsAddr:      "http://127.0.0.1:1",
		RedisAddr:          "127.0.0.1:1", // down: rate limiter runs on its memory fallback
		JWTSecret:          "integration-secret",
		LogLevel:           "error",
		RateLimitRPM:       60000,
		RateLimitBurst:     100000,
		APIKeysDatabaseURL: keysDSN,
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

func keysReq(t *testing.T, method, url, authHeader, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var parsed map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &parsed); err != nil {
			t.Fatalf("decode response %s: %v", raw, err)
		}
	}
	return resp.StatusCode, parsed
}

func TestAPIKeysEndToEnd(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool, dsn := keysDB(t, "raven_api_keys_e2e")

	ownerID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash) VALUES ($1, 'keys-owner@example.com', 'x')`, ownerID); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	authAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genauth.RegisterAuthServiceServer(s, keysFakeAuth{ownerID: ownerID})
	})
	fakeJobs := &keysFakeJobs{}
	jobsAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genjobs.RegisterJobServiceServer(s, fakeJobs)
	})
	base := bootKeysGateway(t, authAddr, jobsAddr, dsn)

	// 1. Create (JWT). The plaintext key comes back exactly once.
	status, body := keysReq(t, "POST", base+"/api/keys", "Bearer token-owner",
		`{"name":"ci bot","scopes":["jobs:read"]}`)
	if status != http.StatusCreated {
		t.Fatalf("create: got %d (%v), want 201", status, body)
	}
	plaintext, _ := body["key"].(string)
	if !strings.HasPrefix(plaintext, "rav_live_") || len(plaintext) != 52 {
		t.Fatalf("key: got %q, want rav_live_<43 base64url>", plaintext)
	}
	keyID := body["api_key"].(map[string]any)["id"].(string)

	// The database holds only the hash, an 8-char prefix and the scopes.
	var prefix, hash string
	var scopes []string
	var lastUsed *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT key_prefix, key_hash, scopes, last_used_at FROM api_keys WHERE id = $1`,
		keyID).Scan(&prefix, &hash, &scopes, &lastUsed); err != nil {
		t.Fatalf("read key row: %v", err)
	}
	if len(prefix) != 8 || !strings.HasPrefix(plaintext, "rav_live_"+prefix) {
		t.Errorf("prefix: got %q, want the key's first 8 body chars", prefix)
	}
	if len(hash) != 64 || strings.Contains(hash, plaintext) {
		t.Errorf("hash: got %q, want hex sha256 (never the plaintext)", hash)
	}
	if len(scopes) != 1 || scopes[0] != "jobs:read" {
		t.Errorf("scopes: got %v, want [jobs:read]", scopes)
	}
	if lastUsed != nil {
		t.Errorf("last_used_at: got %v, want NULL before first use", lastUsed)
	}

	// 2. List (JWT): no hash, prefix rendered with the rav_live_ prefix.
	status, body = keysReq(t, "GET", base+"/api/keys", "Bearer token-owner", "")
	if status != http.StatusOK {
		t.Fatalf("list: got %d (%v), want 200", status, body)
	}
	listed := body["api_keys"].([]any)
	if len(listed) != 1 {
		t.Fatalf("list: got %d keys, want 1", len(listed))
	}
	if raw, _ := json.Marshal(body); strings.Contains(string(raw), hash) {
		t.Error("list response must never contain key hashes")
	}

	// 3. Authenticate with the key: owner + scopes forwarded as metadata.
	status, _ = keysReq(t, "GET", base+"/api/jobs", "ApiKey "+plaintext, "")
	if status != http.StatusOK {
		t.Fatalf("jobs via api key: got %d, want 200", status)
	}
	if fakeJobs.lastUserID != ownerID {
		t.Errorf("x-user-id: got %q, want %q", fakeJobs.lastUserID, ownerID)
	}
	if fakeJobs.lastPerms != "jobs:read" {
		t.Errorf("x-user-perms: got %q, want the key scopes [jobs:read]", fakeJobs.lastPerms)
	}

	// last_used_at lands asynchronously — poll briefly.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx, `SELECT last_used_at FROM api_keys WHERE id = $1`, keyID).Scan(&lastUsed); err != nil {
			t.Fatalf("re-read last_used_at: %v", err)
		}
		if lastUsed != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastUsed == nil {
		t.Error("last_used_at was not touched after an authenticated call")
	}

	// 4. Scopes are the whole permission set: jobs:create is missing → 403.
	status, body = keysReq(t, "POST", base+"/api/jobs", "ApiKey "+plaintext,
		`{"type":"t","payload":{}}`)
	if status != http.StatusForbidden {
		t.Fatalf("create with read-only key: got %d (%v), want 403", status, body)
	}

	// 5. A key cannot mint keys (JWT-only create).
	status, body = keysReq(t, "POST", base+"/api/keys", "ApiKey "+plaintext,
		`{"name":"spawn","scopes":["jobs:read"]}`)
	if status != http.StatusForbidden {
		t.Fatalf("create via api key: got %d (%v), want 403", status, body)
	}

	// 6. Revoke (JWT, owner): the key dies immediately — no auth cache.
	status, _ = keysReq(t, "DELETE", base+"/api/keys/"+keyID, "Bearer token-owner", "")
	if status != http.StatusOK {
		t.Fatalf("revoke: got %d, want 200", status)
	}
	var revokedAt *time.Time
	if err := pool.QueryRow(ctx, `SELECT revoked_at FROM api_keys WHERE id = $1`, keyID).Scan(&revokedAt); err != nil {
		t.Fatalf("read revoked_at: %v", err)
	}
	if revokedAt == nil {
		t.Error("revoked_at must be set after DELETE")
	}
	status, body = keysReq(t, "GET", base+"/api/jobs", "ApiKey "+plaintext, "")
	if status != http.StatusUnauthorized {
		t.Fatalf("revoked key: got %d (%v), want 401", status, body)
	}

	// 7. admin:* is never issuable, even to an admin-flavored request.
	status, body = keysReq(t, "POST", base+"/api/keys", "Bearer token-owner",
		`{"name":"root","scopes":["admin:*"]}`)
	if status != http.StatusBadRequest {
		t.Fatalf("admin scope: got %d (%v), want 400", status, body)
	}
}

// TestAPIKeysMigrationUpDown applies and rolls back 000007 verbatim.
func TestAPIKeysMigrationUpDown(t *testing.T) {
	check(t)
	ctx := context.Background()
	pool, dsn := keysDB(t, "raven_api_keys_migrate")

	assertTable := func(want bool) {
		t.Helper()
		var got bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM information_schema.tables
				WHERE table_name = 'api_keys')`).Scan(&got); err != nil {
			t.Fatalf("table probe: %v", err)
		}
		if got != want {
			t.Fatalf("api_keys table exists: got %v, want %v", got, want)
		}
	}

	assertTable(true)

	// Insert + FK behavior smoke: owner delete cascades.
	ownerID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		INSERT INTO users (id, email, password_hash) VALUES ($1, 'cascade@example.com', 'x')`, ownerID); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO api_keys (owner_id, name, key_prefix, key_hash, scopes)
		VALUES ($1, 'k', '12345678', 'hash', '{jobs:read}')`, ownerID); err != nil {
		t.Fatalf("insert key: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, ownerID); err != nil {
		t.Fatalf("delete owner: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM api_keys`).Scan(&n); err != nil {
		t.Fatalf("count keys: %v", err)
	}
	if n != 0 {
		t.Errorf("cascade: got %d keys left, want 0", n)
	}

	// Down drops the table; up brings it back (idempotent direction change).
	if err := applyMigrationFile(ctx, dsn, "000007_api_keys.down.sql"); err != nil {
		t.Fatalf("apply down: %v", err)
	}
	assertTable(false)
	if err := applyMigrationFile(ctx, dsn, "000007_api_keys.up.sql"); err != nil {
		t.Fatalf("re-apply up: %v", err)
	}
	assertTable(true)
}

// TestAPIKeysDisabledWithoutDatabase boots the gateway with no keys
// database: /api/keys and the ApiKey scheme answer 503, JWT is unaffected.
func TestAPIKeysDisabledWithoutDatabase(t *testing.T) {
	check(t)
	ownerID := uuid.NewString()
	authAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genauth.RegisterAuthServiceServer(s, keysFakeAuth{ownerID: ownerID})
	})
	jobsAddr := serveKeysGRPC(t, func(s *grpc.Server) {
		genjobs.RegisterJobServiceServer(s, &keysFakeJobs{})
	})
	base := bootKeysGateway(t, authAddr, jobsAddr, "") // no keys DSN

	status, body := keysReq(t, "GET", base+"/api/keys", "Bearer token-owner", "")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("list with keys disabled: got %d (%v), want 503", status, body)
	}
	status, body = keysReq(t, "GET", base+"/api/jobs", "ApiKey rav_live_"+strings.Repeat("a", 43), "")
	if status != http.StatusServiceUnavailable {
		t.Fatalf("api key scheme with keys disabled: got %d (%v), want 503", status, body)
	}
	// JWT keeps working on the very same gateway.
	status, _ = keysReq(t, "GET", base+"/api/jobs", "Bearer token-owner", "")
	if status != http.StatusOK {
		t.Fatalf("JWT path must be unaffected: got %d, want 200", status)
	}
}
