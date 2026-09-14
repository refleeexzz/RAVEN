//go:build integration

// Integration security tests for the RAVEN identity layer. They run the real
// auth + users gRPC services against real Postgres (migration 000001) and
// real Redis in testcontainers, and attack them over the wire. The harness
// mirrors tests/integration (same containers, same migration), but starts
// lazily via sync.Once because another suite in this package already owns
// TestMain. Container cleanup is handled by the testcontainers Ryuk reaper
// when the test process exits. Findings are registered in
// docs/security/auth.md.
package security

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/database"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	authsvc "github.com/refleeexzz/RAVEN/services/auth"
	userssvc "github.com/refleeexzz/RAVEN/services/users"
)

const (
	authSecJWTSecret   = "security-integration-secret"
	authSecSetupWindow = 5 * time.Minute
	authSecSlowCost    = 12 // production bcrypt cost, for the timing test
)

// authSecEnv mirrors the tests/integration harness: shared containers, pools
// and gRPC clients. Auth runs at the fast test cost; AuthSlow runs at the
// production cost so timing attacks are measurable.
type authSecEnv struct {
	pool *pgxpool.Pool
	rdb  *goredis.Client

	servers []*grpc.Server
	conns   []*grpc.ClientConn

	Auth     genauth.AuthServiceClient
	AuthSlow genauth.AuthServiceClient
	Users    genusers.UserServiceClient

	pgContainer    *postgres.PostgresContainer
	redisContainer *redis.RedisContainer
}

var (
	authSecOnce sync.Once
	authSecE    *authSecEnv
	authSecErr  error
)

// authSecCheck starts the environment on first use and skips the test when
// Docker is unavailable.
func authSecCheck(t *testing.T) {
	t.Helper()
	authSecOnce.Do(func() {
		authSecE, authSecErr = authSecSetup()
	})
	if authSecErr != nil {
		t.Skipf("skipping security integration test: %v", authSecErr)
	}
}

func authSecSetup() (*authSecEnv, error) {
	if err := authSecDockerOK(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), authSecSetupWindow)
	defer cancel()

	e := &authSecEnv{}

	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("raven"),
		postgres.WithUsername("raven"),
		postgres.WithPassword("raven"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	e.pgContainer = pg

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("postgres connection string: %w", err)
	}

	rd, err := redis.Run(ctx, "redis:8")
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	e.redisContainer = rd

	redisURL, err := rd.ConnectionString(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis connection string: %w", err)
	}
	redisOpt, err := goredis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	if err := authSecApplyMigration(ctx, dsn); err != nil {
		return nil, err
	}

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pool: %w", err)
	}
	e.pool = pool
	e.rdb = goredis.NewClient(redisOpt)
	if err := e.rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	serve := func(register func(s *grpc.Server)) error {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		s := grpc.NewServer()
		register(s)
		go func() { _ = s.Serve(lis) }()
		conn, err := grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		e.servers = append(e.servers, s)
		e.conns = append(e.conns, conn)
		return nil
	}

	if err := serve(func(s *grpc.Server) {
		genauth.RegisterAuthServiceServer(s, authsvc.NewServer(pool, e.rdb, log,
			authSecJWTSecret, bcrypt.MinCost, authsvc.NewServiceMetrics(metrics.New("auth-sec"))))
	}); err != nil {
		return nil, err
	}
	// Production-cost server for the timing test: bcrypt at cost 12 takes
	// long enough that a skipped compare is unambiguous.
	if err := serve(func(s *grpc.Server) {
		genauth.RegisterAuthServiceServer(s, authsvc.NewServer(pool, e.rdb, log,
			authSecJWTSecret, authSecSlowCost, authsvc.NewServiceMetrics(metrics.New("auth-sec-slow"))))
	}); err != nil {
		return nil, err
	}
	if err := serve(func(s *grpc.Server) {
		genusers.RegisterUserServiceServer(s, userssvc.NewServer(pool, log,
			bcrypt.MinCost, userssvc.NewServiceMetrics(metrics.New("users-sec"))))
	}); err != nil {
		return nil, err
	}

	e.Auth = genauth.NewAuthServiceClient(e.conns[0])
	e.AuthSlow = genauth.NewAuthServiceClient(e.conns[1])
	e.Users = genusers.NewUserServiceClient(e.conns[2])
	return e, nil
}

// authSecDockerOK verifies the Docker CLI and daemon before paying the cost
// of container startup.
func authSecDockerOK() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker CLI not found: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", "info").CombinedOutput(); err != nil {
		return fmt.Errorf("docker daemon unavailable: %v: %s", err, out)
	}
	return nil
}

// authSecApplyMigration runs migrations/000001_auth_users.up.sql verbatim
// (simple protocol; retried briefly for the Windows Docker Desktop port
// proxy).
func authSecApplyMigration(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	path, err := authSecMigrationPath()
	if err != nil {
		return err
	}
	sql, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		lastErr = func() error {
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				return err
			}
			defer conn.Close(ctx)
			_, err = conn.Exec(ctx, string(sql))
			return err
		}()
		if lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("apply migration 000001: %w", lastErr)
}

// authSecMigrationPath finds migrations/000001_auth_users.up.sql by walking
// up from the working directory, so the suite runs both from tests/security
// and from any scratch copy of this file.
func authSecMigrationPath() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	start := dir
	for range 6 {
		p := filepath.Join(dir, "migrations", "000001_auth_users.up.sql")
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("migrations/000001_auth_users.up.sql not found above %s", start)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func authSecRequireCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected gRPC error %v, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("gRPC code: got %v, want %v (err: %v)", got, want, err)
	}
}

// authSecAsCaller attaches the identity the gateway forwards after a
// successful AuthN check (see upstream.call in the gateway).
func authSecAsCaller(ctx context.Context, userID string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "x-user-id", userID)
}

// authSecRegister registers via the fast auth service and returns the id.
func authSecRegister(t *testing.T, email string) string {
	t.Helper()
	reg, err := authSecE.Auth.Register(context.Background(), &genauth.RegisterRequest{
		Email: email, Password: "password-123",
	})
	if err != nil {
		t.Fatalf("Register(%s): %v", email, err)
	}
	return reg.GetUserId()
}

// authSecGrantAdmin gives a user the ADMIN role directly in the database —
// the platform has no public RPC for role grants by design.
func authSecGrantAdmin(t *testing.T, userID string) {
	t.Helper()
	_, err := authSecE.pool.Exec(context.Background(), `
		INSERT INTO user_roles (user_id, role_id)
		SELECT $1::uuid, id FROM roles WHERE name = 'ADMIN'
		ON CONFLICT DO NOTHING`, userID)
	if err != nil {
		t.Fatalf("grant ADMIN to %s: %v", userID, err)
	}
}

// authSecLiveSessions counts non-revoked sessions of a user.
func authSecLiveSessions(t *testing.T, userID string) int {
	t.Helper()
	var n int
	if err := authSecE.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM sessions WHERE user_id = $1::uuid AND revoked_at IS NULL`,
		userID).Scan(&n); err != nil {
		t.Fatalf("count live sessions: %v", err)
	}
	return n
}

// ---------------------------------------------------------------------------
// AUTH-01: login timing oracle (account enumeration)
// ---------------------------------------------------------------------------

// TestSecure_LoginTimingNoOracle measures the unknown-email login path
// against the wrong-password path on a production-cost (bcrypt 12) server.
// Without a dummy compare the unknown-email path returns in ~1 ms while the
// real compare takes hundreds of ms — a remote, unambiguous oracle for
// which emails are registered.
func TestSecure_LoginTimingNoOracle(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	const email = "timing@sec-enum.example.com"
	if _, err := authSecE.AuthSlow.Register(ctx, &genauth.RegisterRequest{
		Email: email, Password: "password-123",
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	measure := func(email, password string) time.Duration {
		start := time.Now()
		_, err := authSecE.AuthSlow.Login(ctx, &genauth.LoginRequest{
			Email: email, Password: password,
		})
		authSecRequireCode(t, err, codes.Unauthenticated)
		return time.Since(start)
	}

	// Warm up connections and the pool, then measure.
	const rounds = 5
	var knownTotal, unknownTotal time.Duration
	for range rounds {
		knownTotal += measure(email, "wrong-password")
		unknownTotal += measure("ghost@sec-enum.example.com", "wrong-password")
	}
	knownMean := knownTotal / rounds
	unknownMean := unknownTotal / rounds
	t.Logf("login latency: known-email/wrong-password mean=%v, unknown-email mean=%v",
		knownMean, unknownMean)

	// A bcrypt compare at cost 12 costs far more than 50 ms on any machine;
	// a skipped compare (DB lookup only) costs single-digit ms.
	if unknownMean < 50*time.Millisecond {
		t.Errorf("unknown-email login returns in %v: no password compare happens, "+
			"response time leaks which emails are registered", unknownMean)
	}
	if unknownMean < knownMean/2 {
		t.Errorf("unknown-email path (%v) is >2x faster than the wrong-password path (%v)",
			unknownMean, knownMean)
	}
}

// TestSecure_LoginErrorMessagesIdentical pins the message side of the
// anti-enumeration contract: unknown email and wrong password must be
// indistinguishable to the client.
func TestSecure_LoginErrorMessagesIdentical(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	const email = "msgs@sec-enum.example.com"
	authSecRegister(t, email)

	_, errUnknown := authSecE.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "ghost@sec-enum.example.com", Password: "password-123"})
	_, errWrong := authSecE.Auth.Login(ctx, &genauth.LoginRequest{
		Email: email, Password: "wrong-password"})

	authSecRequireCode(t, errUnknown, codes.Unauthenticated)
	authSecRequireCode(t, errWrong, codes.Unauthenticated)
	if status.Convert(errUnknown).Message() != status.Convert(errWrong).Message() {
		t.Errorf("login errors differ: unknown=%q wrong=%q",
			status.Convert(errUnknown).Message(), status.Convert(errWrong).Message())
	}
}

// TestSecure_RegisterDuplicateOracle documents the accepted registration
// oracle: a duplicate email is reported as AlreadyExists. This is inherent
// to registration without an email-verification flow and is rate-limited at
// the gateway; it is registered as an info finding (AUTH-09).
func TestSecure_RegisterDuplicateOracle(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	const email = "dup@sec-enum.example.com"
	authSecRegister(t, email)
	_, err := authSecE.Auth.Register(ctx, &genauth.RegisterRequest{
		Email: email, Password: "password-123"})
	authSecRequireCode(t, err, codes.AlreadyExists)
}

// ---------------------------------------------------------------------------
// AUTH-02: IDOR/BOLA on the users service
// ---------------------------------------------------------------------------

// TestSecure_UsersUpdateIDOR attacks UpdateUser: a non-admin caller must
// not be able to modify another user's profile. Pre-fix this succeeds for
// anyone who can reach the gRPC port; the fix enforces owner-or-admin.
func TestSecure_UsersUpdateIDOR(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	victim := authSecRegister(t, "victim-upd@sec-bola.example.com")
	attacker := authSecRegister(t, "attacker-upd@sec-bola.example.com")
	admin := authSecRegister(t, "admin-upd@sec-bola.example.com")
	authSecGrantAdmin(t, admin)

	// Attack 1: authenticated non-admin modifies someone else's profile.
	_, err := authSecE.Users.UpdateUser(authSecAsCaller(ctx, attacker), &genusers.UpdateUserRequest{
		Id: victim, DisplayName: "pwned", Bio: "defaced by attacker",
	})
	authSecRequireCode(t, err, codes.PermissionDenied)

	// Attack 2: no caller identity at all (direct gRPC access).
	_, err = authSecE.Users.UpdateUser(ctx, &genusers.UpdateUserRequest{
		Id: victim, DisplayName: "pwned",
	})
	authSecRequireCode(t, err, codes.Unauthenticated)

	// Attack 3: forged identity — a random uuid that owns nothing.
	_, err = authSecE.Users.UpdateUser(
		authSecAsCaller(ctx, "00000000-0000-0000-0000-000000000000"),
		&genusers.UpdateUserRequest{Id: victim, DisplayName: "pwned"})
	authSecRequireCode(t, err, codes.PermissionDenied)

	// The victim must be untouched after all three attacks.
	got, err := authSecE.Users.GetUser(ctx, &genusers.GetUserRequest{Id: victim})
	if err != nil {
		t.Fatalf("GetUser(victim): %v", err)
	}
	if got.GetDisplayName() == "pwned" || got.GetBio() != "" {
		t.Errorf("victim profile was modified by an unauthorized caller: %+v", got)
	}

	// Owner can update their own profile.
	if _, err := authSecE.Users.UpdateUser(authSecAsCaller(ctx, victim), &genusers.UpdateUserRequest{
		Id: victim, DisplayName: "Victim Renamed", Bio: "mine",
	}); err != nil {
		t.Errorf("owner update must succeed: %v", err)
	}

	// Admin can update anyone (moderation tooling).
	if _, err := authSecE.Users.UpdateUser(authSecAsCaller(ctx, admin), &genusers.UpdateUserRequest{
		Id: victim, DisplayName: "Moderated",
	}); err != nil {
		t.Errorf("admin update must succeed: %v", err)
	}
}

// TestSecure_UsersDeleteIDOR attacks DeleteUser with the same matrix.
func TestSecure_UsersDeleteIDOR(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	victim := authSecRegister(t, "victim-del@sec-bola.example.com")
	attacker := authSecRegister(t, "attacker-del@sec-bola.example.com")
	admin := authSecRegister(t, "admin-del@sec-bola.example.com")
	authSecGrantAdmin(t, admin)

	// Non-admin attacker deletes the victim.
	_, err := authSecE.Users.DeleteUser(authSecAsCaller(ctx, attacker),
		&genusers.DeleteUserRequest{Id: victim})
	authSecRequireCode(t, err, codes.PermissionDenied)

	// Anonymous (no forwarded identity).
	_, err = authSecE.Users.DeleteUser(ctx, &genusers.DeleteUserRequest{Id: victim})
	authSecRequireCode(t, err, codes.Unauthenticated)

	// Victim must still be active.
	if _, err := authSecE.Users.GetUser(ctx, &genusers.GetUserRequest{Id: victim}); err != nil {
		t.Errorf("victim must survive unauthorized delete attempts: %v", err)
	}

	// Admin delete works (and stays soft).
	if _, err := authSecE.Users.DeleteUser(authSecAsCaller(ctx, admin),
		&genusers.DeleteUserRequest{Id: victim}); err != nil {
		t.Errorf("admin delete must succeed: %v", err)
	}
	_, err = authSecE.Users.GetUser(ctx, &genusers.GetUserRequest{Id: victim})
	authSecRequireCode(t, err, codes.NotFound)
}

// TestSecure_UsersIncludeDeletedRequiresAdmin: the include_deleted flag is
// documented as admin tooling, but the service used to honor it for any
// caller, exposing soft-deleted profiles (email/bio/avatar) to every
// authenticated user. Now admin-only.
func TestSecure_UsersIncludeDeletedRequiresAdmin(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	gone := authSecRegister(t, "gone@sec-bola.example.com")
	authSecGrantAdmin(t, gone) // self-delete as admin to set the stage
	if _, err := authSecE.Users.DeleteUser(authSecAsCaller(ctx, gone),
		&genusers.DeleteUserRequest{Id: gone}); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	caller := authSecRegister(t, "caller@sec-bola.example.com")
	filter := &genusers.ListUsersRequest{
		Page:           &gencommon.PageRequest{Page: 1, PageSize: 10},
		EmailFilter:    "sec-bola.example.com",
		IncludeDeleted: true,
	}

	// Non-admin caller must be refused, not silently downgraded.
	_, err := authSecE.Users.ListUsers(authSecAsCaller(ctx, caller), filter)
	authSecRequireCode(t, err, codes.PermissionDenied)

	// Anonymous caller must be refused as well.
	_, err = authSecE.Users.ListUsers(ctx, filter)
	authSecRequireCode(t, err, codes.Unauthenticated)

	// Admin sees the deleted row, flagged as deleted.
	admin := authSecRegister(t, "admin-list@sec-bola.example.com")
	authSecGrantAdmin(t, admin)
	resp, err := authSecE.Users.ListUsers(authSecAsCaller(ctx, admin), filter)
	if err != nil {
		t.Fatalf("ListUsers as admin: %v", err)
	}
	found := false
	for _, u := range resp.GetUsers() {
		if u.GetId() == gone && u.GetDeleted() {
			found = true
		}
	}
	if !found {
		t.Error("admin listing must include the soft-deleted user, flagged deleted")
	}
}

// ---------------------------------------------------------------------------
// AUTH-04: refresh-token rotation race
// ---------------------------------------------------------------------------

// TestSecure_RefreshRotationConcurrency fires 20 concurrent refreshes of the
// SAME refresh token. Exactly one may succeed; every other caller must get
// Unauthenticated, and the reuse detection must then revoke the whole
// session family (including the winner's fresh session — the safe choice
// when a token family may be stolen). Run under -race via the CI gate.
func TestSecure_RefreshRotationConcurrency(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	userID := authSecRegister(t, "race@sec-refresh.example.com")
	pair, err := authSecE.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "race@sec-refresh.example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	const attackers = 20
	var wg sync.WaitGroup
	results := make(chan error, attackers)
	for range attackers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := authSecE.Auth.Refresh(ctx, &genauth.RefreshRequest{
				RefreshToken: pair.GetRefreshToken()})
			results <- err
		}()
	}
	wg.Wait()
	close(results)

	var succeeded, failed int
	for err := range results {
		if err == nil {
			succeeded++
			continue
		}
		failed++
		if code := status.Code(err); code != codes.Unauthenticated {
			t.Errorf("losing refresh: got code %v, want Unauthenticated", code)
		}
	}
	if succeeded != 1 {
		t.Errorf("concurrent refreshes succeeded %d times, want exactly 1 (double-spend bug)", succeeded)
	}
	if failed != attackers-1 {
		t.Errorf("failures: got %d, want %d", failed, attackers-1)
	}

	// Reuse detection is conservative: after a race the whole family is
	// revoked, so a fresh login is required. This is the documented safe
	// behavior (docs/security/auth.md, AUTH-04 note).
	if n := authSecLiveSessions(t, userID); n != 0 {
		t.Errorf("live sessions after rotation race: got %d, want 0 (family revoked)", n)
	}
}

// TestSecure_RefreshTokenShape pins the wire shape of minted refresh tokens:
// 32 bytes of crypto/rand entropy, base64url-encoded (43 chars), unique
// across logins.
func TestSecure_RefreshTokenShape(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	authSecRegister(t, "shape@sec-refresh.example.com")
	seen := map[string]bool{}
	for range 4 {
		pair, err := authSecE.Auth.Login(ctx, &genauth.LoginRequest{
			Email: "shape@sec-refresh.example.com", Password: "password-123"})
		if err != nil {
			t.Fatalf("Login: %v", err)
		}
		tok := pair.GetRefreshToken()
		if len(tok) != 43 {
			t.Errorf("refresh token length: got %d, want 43 (32 bytes base64url)", len(tok))
		}
		if seen[tok] {
			t.Error("refresh token reused across logins")
		}
		seen[tok] = true
	}
}

// TestSecure_RefreshExpiredAndUnknown: unknown tokens and expired sessions
// are both rejected with Unauthenticated.
func TestSecure_RefreshExpiredAndUnknown(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	// Never-issued token.
	_, err := authSecE.Auth.Refresh(ctx, &genauth.RefreshRequest{
		RefreshToken: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"})
	authSecRequireCode(t, err, codes.Unauthenticated)

	// Expired session: plant one directly with a known token hash.
	userID := authSecRegister(t, "expired@sec-refresh.example.com")
	rawTok := "expired-session-token-0123456789abcdefg"
	_, err = authSecE.pool.Exec(ctx, `
		INSERT INTO sessions (user_id, refresh_token_hash, expires_at)
		VALUES ($1::uuid, $2, now() - interval '1 hour')`,
		userID, authsvc.HashRefreshToken(rawTok))
	if err != nil {
		t.Fatalf("plant expired session: %v", err)
	}
	_, err = authSecE.Auth.Refresh(ctx, &genauth.RefreshRequest{RefreshToken: rawTok})
	authSecRequireCode(t, err, codes.Unauthenticated)
	if msg := status.Convert(err).Message(); !strings.Contains(msg, "expired") {
		t.Errorf("expired session error message: got %q, want it to say expired", msg)
	}
}

// TestSecure_RevocationDenylist: a revoked jti must stop validating
// immediately, and the denylist entry must expire with the token (Redis TTL
// ≈ remaining access-token lifetime).
func TestSecure_RevocationDenylist(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	authSecRegister(t, "revoke@sec-refresh.example.com")
	pair, err := authSecE.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "revoke@sec-refresh.example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	claims, err := ravenauth.ParseAccessToken(authSecJWTSecret, pair.GetAccessToken())
	if err != nil {
		t.Fatalf("ParseAccessToken: %v", err)
	}

	if _, err := authSecE.Auth.RevokeToken(ctx, &genauth.RevokeTokenRequest{
		AccessToken: pair.GetAccessToken()}); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	v, err := authSecE.Auth.ValidateToken(ctx, &genauth.ValidateTokenRequest{
		AccessToken: pair.GetAccessToken()})
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if v.GetValid() {
		t.Error("revoked jti still validates")
	}

	ttl, err := authSecE.rdb.TTL(ctx, "revoked:jti:"+claims.ID).Result()
	if err != nil {
		t.Fatalf("redis TTL: %v", err)
	}
	if ttl <= 0 || ttl > ravenauth.AccessTokenTTL {
		t.Errorf("denylist TTL: got %v, want in (0, %v]", ttl, ravenauth.AccessTokenTTL)
	}
}

// ---------------------------------------------------------------------------
// SQL injection surface on users/auth queries (tested, not vulnerable)
// ---------------------------------------------------------------------------

// TestSecure_SQLiEmailFilter throws classic injection payloads at the one
// free-text query input of the identity layer: the ListUsers email filter.
func TestSecure_SQLiEmailFilter(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	authSecRegister(t, "mallory@sec-sqli.example.com")

	payloads := []string{
		`' OR '1'='1`,
		`' OR email LIKE '%`,
		`%`, `_`, `\`, `%\_%`,
		`'; DROP TABLE users; --`,
		`sec-sqli.example.com' OR 'x'='x`,
	}
	for _, p := range payloads {
		resp, err := authSecE.Users.ListUsers(ctx, &genusers.ListUsersRequest{
			Page:        &gencommon.PageRequest{Page: 1, PageSize: 10},
			EmailFilter: p,
		})
		if err != nil {
			t.Fatalf("ListUsers(%q): %v", p, err)
		}
		if resp.GetPage().GetTotal() != 0 {
			t.Errorf("filter %q matched %d rows; metacharacters must be literal", p,
				resp.GetPage().GetTotal())
		}
	}

	// Sanity: a plain substring still matches.
	resp, err := authSecE.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page:        &gencommon.PageRequest{Page: 1, PageSize: 10},
		EmailFilter: "sec-sqli.example.com",
	})
	if err != nil {
		t.Fatalf("ListUsers plain filter: %v", err)
	}
	if resp.GetPage().GetTotal() != 1 {
		t.Errorf("plain filter: got total %d, want 1", resp.GetPage().GetTotal())
	}
}

// TestSecure_SQLiIdentifiers: emails and display names carrying injection
// payloads are either rejected by validation or stored/compared literally.
func TestSecure_SQLiIdentifiers(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	// Email validation rejects anything without a clean local@domain shape.
	for _, email := range []string{
		`' OR '1'='1`, `a@b.c' OR '1'='1`, `a@b.c; DROP TABLE users`,
	} {
		_, err := authSecE.Auth.Register(ctx, &genauth.RegisterRequest{
			Email: email, Password: "password-123"})
		authSecRequireCode(t, err, codes.InvalidArgument)
	}

	// A display name with SQL metacharacters is data, not code: login by
	// email is unaffected and the name round-trips literally.
	const email = "sqlname@sec-sqli.example.com"
	const name = `x'); DROP TABLE users; --`
	reg, err := authSecE.Auth.Register(ctx, &genauth.RegisterRequest{
		Email: email, Password: "password-123", DisplayName: name})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, err := authSecE.Users.GetUser(ctx, &genusers.GetUserRequest{Id: reg.GetUserId()})
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.GetDisplayName() != name {
		t.Errorf("display name: got %q, want literal %q", got.GetDisplayName(), name)
	}
	if _, err := authSecE.Auth.Login(ctx, &genauth.LoginRequest{
		Email: email, Password: "password-123"}); err != nil {
		t.Errorf("login after SQLi display name: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Integer / pagination abuse (tested, not vulnerable)
// ---------------------------------------------------------------------------

func TestSecure_PaginationBounds(t *testing.T) {
	authSecCheck(t)
	ctx := context.Background()

	// Negative and zero values fall back to the platform defaults.
	resp, err := authSecE.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page: &gencommon.PageRequest{Page: -5, PageSize: -100},
	})
	if err != nil {
		t.Fatalf("ListUsers negative: %v", err)
	}
	if resp.GetPage().GetPage() != 1 || resp.GetPage().GetPageSize() != 20 {
		t.Errorf("negative pagination: got page=%d size=%d, want 1/20",
			resp.GetPage().GetPage(), resp.GetPage().GetPageSize())
	}

	// Huge page size is capped at 100.
	resp, err = authSecE.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page: &gencommon.PageRequest{Page: 1, PageSize: 1 << 30},
	})
	if err != nil {
		t.Fatalf("ListUsers huge size: %v", err)
	}
	if resp.GetPage().GetPageSize() != 100 {
		t.Errorf("huge page size: got %d, want 100 (capped)", resp.GetPage().GetPageSize())
	}

	// Huge page number: the offset fits int64, the page is simply empty.
	resp, err = authSecE.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page: &gencommon.PageRequest{Page: 1<<31 - 1, PageSize: 100},
	})
	if err != nil {
		t.Fatalf("ListUsers huge page: %v", err)
	}
	if len(resp.GetUsers()) != 0 {
		t.Errorf("huge page: got %d users, want 0", len(resp.GetUsers()))
	}
}
