//go:build integration

// Package integration runs the auth + users services end to end against real
// Postgres (migration 000001 applied) and real Redis, both in testcontainers.
// Every test skips cleanly when Docker is unavailable.
package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
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

	"github.com/refleeexzz/RAVEN/internal/database"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	authsvc "github.com/refleeexzz/RAVEN/services/auth"
	userssvc "github.com/refleeexzz/RAVEN/services/users"
)

const (
	jwtTestSecret = "integration-test-secret"
	setupTimeout  = 5 * time.Minute
)

// testEnv holds the shared containers, connections and gRPC clients.
type testEnv struct {
	pool *pgxpool.Pool
	rdb  *goredis.Client

	authGRPC  *grpc.Server
	usersGRPC *grpc.Server
	authConn  *grpc.ClientConn
	usersConn *grpc.ClientConn

	Auth  genauth.AuthServiceClient
	Users genusers.UserServiceClient

	pgContainer    *postgres.PostgresContainer
	redisContainer *redis.RedisContainer
}

var (
	env      *testEnv
	setupErr error
)

func TestMain(m *testing.M) {
	env, setupErr = setup()
	code := m.Run()
	if env != nil {
		env.teardown()
	}
	os.Exit(code)
}

// check skips the test when the integration environment is unavailable.
func check(t *testing.T) {
	t.Helper()
	if setupErr != nil {
		t.Skipf("skipping integration test: %v", setupErr)
	}
}

// setup starts the containers, applies migration 000001 and serves both
// gRPC services on loopback. Any failure is reported as setupErr so tests
// SKIP instead of failing on machines without Docker.
func setup() (*testEnv, error) {
	if err := dockerAvailable(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	e := &testEnv{}

	pg, err := postgres.Run(ctx, "postgres:17-alpine",
		postgres.WithDatabase("raven"),
		postgres.WithUsername("raven"),
		postgres.WithPassword("raven"),
		// Not applied by default: wait for the second "ready to accept
		// connections" (postgres restarts after init) AND for the port to
		// be served on localhost — required on Windows/Docker Desktop.
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, fmt.Errorf("start postgres container: %w", err)
	}
	e.pgContainer = pg

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("postgres connection string: %w", err)
	}

	rd, err := redis.Run(ctx, "redis:8")
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	e.redisContainer = rd

	redisURL, err := rd.ConnectionString(ctx)
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("redis connection string: %w", err)
	}
	redisOpt, err := goredis.ParseURL(redisURL)
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("parse redis url: %w", err)
	}

	if err := applyMigration(ctx, dsn); err != nil {
		e.teardown()
		return nil, err
	}

	pool, err := database.NewPool(ctx, dsn)
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("pool: %w", err)
	}
	e.pool = pool
	e.rdb = goredis.NewClient(redisOpt)
	if err := e.rdb.Ping(ctx).Err(); err != nil {
		e.teardown()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Serve auth on a random loopback port.
	authLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.teardown()
		return nil, err
	}
	authMetrics := authsvc.NewServiceMetrics(metrics.New("auth"))
	e.authGRPC = grpc.NewServer()
	genauth.RegisterAuthServiceServer(e.authGRPC,
		authsvc.NewServer(pool, e.rdb, log, jwtTestSecret, bcrypt.MinCost, authMetrics))
	go func() { _ = e.authGRPC.Serve(authLis) }()

	// Serve users on a random loopback port.
	usersLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		e.teardown()
		return nil, err
	}
	usersMetrics := userssvc.NewServiceMetrics(metrics.New("users"))
	e.usersGRPC = grpc.NewServer()
	genusers.RegisterUserServiceServer(e.usersGRPC,
		userssvc.NewServer(pool, log, bcrypt.MinCost, usersMetrics))
	go func() { _ = e.usersGRPC.Serve(usersLis) }()

	e.authConn, err = grpc.NewClient(authLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("dial auth: %w", err)
	}
	e.usersConn, err = grpc.NewClient(usersLis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		e.teardown()
		return nil, fmt.Errorf("dial users: %w", err)
	}

	e.Auth = genauth.NewAuthServiceClient(e.authConn)
	e.Users = genusers.NewUserServiceClient(e.usersConn)
	return e, nil
}

func (e *testEnv) teardown() {
	if e.authConn != nil {
		_ = e.authConn.Close()
	}
	if e.usersConn != nil {
		_ = e.usersConn.Close()
	}
	if e.authGRPC != nil {
		e.authGRPC.Stop()
	}
	if e.usersGRPC != nil {
		e.usersGRPC.Stop()
	}
	if e.rdb != nil {
		_ = e.rdb.Close()
	}
	if e.pool != nil {
		e.pool.Close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if e.redisContainer != nil {
		_ = e.redisContainer.Terminate(ctx)
	}
	if e.pgContainer != nil {
		_ = e.pgContainer.Terminate(ctx)
	}
}

// dockerAvailable verifies the Docker CLI and daemon before paying the cost
// of container startup.
func dockerAvailable() error {
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

// applyMigration runs migrations/000001_auth_users.up.sql verbatim. Simple
// protocol mode is required because the file holds many statements. The
// connect is retried briefly: on Windows the Docker Desktop port proxy can
// accept-then-drop connections for a moment after the container is ready.
func applyMigration(ctx context.Context, dsn string) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	path := filepath.Join("..", "..", "migrations", "000001_auth_users.up.sql")
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

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func requireGRPCCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected gRPC error %v, got nil", want)
	}
	if got := status.Code(err); got != want {
		t.Fatalf("gRPC code: got %v, want %v (err: %v)", got, want, err)
	}
}

// ---------------------------------------------------------------------------
// auth flows
// ---------------------------------------------------------------------------

func TestAuthUsers_RegisterLoginValidate(t *testing.T) {
	check(t)
	ctx := context.Background()

	// Register.
	reg, err := env.Auth.Register(ctx, &genauth.RegisterRequest{
		Email:       "alice@example.com",
		Password:    "password-123",
		DisplayName: "Alice",
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reg.GetUserId() == "" {
		t.Fatal("Register returned an empty user id")
	}

	// Duplicate email -> AlreadyExists.
	_, err = env.Auth.Register(ctx, &genauth.RegisterRequest{
		Email:    "alice@example.com",
		Password: "password-123",
	})
	requireGRPCCode(t, err, codes.AlreadyExists)

	// Case-insensitive uniqueness (citext).
	_, err = env.Auth.Register(ctx, &genauth.RegisterRequest{
		Email:    "ALICE@example.com",
		Password: "password-123",
	})
	requireGRPCCode(t, err, codes.AlreadyExists)

	// Validation errors.
	_, err = env.Auth.Register(ctx, &genauth.RegisterRequest{Email: "not-an-email", Password: "password-123"})
	requireGRPCCode(t, err, codes.InvalidArgument)
	_, err = env.Auth.Register(ctx, &genauth.RegisterRequest{Email: "x@example.com", Password: "short"})
	requireGRPCCode(t, err, codes.InvalidArgument)

	// Login: wrong password and unknown email both give Unauthenticated.
	_, err = env.Auth.Login(ctx, &genauth.LoginRequest{Email: "alice@example.com", Password: "wrong-password"})
	requireGRPCCode(t, err, codes.Unauthenticated)
	_, err = env.Auth.Login(ctx, &genauth.LoginRequest{Email: "nobody@example.com", Password: "password-123"})
	requireGRPCCode(t, err, codes.Unauthenticated)

	// Login: success.
	pair, err := env.Auth.Login(ctx, &genauth.LoginRequest{Email: "alice@example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if pair.GetAccessToken() == "" || pair.GetRefreshToken() == "" {
		t.Fatal("Login returned an incomplete token pair")
	}
	if pair.GetAccessExpiresAt() <= time.Now().Unix() {
		t.Error("access token expiry is not in the future")
	}
	if got := pair.GetRefreshExpiresAt() - pair.GetAccessExpiresAt(); got <= 0 {
		t.Error("refresh token must outlive the access token")
	}

	// ValidateToken.
	v, err := env.Auth.ValidateToken(ctx, &genauth.ValidateTokenRequest{AccessToken: pair.GetAccessToken()})
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if !v.GetValid() {
		t.Fatal("fresh access token must validate")
	}
	if v.GetUserId() != reg.GetUserId() || v.GetEmail() != "alice@example.com" {
		t.Errorf("claims: got (%q, %q), want (%q, %q)",
			v.GetUserId(), v.GetEmail(), reg.GetUserId(), "alice@example.com")
	}
	if !slices.Contains(v.GetRoles(), "USER") {
		t.Errorf("roles %v: want USER", v.GetRoles())
	}
	if !slices.Contains(v.GetPermissions(), "users:read") {
		t.Errorf("permissions %v: want users:read", v.GetPermissions())
	}

	// Garbage token -> invalid, not an error.
	v, err = env.Auth.ValidateToken(ctx, &genauth.ValidateTokenRequest{AccessToken: "garbage"})
	if err != nil {
		t.Fatalf("ValidateToken(garbage): %v", err)
	}
	if v.GetValid() {
		t.Error("garbage token must not validate")
	}

	// CheckPermission: USER has users:read but not admin:*.
	cp, err := env.Auth.CheckPermission(ctx, &genauth.CheckPermissionRequest{
		UserId: reg.GetUserId(), Permission: "users:read"})
	if err != nil {
		t.Fatalf("CheckPermission: %v", err)
	}
	if !cp.GetAllowed() {
		t.Error("USER role must be allowed users:read")
	}
	cp, err = env.Auth.CheckPermission(ctx, &genauth.CheckPermissionRequest{
		UserId: reg.GetUserId(), Permission: "admin:*"})
	if err != nil {
		t.Fatalf("CheckPermission: %v", err)
	}
	if cp.GetAllowed() {
		t.Error("USER role must not be allowed admin:*")
	}
	_, err = env.Auth.CheckPermission(ctx, &genauth.CheckPermissionRequest{
		UserId: "not-a-uuid", Permission: "users:read"})
	requireGRPCCode(t, err, codes.InvalidArgument)
}

func TestAuthUsers_RefreshRotationReuseDetection(t *testing.T) {
	check(t)
	ctx := context.Background()

	reg, err := env.Auth.Register(ctx, &genauth.RegisterRequest{
		Email: "bob@example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	pair1, err := env.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "bob@example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	// Rotate once: new pair, old refresh token is burned.
	pair2, err := env.Auth.Refresh(ctx, &genauth.RefreshRequest{RefreshToken: pair1.GetRefreshToken()})
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if pair2.GetRefreshToken() == pair1.GetRefreshToken() {
		t.Fatal("refresh rotation must issue a new refresh token")
	}

	// Reuse the old token: reuse detection revokes ALL sessions.
	_, err = env.Auth.Refresh(ctx, &genauth.RefreshRequest{RefreshToken: pair1.GetRefreshToken()})
	requireGRPCCode(t, err, codes.Unauthenticated)

	// The revocation must be committed even though the RPC failed: all of
	// bob's sessions are revoked and a security audit entry exists.
	var liveSessions int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM sessions WHERE user_id = $1::uuid AND revoked_at IS NULL`,
		reg.GetUserId()).Scan(&liveSessions); err != nil {
		t.Fatalf("count live sessions: %v", err)
	}
	if liveSessions != 0 {
		t.Errorf("live sessions after reuse: got %d, want 0", liveSessions)
	}
	var reuseAudits int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM audit_logs WHERE user_id = $1::uuid AND action = 'security_refresh_reuse'`,
		reg.GetUserId()).Scan(&reuseAudits); err != nil {
		t.Fatalf("count reuse audits: %v", err)
	}
	if reuseAudits == 0 {
		t.Error("expected a security_refresh_reuse audit entry")
	}

	// Even the newest refresh token is now revoked.
	_, err = env.Auth.Refresh(ctx, &genauth.RefreshRequest{RefreshToken: pair2.GetRefreshToken()})
	requireGRPCCode(t, err, codes.Unauthenticated)

	// A fresh login still works (reuse lockout is session-scoped).
	pair3, err := env.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "bob@example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Login after reuse lockout: %v", err)
	}

	// Logout revokes the session; the token no longer refreshes.
	if _, err := env.Auth.Logout(ctx, &genauth.LogoutRequest{
		RefreshToken: pair3.GetRefreshToken()}); err != nil {
		t.Fatalf("Logout: %v", err)
	}
	_, err = env.Auth.Refresh(ctx, &genauth.RefreshRequest{RefreshToken: pair3.GetRefreshToken()})
	requireGRPCCode(t, err, codes.Unauthenticated)
}

func TestAuthUsers_RevokeToken(t *testing.T) {
	check(t)
	ctx := context.Background()

	if _, err := env.Auth.Register(ctx, &genauth.RegisterRequest{
		Email: "carol-auth@example.com", Password: "password-123"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	pair, err := env.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "carol-auth@example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	if _, err := env.Auth.RevokeToken(ctx, &genauth.RevokeTokenRequest{
		AccessToken: pair.GetAccessToken()}); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}

	v, err := env.Auth.ValidateToken(ctx, &genauth.ValidateTokenRequest{
		AccessToken: pair.GetAccessToken()})
	if err != nil {
		t.Fatalf("ValidateToken: %v", err)
	}
	if v.GetValid() {
		t.Error("revoked token must not validate (Redis denylist)")
	}
}

// ---------------------------------------------------------------------------
// users flows
// ---------------------------------------------------------------------------

func TestAuthUsers_UsersCRUD(t *testing.T) {
	check(t)
	ctx := context.Background()

	created, err := env.Users.CreateUser(ctx, &genusers.CreateUserRequest{
		Email:       "dave@example.com",
		Password:    "password-123",
		DisplayName: "Dave",
	})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if created.GetId() == "" || created.GetEmail() != "dave@example.com" {
		t.Fatalf("CreateUser returned %+v", created)
	}

	// The new user can log in via the auth service (shared users table).
	if _, err := env.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "dave@example.com", Password: "password-123"}); err != nil {
		t.Fatalf("Login as created user: %v", err)
	}

	// Duplicate + invalid input.
	_, err = env.Users.CreateUser(ctx, &genusers.CreateUserRequest{
		Email: "dave@example.com", Password: "password-123"})
	requireGRPCCode(t, err, codes.AlreadyExists)
	_, err = env.Users.CreateUser(ctx, &genusers.CreateUserRequest{
		Email: "bad", Password: "password-123"})
	requireGRPCCode(t, err, codes.InvalidArgument)

	// Get.
	got, err := env.Users.GetUser(ctx, &genusers.GetUserRequest{Id: created.GetId()})
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if got.GetDisplayName() != "Dave" || got.GetDeleted() {
		t.Errorf("GetUser = %+v", got)
	}

	// Get: bad id and unknown id.
	_, err = env.Users.GetUser(ctx, &genusers.GetUserRequest{Id: "not-a-uuid"})
	requireGRPCCode(t, err, codes.InvalidArgument)
	_, err = env.Users.GetUser(ctx, &genusers.GetUserRequest{Id: "00000000-0000-0000-0000-000000000000"})
	requireGRPCCode(t, err, codes.NotFound)

	// Update profile fields. The users service enforces owner-or-admin
	// (AUTH-02); the gateway forwards the caller id as x-user-id metadata,
	// which the test reproduces directly.
	ownerCtx := metadata.AppendToOutgoingContext(ctx, "x-user-id", created.GetId())
	updated, err := env.Users.UpdateUser(ownerCtx, &genusers.UpdateUserRequest{
		Id:          created.GetId(),
		DisplayName: "Dave B",
		Bio:         "learning distributed systems",
		AvatarUrl:   "https://example.com/avatar.png",
	})
	if err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}
	if updated.GetDisplayName() != "Dave B" ||
		updated.GetBio() != "learning distributed systems" ||
		updated.GetAvatarUrl() != "https://example.com/avatar.png" {
		t.Errorf("UpdateUser = %+v", updated)
	}

	// Soft delete: afterwards GetUser is NotFound and re-delete is NotFound.
	if _, err := env.Users.DeleteUser(ownerCtx, &genusers.DeleteUserRequest{Id: created.GetId()}); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	_, err = env.Users.GetUser(ctx, &genusers.GetUserRequest{Id: created.GetId()})
	requireGRPCCode(t, err, codes.NotFound)
	_, err = env.Users.DeleteUser(ownerCtx, &genusers.DeleteUserRequest{Id: created.GetId()})
	requireGRPCCode(t, err, codes.NotFound)

	// A soft-deleted user can no longer log in.
	_, err = env.Auth.Login(ctx, &genauth.LoginRequest{
		Email: "dave@example.com", Password: "password-123"})
	requireGRPCCode(t, err, codes.Unauthenticated)
}

func TestAuthUsers_UsersListPagination(t *testing.T) {
	check(t)
	ctx := context.Background()

	for _, name := range []string{"erin", "frank", "grace"} {
		if _, err := env.Users.CreateUser(ctx, &genusers.CreateUserRequest{
			Email:    name + "@list.example.com",
			Password: "password-123",
		}); err != nil {
			t.Fatalf("CreateUser(%s): %v", name, err)
		}
	}

	// Page 1 of 2 with a page size of 2.
	p1, err := env.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page:        &gencommon.PageRequest{Page: 1, PageSize: 2},
		EmailFilter: "list.example.com",
	})
	if err != nil {
		t.Fatalf("ListUsers page 1: %v", err)
	}
	if len(p1.GetUsers()) != 2 || p1.GetPage().GetTotal() != 3 {
		t.Fatalf("page 1: got %d users, total %d; want 2 users, total 3",
			len(p1.GetUsers()), p1.GetPage().GetTotal())
	}

	// Page 2 has the remaining row.
	p2, err := env.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page:        &gencommon.PageRequest{Page: 2, PageSize: 2},
		EmailFilter: "list.example.com",
	})
	if err != nil {
		t.Fatalf("ListUsers page 2: %v", err)
	}
	if len(p2.GetUsers()) != 1 || p2.GetPage().GetTotal() != 3 {
		t.Fatalf("page 2: got %d users, total %d; want 1 user, total 3",
			len(p2.GetUsers()), p2.GetPage().GetTotal())
	}

	// Page size is capped at 100.
	capped, err := env.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page: &gencommon.PageRequest{Page: 1, PageSize: 500},
	})
	if err != nil {
		t.Fatalf("ListUsers capped: %v", err)
	}
	if capped.GetPage().GetPageSize() != 100 {
		t.Errorf("page size: got %d, want 100 (capped)", capped.GetPage().GetPageSize())
	}

	// ILIKE metacharacters in the filter are escaped: "list.example%com"
	// contains a literal %, which no email has, so it matches nothing.
	none, err := env.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page:        &gencommon.PageRequest{Page: 1, PageSize: 10},
		EmailFilter: `list.example%com`,
	})
	if err != nil {
		t.Fatalf("ListUsers escaped filter: %v", err)
	}
	if none.GetPage().GetTotal() != 0 {
		t.Errorf("escaped filter: got total %d, want 0", none.GetPage().GetTotal())
	}

	// Soft delete hides the row unless include_deleted is set.
	var graceID string
	for _, u := range p1.GetUsers() {
		if strings.HasPrefix(u.GetEmail(), "grace@") {
			graceID = u.GetId()
		}
	}
	if graceID == "" {
		for _, u := range p2.GetUsers() {
			if strings.HasPrefix(u.GetEmail(), "grace@") {
				graceID = u.GetId()
			}
		}
	}
	if graceID == "" {
		t.Fatal("grace@list.example.com not found in listing")
	}
	// grace deletes her own account (owner-or-admin enforcement, AUTH-02).
	graceCtx := metadata.AppendToOutgoingContext(ctx, "x-user-id", graceID)
	if _, err := env.Users.DeleteUser(graceCtx, &genusers.DeleteUserRequest{Id: graceID}); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	visible, err := env.Users.ListUsers(ctx, &genusers.ListUsersRequest{
		Page:        &gencommon.PageRequest{Page: 1, PageSize: 10},
		EmailFilter: "list.example.com",
	})
	if err != nil {
		t.Fatalf("ListUsers after delete: %v", err)
	}
	if visible.GetPage().GetTotal() != 2 {
		t.Errorf("after delete: got total %d, want 2", visible.GetPage().GetTotal())
	}

	// include_deleted is admin-only (AUTH-04): register and promote a caller
	// to ADMIN directly in the database (no public role-grant RPC exists).
	adm, err := env.Auth.Register(ctx, &genauth.RegisterRequest{
		Email: "listadmin@example.com", Password: "password-123"})
	if err != nil {
		t.Fatalf("Register admin: %v", err)
	}
	if _, err := env.pool.Exec(ctx, `
		INSERT INTO user_roles (user_id, role_id)
		SELECT $1::uuid, id FROM roles WHERE name = 'ADMIN'
		ON CONFLICT DO NOTHING`, adm.GetUserId()); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	adminCtx := metadata.AppendToOutgoingContext(ctx, "x-user-id", adm.GetUserId())

	withDeleted, err := env.Users.ListUsers(adminCtx, &genusers.ListUsersRequest{
		Page:           &gencommon.PageRequest{Page: 1, PageSize: 10},
		EmailFilter:    "list.example.com",
		IncludeDeleted: true,
	})
	if err != nil {
		t.Fatalf("ListUsers include_deleted: %v", err)
	}
	if withDeleted.GetPage().GetTotal() != 3 {
		t.Errorf("include_deleted: got total %d, want 3", withDeleted.GetPage().GetTotal())
	}
	foundDeleted := false
	for _, u := range withDeleted.GetUsers() {
		if u.GetId() == graceID && u.GetDeleted() {
			foundDeleted = true
		}
	}
	if !foundDeleted {
		t.Error("include_deleted listing must mark grace as deleted")
	}
}
