//go:build integration

// audit_test.go runs the platform audit trail end to end: a real gateway
// (with the audit middleware wired to real Postgres via migration 000006)
// in front of the real auth + users gRPC services, all against the shared
// testcontainers environment from TestMain. Covered:
//
//   - login success/failure emit auth.login.* events
//   - mutations emit events with the right outcome (success and denied)
//   - secrets never reach the trail
//   - GET /api/audit is admin-only (users:delete), with filters + pagination
//   - migration 000006 goes down and back up cleanly
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"

	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	authsvc "github.com/refleeexzz/RAVEN/services/auth"
	gatewaysvc "github.com/refleeexzz/RAVEN/services/gateway"
	userssvc "github.com/refleeexzz/RAVEN/services/users"
)

const gatewayAddr = "127.0.0.1:18099"

var (
	auditMigOnce sync.Once
	auditMigErr  error
)

// applyAuditMigration applies migrations/000006_audit_events.up.sql once
// per package run. The shared setup() only applies 000001, and the audit
// table belongs to this suite.
func applyAuditMigration(t *testing.T) {
	t.Helper()
	auditMigOnce.Do(func() {
		auditMigErr = execMigrationFile("000006_audit_events.up.sql")
	})
	if auditMigErr != nil {
		t.Fatalf("apply migration 000006: %v", auditMigErr)
	}
}

// execMigrationFile runs one migration file verbatim on the shared
// Postgres. Like applyMigration in auth_users_test.go it needs simple
// protocol mode (multi-statement file) and a short connect retry for the
// Windows Docker port proxy.
func execMigrationFile(name string) error {
	dsn := env.pool.Config().ConnString()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	sql, err := os.ReadFile(filepath.Join("..", "..", "migrations", name))
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			time.Sleep(300 * time.Millisecond)
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
	return fmt.Errorf("exec %s: %w", name, lastErr)
}

// auditGateway boots a real gateway wired to fresh auth/users gRPC
// listeners (own addresses, full control) on the shared containers.
func startAuditGateway(t *testing.T) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	authLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen auth: %v", err)
	}
	authGRPC := grpc.NewServer()
	genauth.RegisterAuthServiceServer(authGRPC, authsvc.NewServer(
		env.pool, env.rdb, log, jwtTestSecret, bcrypt.MinCost,
		authsvc.NewServiceMetrics(metrics.New("auth-audit-it"))))
	go func() { _ = authGRPC.Serve(authLis) }()
	t.Cleanup(authGRPC.Stop)

	usersLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen users: %v", err)
	}
	usersGRPC := grpc.NewServer()
	genusers.RegisterUserServiceServer(usersGRPC, userssvc.NewServer(
		env.pool, log, bcrypt.MinCost,
		userssvc.NewServiceMetrics(metrics.New("users-audit-it"))))
	go func() { _ = usersGRPC.Serve(usersLis) }()
	t.Cleanup(usersGRPC.Stop)

	gwCtx, stopGW := context.WithCancel(context.Background())
	gwDone := make(chan error, 1)
	go func() {
		gwDone <- gatewaysvc.Run(gwCtx, gatewaysvc.Config{
			HTTPAddr:         gatewayAddr,
			AuthGRPCAddr:     authLis.Addr().String(),
			UsersGRPCAddr:    usersLis.Addr().String(),
			JobsGRPCAddr:     "127.0.0.1:1", // lazy dial, unused by this suite
			WSAddr:           "http://127.0.0.1:1",
			BrokerOpsAddr:    "http://127.0.0.1:1",
			RedisAddr:        env.rdb.Options().Addr,
			JWTSecret:        jwtTestSecret,
			LogLevel:         "error",
			RateLimitRPM:     100_000,
			RateLimitBurst:   100_000,
			AuditDatabaseURL: env.pool.Config().ConnString(),
		})
	}()
	t.Cleanup(func() {
		stopGW()
		if err := <-gwDone; err != nil {
			t.Errorf("gateway run: %v", err)
		}
	})

	waitForGateway(t)
}

func waitForGateway(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := auditHTTP.Get("http://" + gatewayAddr + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("gateway did not come up on " + gatewayAddr)
}

var auditHTTP = &http.Client{Timeout: 10 * time.Second}

// doJSON fires one JSON request at the gateway and returns status + body.
func doJSON(t *testing.T, method, path, body, bearer string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+gatewayAddr+path, rdr)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := auditHTTP.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	_ = json.NewDecoder(resp.Body).Decode(&out) // empty/error bodies stay {}
	return resp.StatusCode, out
}

// auditEventRow is one audit_events row as the test reads it.
type auditEventRow struct {
	Action       string
	Outcome      string
	ActorID      string
	ResourceType string
	ResourceID   string
	TraceID      string
	Detail       string
}

// queryAuditRows returns every event whose actor is one of the test's
// identities, oldest first.
func queryAuditRows(t *testing.T, actors ...string) []auditEventRow {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	placeholders := make([]string, len(actors))
	args := make([]any, len(actors))
	for i, a := range actors {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = a
	}
	rows, err := env.pool.Query(ctx,
		`SELECT action, outcome, actor_id, resource_type, resource_id, trace_id, detail::text
		 FROM audit_events
		 WHERE actor_id IN (`+strings.Join(placeholders, ",")+`) ORDER BY id`, args...)
	if err != nil {
		t.Fatalf("query audit_events: %v", err)
	}
	defer rows.Close()

	var out []auditEventRow
	for rows.Next() {
		var r auditEventRow
		if err := rows.Scan(&r.Action, &r.Outcome, &r.ActorID, &r.ResourceType,
			&r.ResourceID, &r.TraceID, &r.Detail); err != nil {
			t.Fatalf("scan audit event: %v", err)
		}
		out = append(out, r)
	}
	return out
}

// waitForAuditRows polls until at least want events exist (the writer
// flushes asynchronously: batch size 100 or every 2s).
func waitForAuditRows(t *testing.T, want int, actors ...string) []auditEventRow {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		rows := queryAuditRows(t, actors...)
		if len(rows) >= want {
			return rows
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d audit events, got %d: %+v", want, len(rows), rows)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func findAuditRow(rows []auditEventRow, action, outcome string) *auditEventRow {
	for i := range rows {
		if rows[i].Action == action && rows[i].Outcome == outcome {
			return &rows[i]
		}
	}
	return nil
}

func TestAudit_EndToEnd(t *testing.T) {
	check(t)
	applyAuditMigration(t)
	startAuditGateway(t)

	ctx := context.Background()
	const (
		adminEmail  = "audit-admin@example.com"
		targetEmail = "audit-target@example.com"
		password    = "password-123"
	)

	// 1. Register a user (public mutation).
	code, body := doJSON(t, "POST", "/api/auth/register",
		`{"email":"`+adminEmail+`","password":"`+password+`","display_name":"Audit Admin"}`, "")
	if code != http.StatusCreated {
		t.Fatalf("register: status %d, body %v", code, body)
	}
	adminID, _ := body["user_id"].(string)
	if adminID == "" {
		t.Fatalf("register returned no user_id: %v", body)
	}

	// 2. Failed login (bad password).
	code, _ = doJSON(t, "POST", "/api/auth/login",
		`{"email":"`+adminEmail+`","password":"wrong-password"}`, "")
	if code != http.StatusUnauthorized {
		t.Fatalf("bad login: status %d, want 401", code)
	}

	// 3. Good login -> USER-role token.
	code, body = doJSON(t, "POST", "/api/auth/login",
		`{"email":"`+adminEmail+`","password":"`+password+`"}`, "")
	if code != http.StatusOK {
		t.Fatalf("login: status %d, body %v", code, body)
	}
	userToken, _ := body["access_token"].(string)
	if userToken == "" {
		t.Fatalf("login returned no access_token: %v", body)
	}

	// 4. Mutation without the required permission -> 403 denied, attributed.
	code, _ = doJSON(t, "POST", "/api/users",
		`{"email":"audit-nope@example.com","password":"`+password+`","display_name":"Nope"}`, userToken)
	if code != http.StatusForbidden {
		t.Fatalf("users.create as USER: status %d, want 403", code)
	}

	// 5. Promote to ADMIN and log in again (claims are minted at login).
	if _, err := env.pool.Exec(ctx,
		`INSERT INTO user_roles (user_id, role_id)
		 SELECT $1, r.id FROM roles r WHERE r.name = 'ADMIN'
		 ON CONFLICT DO NOTHING`, adminID); err != nil {
		t.Fatalf("grant ADMIN: %v", err)
	}
	code, body = doJSON(t, "POST", "/api/auth/login",
		`{"email":"`+adminEmail+`","password":"`+password+`"}`, "")
	if code != http.StatusOK {
		t.Fatalf("admin login: status %d", code)
	}
	adminToken, _ := body["access_token"].(string)

	// 6. Admin mutation -> success, resource id from the response.
	code, body = doJSON(t, "POST", "/api/users",
		`{"email":"`+targetEmail+`","password":"`+password+`","display_name":"Audit Target"}`, adminToken)
	if code != http.StatusCreated {
		t.Fatalf("users.create as ADMIN: status %d, body %v", code, body)
	}
	targetID, _ := body["id"].(string)
	if targetID == "" {
		t.Fatalf("users.create returned no id: %v", body)
	}

	// 7. GET /api/audit is admin-only.
	if code, _ := doJSON(t, "GET", "/api/audit", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("GET /api/audit without token: status %d, want 401", code)
	}
	if code, _ := doJSON(t, "GET", "/api/audit", "", userToken); code != http.StatusForbidden {
		t.Fatalf("GET /api/audit as USER: status %d, want 403", code)
	}

	// 8. The events must land (async writer: poll).
	rows := waitForAuditRows(t, 6, adminID, adminEmail)

	reg := findAuditRow(rows, "auth.register", "success")
	if reg == nil {
		t.Fatal("missing auth.register success event")
	}
	if reg.ActorID != adminID {
		t.Errorf("register actor = %q, want the new user id %q", reg.ActorID, adminID)
	}

	fail := findAuditRow(rows, "auth.login.failure", "failure")
	if fail == nil {
		t.Fatal("missing auth.login.failure event")
	}
	if fail.ActorID != adminEmail {
		t.Errorf("failed login actor = %q, want the attempted email %q", fail.ActorID, adminEmail)
	}

	ok := findAuditRow(rows, "auth.login.success", "success")
	if ok == nil {
		t.Fatal("missing auth.login.success event")
	}
	if ok.ActorID != adminID {
		t.Errorf("login actor = %q, want %q (parsed from the minted token)", ok.ActorID, adminID)
	}

	denied := findAuditRow(rows, "users.create", "denied")
	if denied == nil {
		t.Fatal("missing users.create denied event")
	}
	if denied.ActorID != adminID {
		t.Errorf("denied users.create actor = %q, want %q", denied.ActorID, adminID)
	}

	created := findAuditRow(rows, "users.create", "success")
	if created == nil {
		t.Fatal("missing users.create success event")
	}
	if created.ActorID != adminID || created.ResourceID != targetID {
		t.Errorf("users.create = actor %q resource %q, want %q / %q",
			created.ActorID, created.ResourceID, adminID, targetID)
	}
	if created.ResourceType != "user" {
		t.Errorf("users.create resource_type = %q, want user", created.ResourceType)
	}
	if created.TraceID == "" {
		t.Error("users.create has no trace_id")
	}

	// 9. Secrets never reach the trail — not in any row, at any depth.
	for _, r := range rows {
		if strings.Contains(r.Detail, password) || strings.Contains(r.Detail, "wrong-password") {
			t.Fatalf("audit detail leaked a password: %s", r.Detail)
		}
	}

	// 10. The read API: filters, then pagination.
	code, body = doJSON(t, "GET", "/api/audit?action=auth.login.failure", "", adminToken)
	if code != http.StatusOK {
		t.Fatalf("GET /api/audit as ADMIN: status %d, body %v", code, body)
	}
	events, _ := body["events"].([]any)
	if len(events) == 0 {
		t.Fatal("action filter returned no auth.login.failure events")
	}
	for _, raw := range events {
		ev, _ := raw.(map[string]any)
		if ev["action"] != "auth.login.failure" {
			t.Errorf("action filter leaked %v", ev["action"])
		}
	}

	code, body = doJSON(t, "GET", "/api/audit?actor="+adminID+"&limit=2", "", adminToken)
	if code != http.StatusOK {
		t.Fatalf("GET /api/audit actor filter: status %d", code)
	}
	page1, _ := body["events"].([]any)
	if len(page1) != 2 {
		t.Fatalf("limit=2 returned %d events", len(page1))
	}
	for _, raw := range page1 {
		ev, _ := raw.(map[string]any)
		if ev["actor_id"] != adminID {
			t.Errorf("actor filter leaked %v", ev["actor_id"])
		}
	}
	cursor, _ := body["next_before_id"].(float64)
	if cursor == 0 {
		t.Fatal("full page must hand back next_before_id")
	}
	code, body = doJSON(t, "GET",
		fmt.Sprintf("/api/audit?actor=%s&limit=2&before_id=%d", adminID, int64(cursor)), "", adminToken)
	if code != http.StatusOK {
		t.Fatalf("GET /api/audit page 2: status %d", code)
	}
	page2, _ := body["events"].([]any)
	if len(page2) == 0 {
		t.Fatal("page 2 is empty although page 1 was full")
	}
	firstID1, _ := page1[0].(map[string]any)["id"].(float64)
	for _, raw := range page2 {
		ev, _ := raw.(map[string]any)
		if id, _ := ev["id"].(float64); id >= firstID1 {
			t.Errorf("page 2 overlaps page 1: id %v >= %v", id, firstID1)
		}
	}
}

func TestAudit_MigrationUpDown(t *testing.T) {
	check(t)
	applyAuditMigration(t) // table present

	tableExists := func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var exists bool
		err := env.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM information_schema.tables
			 WHERE table_schema = 'public' AND table_name = 'audit_events')`).Scan(&exists)
		if err != nil {
			t.Fatalf("check table: %v", err)
		}
		return exists
	}

	if !tableExists() {
		t.Fatal("audit_events must exist before the down migration")
	}
	if err := execMigrationFile("000006_audit_events.down.sql"); err != nil {
		t.Fatalf("down migration: %v", err)
	}
	if tableExists() {
		t.Fatal("audit_events must be gone after the down migration")
	}
	if err := execMigrationFile("000006_audit_events.up.sql"); err != nil {
		t.Fatalf("re-applying up migration: %v", err)
	}
	if !tableExists() {
		t.Fatal("audit_events must be back after re-applying the up migration")
	}
}
