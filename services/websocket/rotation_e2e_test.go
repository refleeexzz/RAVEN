package websocket_test

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	ws "github.com/refleeexzz/RAVEN/services/websocket"
)

// newRotationTestServer builds the full handler with a JWT rotation window
// open: primary "ws-new-secret", previous "ws-retiring-secret".
func newRotationTestServer(t *testing.T) (*httptest.Server, *ws.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := metrics.New("websocket-rotation-test")
	wm := ws.NewMetrics()
	reg.Register(wm.Collectors()...)
	hub := ws.NewHub(log, wm)
	handler := ws.NewHandler(ws.HandlerConfig{
		Hub:               hub,
		Fanout:            ws.NewFanout(nil, hub, log), // no Redis in-process
		Presence:          nil,                         // nil-safe: presence disabled
		JWTSecret:         "ws-new-secret",
		JWTSecretPrevious: "ws-retiring-secret",
		Logger:            log,
		Metrics:           reg,
		Health:            health.NewRegistry(time.Second),
	})
	return httptest.NewServer(handler), hub
}

// TestEndToEndJWTRotationWindow proves the websocket service verifies tokens
// through the dual-secret verifier: a token signed with JWT_SECRET_PREVIOUS
// still upgrades during the rotation window and the fallback is counted on
// /metrics; the primary secret works without touching the counter; unknown
// secrets stay rejected.
func TestEndToEndJWTRotationWindow(t *testing.T) {
	srv, _ := newRotationTestServer(t)
	defer goleak.VerifyNone(t)
	defer srv.Close()

	expiry := time.Now().Add(5 * time.Minute)

	// Token minted just before the rotation: signed with the PREVIOUS
	// secret. It must still open a working connection.
	prev := dialWS(t, srv, makeJWT(t, "ws-retiring-secret", "user-before-rotation", expiry))
	sendJSON(t, prev, map[string]any{"op": "ping"})
	if got := readJSON(t, prev); got["op"] != "pong" {
		t.Fatalf("previous-secret conn ping: %v", got)
	}
	_ = prev.Close()

	// Token signed with the PRIMARY secret: the normal post-rotation path.
	prim := dialWS(t, srv, makeJWT(t, "ws-new-secret", "user-after-rotation", expiry))
	sendJSON(t, prim, map[string]any{"op": "ping"})
	if got := readJSON(t, prim); got["op"] != "pong" {
		t.Fatalf("primary-secret conn ping: %v", got)
	}
	_ = prim.Close()

	waitForConnections(t, srv, 0)

	// Unknown secrets are rejected before the upgrade, exactly as before.
	u := srv.URL + "/ws?token=" +
		url.QueryEscape(makeJWT(t, "ws-unknown-secret", "intruder", expiry))
	resp, err := noKeepAlive.Get(u)
	if err != nil {
		t.Fatalf("GET /ws with unknown-secret token: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatalf("unknown-secret token: status = %d, want 401", resp.StatusCode)
	}

	// The rotation metric is registered on /metrics and counted exactly the
	// one previous-secret authentication above.
	resp, err = noKeepAlive.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "raven_auth_jwt_previous_secret_used_total 1") {
		t.Fatalf("/metrics missing raven_auth_jwt_previous_secret_used_total 1, body:\n%s", body)
	}
}
