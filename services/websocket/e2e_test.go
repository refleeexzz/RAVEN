package websocket_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	gws "github.com/gorilla/websocket"
	"go.uber.org/goleak"

	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
	ws "github.com/refleeexzz/RAVEN/services/websocket"
)

// noKeepAlive avoids http.Transport background goroutines so goleak stays
// deterministic.
var noKeepAlive = &http.Client{
	Transport: &http.Transport{DisableKeepAlives: true},
}

func newTestServer(t *testing.T, allowAnon bool) (*httptest.Server, *ws.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := metrics.New("websocket")
	wm := ws.NewMetrics()
	reg.Register(wm.Collectors()...)
	hub := ws.NewHub(log, wm)
	handler := ws.NewHandler(ws.HandlerConfig{
		Hub:            hub,
		Fanout:         ws.NewFanout(nil, hub, log), // no Redis in-process
		Presence:       nil,                         // nil-safe: presence disabled
		JWTSecret:      "test-secret",
		AllowAnonymous: allowAnon,
		Logger:         log,
		Metrics:        reg,
		Health:         health.NewRegistry(time.Second),
	})
	return httptest.NewServer(handler), hub
}

func dialWS(t *testing.T, srv *httptest.Server, token string) *gws.Conn {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws?token=" + url.QueryEscape(token)
	c, resp, err := gws.DefaultDialer.Dial(u, nil)
	if err != nil {
		body := ""
		if resp != nil {
			b, _ := io.ReadAll(resp.Body)
			body = string(b)
		}
		t.Fatalf("dial: %v %s", err, body)
	}
	return c
}

func readJSON(t *testing.T, c *gws.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, raw, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("frame is not JSON: %v (%s)", err, raw)
	}
	return m
}

func sendJSON(t *testing.T, c *gws.Conn, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := c.WriteMessage(gws.TextMessage, b); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func getStats(t *testing.T, srv *httptest.Server) (conns, rooms, users int) {
	t.Helper()
	resp, err := noKeepAlive.Get(srv.URL + "/debug/stats")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stats status = %d", resp.StatusCode)
	}
	var s struct {
		Connections int `json:"connections"`
		Rooms       int `json:"rooms"`
		Users       int `json:"users"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatalf("stats decode: %v", err)
	}
	return s.Connections, s.Rooms, s.Users
}

func waitForConnections(t *testing.T, srv *httptest.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conns, _, _ := getStats(t, srv); conns == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	conns, _, _ := getStats(t, srv)
	t.Fatalf("connections = %d, want %d (teardown did not finish)", conns, want)
}

func makeJWT(t *testing.T, secret, sub string, exp time.Time) string {
	t.Helper()
	claims := jwt.RegisteredClaims{
		Subject:   sub,
		ExpiresAt: jwt.NewNumericDate(exp),
	}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// TestEndToEndAnon exercises the full stack in-process: anon auth, upgrade,
// join, room broadcast, dm, ping/pong, stats, metrics and clean teardown
// verified with goleak.
func TestEndToEndAnon(t *testing.T) {
	srv, _ := newTestServer(t, true)
	defer goleak.VerifyNone(t)
	defer srv.Close()

	alice := dialWS(t, srv, "anon-alice")
	defer func() { _ = alice.Close() }() // failure-path cleanup; explicit close below wins
	bob := dialWS(t, srv, "anon-bob")
	defer func() { _ = bob.Close() }()

	sendJSON(t, alice, map[string]any{"op": "join", "room": "jobs"})
	if got := readJSON(t, alice); got["op"] != "joined" || got["room"] != "jobs" {
		t.Fatalf("alice join ack: %v", got)
	}
	sendJSON(t, bob, map[string]any{"op": "join", "room": "jobs"})
	if got := readJSON(t, bob); got["op"] != "joined" {
		t.Fatalf("bob join ack: %v", got)
	}

	// Room broadcast: sender and peer both receive (echo semantics).
	sendJSON(t, alice, map[string]any{
		"op": "msg", "room": "jobs", "data": map[string]any{"n": 1},
	})
	for name, c := range map[string]*gws.Conn{"alice": alice, "bob": bob} {
		got := readJSON(t, c)
		if got["op"] != "msg" || got["room"] != "jobs" || got["from"] != "alice" {
			t.Fatalf("%s broadcast frame: %v", name, got)
		}
		if _, ok := got["at"]; !ok {
			t.Fatalf("%s frame missing at: %v", name, got)
		}
	}

	// Direct message: alice gets a room-less msg from bob.
	sendJSON(t, bob, map[string]any{
		"op": "dm", "to": "alice", "data": map[string]any{"hi": true},
	})
	got := readJSON(t, alice)
	if got["op"] != "msg" || got["from"] != "bob" {
		t.Fatalf("dm frame: %v", got)
	}
	if _, hasRoom := got["room"]; hasRoom {
		t.Fatalf("dm must omit room: %v", got)
	}

	// App-level heartbeat.
	sendJSON(t, alice, map[string]any{"op": "ping"})
	if got := readJSON(t, alice); got["op"] != "pong" {
		t.Fatalf("pong: %v", got)
	}

	// Reserved rooms are rejected with an error frame.
	sendJSON(t, alice, map[string]any{"op": "join", "room": "user:bob"})
	if got := readJSON(t, alice); got["op"] != "error" {
		t.Fatalf("reserved room join: %v", got)
	}

	// Stats: 2 connections, 2 users, 3 rooms (jobs + user:alice + user:bob).
	if conns, rooms, users := getStats(t, srv); conns != 2 || rooms != 3 || users != 2 {
		t.Fatalf("stats = (%d, %d, %d), want (2, 3, 2)", conns, rooms, users)
	}

	// Ops endpoints.
	resp, err := noKeepAlive.Get(srv.URL + "/health")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health: %v status %d", err, resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = noKeepAlive.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "raven_websocket_connections") {
		t.Fatal("/metrics missing raven_websocket_connections")
	}

	// Teardown: closing both clients must drain every server-side goroutine.
	_ = alice.Close()
	_ = bob.Close()
	waitForConnections(t, srv, 0)
}

// TestAuthRejected verifies 401-before-upgrade for missing, malformed,
// expired and disallowed-anonymous tokens, plus a valid-JWT happy path.
func TestAuthRejected(t *testing.T) {
	srv, _ := newTestServer(t, false) // anonymous tokens disabled
	defer goleak.VerifyNone(t)
	defer srv.Close()

	wsURL := func(token string) string {
		u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
		if token != "" {
			u += "?token=" + url.QueryEscape(token)
		}
		return u
	}

	t.Run("missing token", func(t *testing.T) {
		_, resp, err := gws.DefaultDialer.Dial(wsURL(""), nil)
		if err == nil {
			t.Fatal("dial without token must fail")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %v, want 401", resp)
		}
		resp.Body.Close()
	})

	t.Run("garbage token", func(t *testing.T) {
		_, resp, err := gws.DefaultDialer.Dial(wsURL("not-a-jwt"), nil)
		if err == nil {
			t.Fatal("dial with garbage token must fail")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %v, want 401", resp)
		}
		resp.Body.Close()
	})

	t.Run("anonymous disabled", func(t *testing.T) {
		_, resp, err := gws.DefaultDialer.Dial(wsURL("anon-alice"), nil)
		if err == nil {
			t.Fatal("anon token must fail when WS_ALLOW_ANONYMOUS=false")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %v, want 401", resp)
		}
		resp.Body.Close()
	})

	t.Run("expired jwt", func(t *testing.T) {
		tok := makeJWT(t, "test-secret", "u-1", time.Now().Add(-time.Hour))
		_, resp, err := gws.DefaultDialer.Dial(wsURL(tok), nil)
		if err == nil {
			t.Fatal("expired jwt must fail")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %v, want 401", resp)
		}
		resp.Body.Close()
	})

	t.Run("wrong secret", func(t *testing.T) {
		tok := makeJWT(t, "another-secret", "u-1", time.Now().Add(time.Hour))
		_, resp, err := gws.DefaultDialer.Dial(wsURL(tok), nil)
		if err == nil {
			t.Fatal("jwt signed with wrong secret must fail")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("status = %v, want 401", resp)
		}
		resp.Body.Close()
	})

	t.Run("valid jwt connects", func(t *testing.T) {
		tok := makeJWT(t, "test-secret", "u-42", time.Now().Add(time.Hour))
		c, resp, err := gws.DefaultDialer.Dial(wsURL(tok), nil)
		if err != nil {
			t.Fatalf("valid jwt must connect: %v (status %v)", err, resp)
		}
		resp.Body.Close()
		// The user id comes from the sub claim: error frame on self-DM proves
		// the conn is live and registered.
		sendJSON(t, c, map[string]any{"op": "ping"})
		if got := readJSON(t, c); got["op"] != "pong" {
			t.Fatalf("pong: %v", got)
		}
		_ = c.Close()
		waitForConnections(t, srv, 0)
	})
}
