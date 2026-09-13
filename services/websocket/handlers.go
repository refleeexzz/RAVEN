package websocket

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"regexp"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	gws "github.com/gorilla/websocket"

	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	ravenerrors "github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// HandlerConfig wires NewHandler.
type HandlerConfig struct {
	Hub            *Hub
	Fanout         *Fanout
	Presence       *Presence
	JWTSecret      string
	AllowAnonymous bool // WS_ALLOW_ANONYMOUS: accept ?token=anon-<name> (local demos)
	Logger         *slog.Logger
	Metrics        *metrics.Registry
	Health         *health.Registry
}

// NewHandler builds the :8084 mux: the public WebSocket upgrade endpoint
// plus the standard ops endpoints from docs/contracts/ports-and-env.md.
func NewHandler(cfg HandlerConfig) http.Handler {
	h := &handler{
		hub:            cfg.Hub,
		fanout:         cfg.Fanout,
		presence:       cfg.Presence,
		allowAnonymous: cfg.AllowAnonymous,
		jwtSecret:      cfg.JWTSecret,
		log:            cfg.Logger,
		upgrader: gws.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// Browsers reach us through the gateway's /ws proxy.
			// TODO(security): enforce an origin allowlist here or at the gateway.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}

	mux := http.NewServeMux()

	// /ws must NOT be wrapped by middleware that replaces the ResponseWriter
	// (Logging, metrics.Middleware): their recorders do not implement
	// http.Hijacker, so the gorilla upgrade would fail behind them.
	mux.Handle("GET /ws", middleware.Chain(http.HandlerFunc(h.serveWS),
		middleware.RequestID,
		middleware.Recovery(cfg.Logger),
	))

	ops := func(next http.Handler) http.Handler {
		return middleware.Chain(next,
			middleware.RequestID,
			middleware.Logging(cfg.Logger),
			middleware.Recovery(cfg.Logger),
			cfg.Metrics.Middleware,
		)
	}
	mux.Handle("GET /health", ops(cfg.Health.Liveness()))
	mux.Handle("GET /ready", ops(cfg.Health.Readiness()))
	mux.Handle("GET /debug/stats", ops(http.HandlerFunc(h.serveStats)))
	// /metrics skips its own middleware to avoid self-scrape noise.
	mux.Handle("GET /metrics", middleware.Chain(cfg.Metrics.Handler(), middleware.RequestID))
	return mux
}

type handler struct {
	hub            *Hub
	fanout         *Fanout
	presence       *Presence
	allowAnonymous bool
	jwtSecret      string
	log            *slog.Logger
	upgrader       gws.Upgrader
}

// serveWS authenticates (?token=), upgrades and registers the connection.
// Auth failures are rejected with 401 BEFORE the upgrade.
func (h *handler) serveWS(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing token")
		return
	}
	userID, err := h.authenticate(token)
	if err != nil {
		logger.WithContext(r.Context(), h.log).Debug("websocket auth rejected", slog.Any("error", err))
		writeError(w, http.StatusUnauthorized, "invalid token")
		return
	}

	ws, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// gorilla already wrote the error response.
		logger.WithContext(r.Context(), h.log).Warn("websocket upgrade failed", slog.Any("error", err))
		return
	}

	c := newConn(userID, ws, h.hub, h.presence, h.fanout, h.log)
	first := h.hub.Register(c)
	h.presence.OnConnect(userID, first)
	h.log.Info("client connected",
		slog.String("conn_id", c.id),
		slog.String("user", userID),
		slog.Bool("first_for_user", first),
	)

	// One writer, one reader, one presence refresher per connection; every
	// goroutine exits on connection teardown (see conn and Presence docs).
	go c.writePump()
	go c.readPump()
	go h.presence.RefreshLoop(c)
}

// serveStats feeds the console UI: {"connections":N,"rooms":N,"users":N}.
func (h *handler) serveStats(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(h.hub.Stats())
}

// authenticate resolves a token to a user ID. In dev mode
// (WS_ALLOW_ANONYMOUS=true) "anon-<name>" tokens skip JWT validation.
func (h *handler) authenticate(token string) (string, error) {
	if h.allowAnonymous {
		if name, ok := strings.CutPrefix(token, "anon-"); ok {
			if !anonNameRE.MatchString(name) {
				return "", ravenerrors.E(ravenerrors.KindUnauthorized,
					"bad_anon_name", "invalid anonymous name", nil)
			}
			return name, nil
		}
	}
	claims, err := parseJWT(h.jwtSecret, token)
	if err != nil {
		return "", err
	}
	return claims.Subject, nil
}

var anonNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// jwtClaims are the HS256 claims RAVEN issues at login.
//
// TODO(auth): internal/auth is being built in parallel by another agent.
// Replace this local validator with that package once it lands so claim
// handling lives in exactly one place.
type jwtClaims struct {
	Email string `json:"email"`
	jwt.RegisteredClaims
}

// parseJWT validates an HS256 token with required sub and exp claims.
func parseJWT(secret, token string) (*jwtClaims, error) {
	claims := &jwtClaims{}
	_, err := jwt.ParseWithClaims(token, claims,
		func(t *jwt.Token) (any, error) { return []byte(secret), nil },
		jwt.WithValidMethods([]string{"HS256"}),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, ravenerrors.E(ravenerrors.KindUnauthorized, "bad_token", "token validation failed", err)
	}
	if claims.Subject == "" {
		return nil, ravenerrors.E(ravenerrors.KindUnauthorized, "no_subject", "token has no subject", nil)
	}
	return claims, nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
