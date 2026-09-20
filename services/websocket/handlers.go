package websocket

import (
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	gws "github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/health"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	ravenerrors "github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/logger"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// HandlerConfig wires NewHandler.
type HandlerConfig struct {
	Hub       *Hub
	Fanout    *Fanout
	Presence  *Presence
	JWTSecret string
	// JWTSecretPrevious (JWT_SECRET_PREVIOUS) opens the JWT rotation
	// window: tokens signed with the previous secret keep validating until
	// they expire. Empty = single-secret operation
	// (docs/security/rotation.md).
	JWTSecretPrevious string
	AllowAnonymous    bool // WS_ALLOW_ANONYMOUS: accept ?token=anon-<name> (local demos)
	// AllowedOrigins (WS_ALLOWED_ORIGINS, comma-separated) lists the exact
	// origins ("https://console.example.com") allowed to open cross-origin
	// WebSocket handshakes. Empty means: same-origin and non-browser clients
	// always pass; cross-origin passes only for anonymous dev tokens. Once
	// the list is set it is enforced for every token type.
	AllowedOrigins []string
	Logger         *slog.Logger
	Metrics        *metrics.Registry
	Health         *health.Registry
}

// NewHandler builds the :8084 mux: the public WebSocket upgrade endpoint
// plus the standard ops endpoints from docs/contracts/ports-and-env.md.
func NewHandler(cfg HandlerConfig) http.Handler {
	// Dual-secret JWT verification (docs/security/rotation.md). A missing
	// primary secret is a misconfiguration: fail closed — every real token
	// is rejected — with a loud boot log, instead of validating tokens
	// against an empty key.
	var (
		verifier *ravenauth.Verifier
		counter  prometheus.Counter
	)
	if cfg.Metrics != nil {
		counter = ravenauth.NewPreviousSecretUsedCounter()
		cfg.Metrics.Register(counter)
	}
	verifier, err := ravenauth.NewVerifier(cfg.JWTSecret, cfg.JWTSecretPrevious, counter)
	if err != nil {
		cfg.Logger.Error("JWT secret not configured; all token authentications will be rejected",
			slog.Any("error", err))
	} else if verifier.HasPrevious() {
		cfg.Logger.Info("JWT rotation window open: verifying against JWT_SECRET_PREVIOUS as fallback",
			slog.String("metric", "raven_auth_jwt_previous_secret_used_total"))
	}

	h := &handler{
		hub:            cfg.Hub,
		fanout:         cfg.Fanout,
		presence:       cfg.Presence,
		allowAnonymous: cfg.AllowAnonymous,
		allowedOrigins: canonicalOrigins(cfg.AllowedOrigins),
		jwt:            verifier,
		log:            cfg.Logger,
		upgrader: gws.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			// Origin policy is enforced in serveWS before the upgrade (it
			// depends on the presented token, which CheckOrigin cannot see),
			// so the gorilla check itself stays permissive.
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
	return secureHeaders(mux)
}

type handler struct {
	hub            *Hub
	fanout         *Fanout
	presence       *Presence
	allowAnonymous bool
	allowedOrigins map[string]struct{}
	jwt            *ravenauth.Verifier // nil = misconfigured secret, fail closed
	log            *slog.Logger
	upgrader       gws.Upgrader
}

// serveWS authenticates (?token=), checks the Origin policy, upgrades and
// registers the connection. Auth failures are rejected with 401 and origin
// failures with 403, both BEFORE the upgrade.
func (h *handler) serveWS(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		writeError(w, http.StatusUnauthorized, "missing token")
		return
	}
	userID, anon, err := h.authenticate(token)
	if err != nil {
		logger.WithContext(r.Context(), h.log).Debug("websocket auth rejected", slog.Any("error", err))
		writeError(w, http.StatusUnauthorized, "invalid token")
		return
	}
	if !h.originAllowed(r, anon) {
		logger.WithContext(r.Context(), h.log).Warn("websocket origin rejected",
			slog.String("origin", r.Header.Get("Origin")),
			slog.String("user", userID),
			slog.Bool("anonymous", anon),
		)
		writeError(w, http.StatusForbidden, "origin not allowed")
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

// authenticate resolves a token to a user ID and reports whether the token
// was an anonymous dev token. In dev mode (WS_ALLOW_ANONYMOUS=true)
// "anon-<name>" tokens skip JWT validation.
func (h *handler) authenticate(token string) (userID string, anonymous bool, err error) {
	if h.allowAnonymous {
		if name, ok := strings.CutPrefix(token, "anon-"); ok {
			if !anonNameRE.MatchString(name) {
				return "", false, ravenerrors.E(ravenerrors.KindUnauthorized,
					"bad_anon_name", "invalid anonymous name", nil)
			}
			return name, true, nil
		}
	}
	if h.jwt == nil {
		return "", false, ravenerrors.E(ravenerrors.KindUnauthorized,
			"jwt_not_configured", "token validation is not configured", nil)
	}
	claims, err := h.jwt.ParseAccessToken(token)
	if err != nil {
		return "", false, err
	}
	if claims.Subject == "" {
		return "", false, ravenerrors.E(ravenerrors.KindUnauthorized,
			"no_subject", "token has no subject", nil)
	}
	return claims.Subject, false, nil
}

// canonicalOrigins normalizes the configured allowlist to
// "scheme://host[:port]" in lowercase for exact comparison.
func canonicalOrigins(origins []string) map[string]struct{} {
	set := make(map[string]struct{}, len(origins))
	for _, o := range origins {
		o = strings.TrimSpace(o)
		if o == "" {
			continue
		}
		u, err := url.Parse(o)
		if err != nil || u.Scheme == "" || u.Host == "" {
			continue // a malformed entry simply never matches
		}
		set[strings.ToLower(u.Scheme+"://"+u.Host)] = struct{}{}
	}
	return set
}

// originAllowed enforces the cross-site WebSocket hijacking policy. The
// token-in-query design means a leaked or guessed token can be ridden by any
// web page unless cross-origin handshakes are denied, so:
//
//   - no Origin header (non-browser clients) → allow;
//   - Origin host matches the request Host (same-origin console) → allow;
//   - Origin in WS_ALLOWED_ORIGINS → allow;
//   - allowlist configured but not matched → deny, even for anon tokens;
//   - no allowlist configured → cross-origin is allowed only for anonymous
//     dev tokens, never for real JWTs.
func (h *handler) originAllowed(r *http.Request, anonymous bool) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if sameOriginHost(u, r.Host) {
		return true
	}
	if _, ok := h.allowedOrigins[strings.ToLower(u.Scheme+"://"+u.Host)]; ok {
		return true
	}
	if len(h.allowedOrigins) > 0 {
		return false
	}
	return anonymous
}

// sameOriginHost compares the Origin host with the request Host, tolerating
// an omitted default port ("http://example.com" vs Host "example.com:80").
func sameOriginHost(origin *url.URL, reqHost string) bool {
	if strings.EqualFold(origin.Host, reqHost) {
		return true
	}
	if origin.Port() != "" {
		return false
	}
	host, port, err := net.SplitHostPort(reqHost)
	if err != nil {
		host, port = reqHost, ""
	}
	defaultPort := port == "" ||
		(origin.Scheme == "http" && port == "80") ||
		(origin.Scheme == "https" && port == "443")
	return defaultPort && strings.EqualFold(origin.Hostname(), host)
}

var anonNameRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
