// proxy.go is the reverse proxy that forwards /ws (and /ws/*) to the
// websocket service. The stdlib ReverseProxy handles the Upgrade dance
// itself, as long as nothing wraps the ResponseWriter — wrappers that lack
// http.Hijacker break upgrades, which is why this branch skips the logging,
// metrics and timeout middleware (see server.go).
package gateway

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/logger"
)

// newWSProxy builds a reverse proxy to the websocket service at wsAddr
// (e.g. "http://localhost:8084"). The path and the query string pass through
// untouched — the websocket service does its own auth via ?token=.
func newWSProxy(wsAddr string, log *slog.Logger) (http.Handler, error) {
	target, err := url.Parse(wsAddr)
	if err != nil {
		return nil, fmt.Errorf("gateway: parse WS_ADDR %q: %w", wsAddr, err)
	}
	if target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("gateway: WS_ADDR %q must be an absolute URL like http://localhost:8084", wsAddr)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	// A dedicated transport with NO ResponseHeaderTimeout: these connections
	// are long-lived websockets, and a header timeout would kill them.
	proxy.Transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout: 10 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		logger.WithContext(r.Context(), log).Warn("websocket proxy error",
			slog.Any("error", err))
		// If the upgrade already happened the writer is hijacked and this
		// write is a harmless no-op.
		writeError(w, r, errors.E(errors.KindUnavailable, "websocket_upstream_unavailable",
			"websocket service is unavailable", err))
	}
	return proxy, nil
}
