// cors.go adds permissive CORS headers so the RAVEN Console (served from
// another localhost port) can call the API from a browser. This is a dev
// platform decision: browsers only enforce CORS on fetch/XHR, and the API
// is authenticated by Bearer header, not cookies, so Allow-Origin "*" is
// safe here. Tighten to an origin list if this ever ships for real.
package gateway

import "net/http"

// cors allows any origin, the headers the console sends, and answers
// preflight OPTIONS directly (204) without hitting the route chain.
// It never wraps the ResponseWriter, so http.Hijacker (websocket upgrade)
// keeps working through it.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key, X-Request-ID")
		h.Set("Access-Control-Max-Age", "300")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
