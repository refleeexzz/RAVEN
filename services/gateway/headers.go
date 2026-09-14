// headers.go sets the baseline browser-security headers on every gateway
// response. The API speaks JSON only, but error paths and the ops endpoints
// must not rely on that: without nosniff a browser can be coaxed into
// rendering a response as HTML, and without Referrer-Policy a browser
// navigating away from an error page would leak the full URL — including
// the ?token= query used by the websocket handshake — in the Referer header.
package gateway

import "net/http"

// secureHeaders returns middleware that sets the edge security baseline:
//
//	X-Content-Type-Options: nosniff        — no MIME sniffing, JSON stays JSON
//	X-Frame-Options: DENY                  — the API must never be framed (clickjacking)
//	Referrer-Policy: no-referrer           — URLs (and their query strings) never leak
//	Content-Security-Policy: default-src 'none' — an API response is never a document
func secureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'")
		next.ServeHTTP(w, r)
	})
}
