package websocket

import "net/http"

// secureHeaders sets the same edge security baseline as the gateway on the
// service's HTTP responses (ops endpoints and pre-upgrade rejections). The
// /ws handshake itself is unaffected: gorilla writes the 101 response after
// hijacking, and these headers are meaningless (and harmless) there.
//
//	X-Content-Type-Options: nosniff        — no MIME sniffing, JSON stays JSON
//	X-Frame-Options: DENY                  — never frame the API (clickjacking)
//	Referrer-Policy: no-referrer           — never leak the ?token= URL
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
