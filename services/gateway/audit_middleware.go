// audit_middleware.go writes the platform audit trail (migration 000006,
// internal/audit) from the edge. Two small middlewares work as a pair:
//
//   - auditRecord wraps every API route OUTSIDE the timeout. It snapshots
//     the (size-capped) request body, tees the response, and after the
//     chain returns emits one event for every mutating call (POST/PUT/
//     DELETE on /api/*). Reads (GET) are deliberately not audited: they
//     are the bulk of traffic and carry no change of state — the access
//     log already covers them.
//   - auditActor sits between AuthN and AuthZ and copies the authenticated
//     user id into the in-flight event, so denied (403) attempts are still
//     attributed. 401s stay "anonymous" — there is no valid identity yet.
//
// Outcome mapping: 2xx success, 401/403 denied on protected routes,
// everything else failure. On the public auth routes a 401 means bad
// credentials, so it counts as failure (and login folds the outcome into
// the action itself: auth.login.success / auth.login.failure).
//
// Sensitive data never leaves the edge: request bodies are redacted
// recursively (any key matching password/token/secret/authorization/
// credential/api-key) before landing in detail, and the response body is
// only mined for ids — never stored.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/refleeexzz/RAVEN/internal/audit"
	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/pkg/logger"
)

// auditResponseTeeCap bounds how much of the response body is mirrored for
// id extraction. API responses are small JSON; 64 KiB is generous.
const auditResponseTeeCap = 64 << 10

// auditBodyDetailCap bounds the redacted request-body copy inside detail.
const auditBodyDetailCap = 4 << 10

// auditEmitter is the slice of *audit.Writer the middleware needs. An
// interface so middleware tests can capture events without a database.
type auditEmitter interface {
	Emit(audit.Event)
}

// auditCtxKey marks the in-flight audit event inside the request context.
type auditCtxKey struct{}

// pendingEvent is the per-request audit skeleton shared between the two
// audit middlewares. It crosses the timeout boundary: on a timed-out
// request the timeout handler returns (and auditRecord finalizes) while
// the inner chain is still unwinding, so every access goes through mu.
type pendingEvent struct {
	mu    sync.Mutex
	event audit.Event
}

func withAudit(ctx context.Context, p *pendingEvent) context.Context {
	return context.WithValue(ctx, auditCtxKey{}, p)
}

func auditFrom(ctx context.Context) *pendingEvent {
	p, _ := ctx.Value(auditCtxKey{}).(*pendingEvent)
	return p
}

func (p *pendingEvent) setActor(id string) {
	if id == "" {
		return
	}
	p.mu.Lock()
	p.event.ActorID = id
	p.mu.Unlock()
}

// auditRecord is the outer half of the pair. It must wrap the chain
// outside middleware.Timeout so a timed-out mutation is audited with the
// 503 the client actually got.
func (s *server) auditRecord(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.audit == nil || !auditableRequest(r) {
			next.ServeHTTP(w, r)
			return
		}

		// Snapshot and restore the body so the handler still reads it
		// exactly once. Bounded by the same 1 MiB the JSON decoder allows.
		var reqBody []byte
		if r.Body != nil {
			reqBody, _ = io.ReadAll(io.LimitReader(r.Body, maxJSONBody+1))
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(reqBody))
		}

		p := &pendingEvent{event: audit.Event{
			TS:        time.Now().UTC(), // request start: the honest audit timestamp
			ActorID:   audit.ActorAnonymous,
			IP:        remoteIP(r),
			UserAgent: r.UserAgent(),
			TraceID:   logger.RequestID(r.Context()),
		}}
		r = r.WithContext(withAudit(r.Context(), p))

		rec := &auditResponseRecorder{ResponseWriter: w}
		finalized := false
		defer func() {
			// Panic path: Recovery (outer) turns it into a 500; make sure
			// the event still lands, with the matching outcome.
			if !finalized {
				s.finalizeAudit(p, r, reqBody, rec, http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(rec, r)
		finalized = true
		s.finalizeAudit(p, r, reqBody, rec, rec.status)
	})
}

// auditActor is the inner half: placed right after AuthN, it attributes
// the in-flight event to the authenticated user before AuthZ can reject
// the request (which is exactly when attribution matters most).
func (s *server) auditActor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if p := auditFrom(r.Context()); p != nil {
			if id, ok := IdentityFrom(r.Context()); ok {
				p.setActor(id.UserID)
			}
		}
		next.ServeHTTP(w, r)
	})
}

// auditableRequest reports whether r is a state-changing API call.
func auditableRequest(r *http.Request) bool {
	if !strings.HasPrefix(r.URL.Path, "/api/") {
		return false
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		return true
	}
	return false
}

// finalizeAudit completes the event from the request/response data and
// emits it. It is the only place events are produced, so the mapping
// rules live here exactly once.
func (s *server) finalizeAudit(p *pendingEvent, r *http.Request, reqBody []byte, rec *auditResponseRecorder, status int) {
	pattern := r.Pattern
	if pattern == "" {
		pattern = r.Method + " " + r.URL.Path
	}

	action, resourceType := auditActionFor(pattern)
	public := isPublicAuthRoute(pattern)
	outcome := auditOutcome(status, public)
	if pattern == "POST /api/auth/login" {
		// The action carries the outcome for the security-critical route:
		// auth.login.success / auth.login.failure.
		action += "." + string(outcome)
	}

	respBody := rec.tee()
	resourceID := r.PathValue("id")
	if resourceID == "" && outcome == audit.OutcomeSuccess {
		resourceID = responseResourceID(respBody)
	}

	p.mu.Lock()
	p.event.Action = action
	p.event.ResourceType = resourceType
	p.event.ResourceID = resourceID
	p.event.Outcome = outcome
	if p.event.ActorID == audit.ActorAnonymous {
		p.event.ActorID = s.auditActorFromAuthFlow(pattern, reqBody, respBody, outcome)
	}
	p.event.Detail = auditDetail(pattern, status, r, reqBody)
	event := p.event
	p.mu.Unlock()

	s.audit.Emit(event)
}

// auditActionFor maps a route pattern onto the audit action vocabulary.
// Unknown mutations still audit: the fallback derives
// "<resource>.<method>" from the URL so a forgotten mapping never means a
// silent gap.
func auditActionFor(pattern string) (action, resourceType string) {
	switch pattern {
	case "POST /api/auth/register":
		return "auth.register", "auth"
	case "POST /api/auth/login":
		return "auth.login", "auth"
	case "POST /api/auth/refresh":
		return "auth.refresh", "auth"
	case "POST /api/auth/logout":
		return "auth.logout", "auth"
	case "POST /api/users":
		return "users.create", "user"
	case "PUT /api/users/{id}":
		return "users.update", "user"
	case "DELETE /api/users/{id}":
		return "users.delete", "user"
	case "POST /api/jobs":
		return "jobs.create", "job"
	case "POST /api/jobs/{id}/cancel":
		return "jobs.cancel", "job"
	case "POST /api/jobs/{id}/requeue":
		return "jobs.requeue", "job"
	}

	// Fallback: "POST /api/widgets/{id}/poke" -> "widgets.post", "widget".
	method, path, _ := strings.Cut(pattern, " ")
	seg := ""
	if rest, ok := strings.CutPrefix(path, "/api/"); ok {
		seg, _, _ = strings.Cut(rest, "/")
	}
	if seg == "" {
		seg = "api"
	}
	return seg + "." + strings.ToLower(method), strings.TrimSuffix(seg, "s")
}

// isPublicAuthRoute marks the routes reachable without a token, where a
// 401 means "bad credentials" (failure) instead of "not allowed" (denied).
func isPublicAuthRoute(pattern string) bool {
	switch pattern {
	case "POST /api/auth/register", "POST /api/auth/login", "POST /api/auth/refresh":
		return true
	}
	return false
}

// auditOutcome maps the HTTP status onto the audit outcome vocabulary.
func auditOutcome(status int, publicRoute bool) audit.Outcome {
	switch {
	case status >= 200 && status < 300:
		return audit.OutcomeSuccess
	case (status == http.StatusUnauthorized || status == http.StatusForbidden) && !publicRoute:
		return audit.OutcomeDenied
	default:
		return audit.OutcomeFailure
	}
}

// auditActorFromAuthFlow attributes public auth calls. Register returns
// the new user id; login signs an access token whose sub claim we parse
// locally (the gateway keeps JWT_SECRET for exactly this kind of check;
// the token was just minted by our own auth service, so trusting a
// verified parse is safe). Failures fall back to the attempted email —
// that is the actor a security review looks for.
func (s *server) auditActorFromAuthFlow(pattern string, reqBody, respBody []byte, outcome audit.Outcome) string {
	if outcome == audit.OutcomeSuccess {
		switch pattern {
		case "POST /api/auth/register":
			var resp struct {
				UserID string `json:"user_id"`
			}
			if json.Unmarshal(respBody, &resp) == nil && resp.UserID != "" {
				return resp.UserID
			}
		case "POST /api/auth/login":
			var resp tokenPairJSON
			if json.Unmarshal(respBody, &resp) == nil && resp.AccessToken != "" && s.jwtSecret != "" {
				if claims, err := ravenauth.ParseAccessToken(s.jwtSecret, resp.AccessToken); err == nil && claims.Subject != "" {
					return claims.Subject
				}
			}
		}
	}
	var req struct {
		Email string `json:"email"`
	}
	if json.Unmarshal(reqBody, &req) == nil && req.Email != "" {
		return req.Email
	}
	return audit.ActorAnonymous
}

// auditDetail builds the detail column: route pattern, final status, path
// params, and the redacted request body. The response is never included —
// it can carry fresh tokens.
func auditDetail(pattern string, status int, r *http.Request, reqBody []byte) map[string]any {
	detail := map[string]any{
		"route":  pattern,
		"status": status,
	}
	params := map[string]string{}
	// PathValue needs known names; the route table today only uses {id}.
	for _, name := range []string{"id"} {
		if v := r.PathValue(name); v != "" {
			params[name] = v
		}
	}
	if len(params) > 0 {
		detail["params"] = params
	}
	if body := redactedBodyDetail(reqBody); body != nil {
		detail["body"] = body
	}
	return detail
}

// sensitiveKeyRe marks request-body keys that must never reach the audit
// trail. Matched on lowercase: password, old_password, refresh_token,
// client_secret, authorization, api_key, credentials...
var sensitiveKeyRe = regexp.MustCompile(`password|passphrase|token|secret|authorization|credential|api[-_]?key`)

// redactedBodyDetail parses the request body as a JSON object and returns
// a recursively redacted, size-capped copy. Non-JSON or non-object bodies
// (nothing the API accepts today) yield nil.
func redactedBodyDetail(reqBody []byte) any {
	if len(reqBody) == 0 {
		return nil
	}
	var body map[string]any
	if err := json.Unmarshal(reqBody, &body); err != nil {
		return nil
	}
	if len(body) == 0 {
		return nil // "{}" carries no information
	}
	redacted := redactValue(body)
	if raw, err := json.Marshal(redacted); err != nil || len(raw) > auditBodyDetailCap {
		return map[string]any{"_truncated": true}
	}
	return redacted
}

// redactValue deep-copies v, replacing every sensitive-keyed value with
// "[REDACTED]". Maps and slices are walked recursively so secrets nested
// inside a job payload are caught too.
func redactValue(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if sensitiveKeyRe.MatchString(strings.ToLower(k)) {
				out[k] = "[REDACTED]"
				continue
			}
			out[k] = redactValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = redactValue(val)
		}
		return out
	default:
		return v
	}
}

// responseResourceID mines a successful JSON response for the id of the
// thing that was just created/changed. First match wins.
func responseResourceID(respBody []byte) string {
	if len(respBody) == 0 {
		return ""
	}
	var body map[string]any
	if err := json.Unmarshal(respBody, &body); err != nil {
		return ""
	}
	for _, key := range []string{"id", "user_id", "job_id"} {
		if v, ok := body[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}

// remoteIP strips the port from RemoteAddr, matching the rate limiter.
func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// auditResponseRecorder captures the status and tees a bounded prefix of
// the body for id extraction.
type auditResponseRecorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (r *auditResponseRecorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *auditResponseRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if room := auditResponseTeeCap - r.buf.Len(); room > 0 {
		r.buf.Write(b[:min(len(b), room)])
	}
	return r.ResponseWriter.Write(b)
}

// Flush and Unwrap keep streaming and http.ResponseController working
// through the recorder (same contract as middleware.statusRecorder).
func (r *auditResponseRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *auditResponseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

// tee returns the captured response prefix.
func (r *auditResponseRecorder) tee() []byte {
	return r.buf.Bytes()
}
