# Edge Layer Security Audit — gateway + websocket

Pre-release security pass for RAVEN's public edge: `services/gateway`,
`services/websocket`, `cmd/gateway`, `cmd/websocket`. Everything was attacked
black-box from `tests/security/gateway_test.go` (package `security`): the
gateway boots through its real `Run` entrypoint against fake gRPC upstreams,
the websocket service runs through `NewHandler` on an `httptest` server. No
external services needed; the whole suite runs in ~10 s and passes under
`-race`.

**Bottom line: 4 real findings, all fixed and regression-tested. 20+ hunt
items came back clean or documented-by-design — details below.**

| ID | Title | Severity | Status |
|----|-------|----------|--------|
| EDGE-01 | Cross-site WebSocket hijacking (CheckOrigin allowed every origin) | High | **Fixed** |
| EDGE-02 | No browser-security headers on any edge response | Medium | **Fixed** |
| EDGE-03 | Idempotency-Key not bound to payload (replay returns wrong job) | Medium | **Fixed** |
| EDGE-04 | Encoded `%2e%2e` bypassed the /ws proxy mount guard | Low | **Fixed** |
| EDGE-05 | Job events broadcast platform-wide to the public `jobs` room | Info | Documented (by design) |
| EDGE-06 | Anonymous WS tokens are full participants, not read-only | Info | Documented (dev flag, default off) |
| EDGE-07 | `/metrics` and `/debug/stats` are unauthenticated | Info | Documented (self-hosted trade-off) |
| EDGE-08 | `/ready` leaks dependency error strings | Info | Documented (fix belongs to `internal/health`) |
| EDGE-09 | Rate limiting is per-process (N replicas → N× budget) | Info | Documented |
| EDGE-10 | Auth cache accepts revoked tokens for up to 30 s | Info | Documented (trade-off) |
| EDGE-11 | No connection cap on the websocket service | Info | Documented |
| — | Clean bills | — | See "Clean bills" section |

---

## EDGE-01 — Cross-site WebSocket hijacking — High — FIXED

**Impact.** The upgrader's `CheckOrigin` returned `true` for every origin.
RAVEN authenticates sockets with `?token=` in the URL — and URLs leak
(browser history, proxy logs, copy-paste). Any web page could open an
authenticated WebSocket to a RAVEN deployment through a victim's browser once
it held a token, and could freely open anonymous connections to instances on
the victim's own machine (`ws://localhost:8084/ws?token=anon-x`), turning the
browser into an intranet relay. From there it could join rooms, read the
global job-event stream and DM users.

**Scope.** `services/websocket` (the gateway just proxies the handshake
headers through, so the fix lands in one place).

**Reproduction.** Dial `/ws?token=<valid JWT>` with header
`Origin: http://evil.example`: handshake succeeded (101) before the fix.

**Regression tests.** `TestWSCrossOriginJWTDenied`,
`TestWSAllowedOriginsAllowlist` (both failed before the fix);
`TestWSSameOriginAndNonBrowserAllowed`,
`TestWSCrossOriginAnonAllowedInDevMode` (pin the intended allow cases).

**Fix.** `serveWS` enforces an origin policy after authentication, before the
upgrade; violators get 403 (`services/websocket/handlers.go`). Policy:

- no `Origin` header (non-browser clients) → allow;
- origin host == request host (same-origin console) → allow;
- origin in the new `WS_ALLOWED_ORIGINS` env allowlist (comma-separated,
  exact `scheme://host[:port]`) → allow;
- allowlist configured but not matched → deny, even for anonymous tokens;
- no allowlist configured → cross-origin is allowed only for anonymous dev
  tokens, never for real JWTs.

Local demos keep working (`WS_ALLOW_ANONYMOUS` tokens stay permissive);
production consoles set `WS_ALLOWED_ORIGINS` or serve same-origin.

## EDGE-02 — Missing security headers — Medium — FIXED

**Impact.** No edge response carried `X-Content-Type-Options`,
`X-Frame-Options`, `Referrer-Policy` or a CSP. The API speaks JSON, but
without `nosniff` a browser can be coaxed into sniffing a response body as
HTML on some paths, error pages were frameable (clickjacking), and the
`Referer` header could leak full URLs — including the `?token=` websocket
credential — when a browser navigated away from an error response.

**Scope.** All gateway responses (`:8080`) and all websocket-service HTTP
responses (`:8084` ops endpoints and pre-upgrade rejections).

**Reproduction.** `curl -i http://localhost:8080/health` — none of the four
headers present.

**Regression tests.** `TestGatewaySecurityHeaders`,
`TestWebsocketSecurityHeaders` (both failed before the fix; they check ops
endpoints, an API 401 and the CORS preflight).

**Fix.** A `secureHeaders` middleware in each service
(`services/gateway/headers.go`, `services/websocket/headers.go`) sets
`X-Content-Type-Options: nosniff`, `X-Frame-Options: DENY`,
`Referrer-Policy: no-referrer`, `Content-Security-Policy: default-src 'none'`
on every response. Header-only (no ResponseWriter wrapping), so the `/ws`
hijack is unaffected.

## EDGE-03 — Idempotency-Key not bound to payload — Medium — FIXED

**Impact.** The gateway forwarded `Idempotency-Key` verbatim and the jobs
service dedupes on the bare key. Replaying a key with a **different** payload
returned the original job: the caller believes it enqueued B while the
platform actually runs A. That's a confused-deputy on every retried client
and every replayed request.

**Scope.** Gateway create path (`POST /api/jobs`). The jobs service's store is
the jobs auditor's; the edge-side fix below closes the hole without touching
it.

**Reproduction.** `POST /api/jobs` with `Idempotency-Key: order-42` and
payload A → job A. Repeat with the same key and payload B → response is job A
(body, status and all), job B never exists.

**Regression tests.** `TestIdempotencyKeyBoundToPayload` (failed before the
fix), `TestIdempotencyKeyAbsentStaysEmpty`, `TestIdempotencyLongKeyStillBounded`.

**Fix.** The gateway now binds the key to a SHA-256 fingerprint of the full
create semantics (`type`, `payload`, `priority`, `max_attempts`) before
forwarding: same key + same request collapses to one job as before; same key
+ different request produces a *new* job instead of the wrong one. The
forwarded key keeps the client key as a readable prefix
(`order-42:9f2a…`) and never exceeds the jobs service's 255-char contract
(over-long client keys are fully hashed). A stricter "409 on payload
mismatch" needs a payload hash stored in the jobs service — handed to the
jobs auditor as a follow-up note.

## EDGE-04 — Encoded `%2e%2e` bypasses the /ws proxy mount guard — Low — FIXED

**Impact.** Raw `/ws/../api/jobs` is cleaned by `ServeMux` with a 307 before
routing — fine. But **encoded** dot-segments (`/ws/%2e%2e/api/jobs`) skip the
cleaning (the mux matches on the escaped path), hit the `/ws/` subtree
pattern and were proxied verbatim to the websocket service. Today nothing
sensitive answers — the ws mux 404s every variant we threw at it — but the
broken invariant ("everything under `/ws` is forwarded untouched") meant any
future `/ws/`-subtree route on the ws service would silently become reachable
through the public gateway, bypassing the mount's purpose.

**Scope.** Gateway `/ws` proxy branch only. API routes are exact-match
patterns and are not affected.

**Reproduction.** Against a gateway whose ws upstream is alive:
`GET /ws/%2e%2e/api/jobs` is proxied (any upstream answer proves it; with a
dead upstream the tell is `503 websocket_upstream_unavailable` instead of a
gateway-side 4xx).

**Regression test.** `TestProxyPathTraversalIsContained` (failed before the
fix: encoded variants returned the proxy's 503).

**Fix.** `wsPathGuard` (`services/gateway/server.go`) rejects any `/ws`
request whose **decoded** path contains a `..` segment with
`400 invalid_path`. Header-only, so hijacking still works. Raw `..` keeps
being 307-cleaned by the mux and lands on the gateway's own authenticated
routes.

---

## Info / documented findings

**EDGE-05 — job events are broadcast platform-wide.** Every
`raven:events:jobs` message (including `owner_id`) is fanned out to the
public `jobs` room that any connected client can join; owner-targeted copies
additionally go to the reserved `user:<owner>` rooms. This is the documented
product design (`docs/api.md`), and the reserved-room enforcement that
protects the owner-targeted copy is regression-tested
(`TestWSReservedRoomsRejectClients`). If RAVEN ever becomes multi-tenant in a
privacy-sensitive way, this broadcast is the first thing to revisit.

**EDGE-06 — anonymous WS tokens are full participants.** With
`WS_ALLOW_ANONYMOUS=true`, `anon-<name>` connections can join arbitrary
(non-reserved) rooms, publish messages and DM any user — they are not
read-only. The flag defaults to **off** and is documented as a local-demo
switch, so this is an accepted trade-off, not a bug. Hardening suggestion for
operators who expose anon mode to untrusted networks: run a separate ws
instance for demos, or we can add a read-only-anon mode later.

**EDGE-07 — unauthenticated ops endpoints.** `GET /metrics` (gateway) and
`GET /debug/stats` + `/metrics` (websocket) need no token, per
`docs/contracts/ports-and-env.md`. Contents are operational only: Prometheus
counters/gauges with route-pattern labels, and connection/room/user counts.
The gateway's public `/api/health/services` already surfaces the ws counts by
design. For a self-hosted product this is standard; the hardening option is
network-level (firewall the port, or split ops onto a second listener) —
noted for the deployment docs, not enforced in code.

**EDGE-08 — `/ready` leaks dependency error strings.** The readiness body
includes `"fail: <err>"` from each checker, which can contain internal
addresses (e.g. a Redis dial error with an IP). Low sensitivity for a
self-hosted platform; the fix belongs to `internal/health`, which is the
infra auditor's file — flagged here so it isn't lost.

**EDGE-09 — rate limits are per-process.** Buckets live in gateway memory;
N replicas give each client N× the configured budget, and restarts reset it.
The code documents this and points at Redis for multi-replica deployments.
The limiter is keyed by the real peer address (`RemoteAddr`) —
`X-Forwarded-For` is never trusted, so public-route limits can't be dodged
by spoofing it (`TestRateLimitIgnoresXForwardedFor`). If a deployment puts
the gateway behind a trusted proxy, an opt-in `TRUST_PROXY_HEADERS` mode
would be the right follow-up.

**EDGE-10 — auth cache accepts revoked tokens for up to 30 s.** Successful
validations are cached in memory for 30 s (bounded 10k entries, SHA-256 of
the token as key — the raw token is never a map key). A token revoked
server-side stays accepted within that window. Same trade-off the auth
service documents for its own caches; intentional.

**EDGE-11 — no connection cap on the websocket service.** Each connection
costs three goroutines and a 256-frame buffer, and nothing caps the count
(the broker has `BROKER_MAX_CONNECTIONS`; the ws service doesn't). Per-connection
DoS guards are real and tested (32 KiB frame cap, 20 msg/s + burst 40 per
conn, slow-consumer eviction, 60 s heartbeat timeout), but a raw connection
flood is bounded only by OS resources. Accepted for the learning platform;
a `WS_MAX_CONNECTIONS` env gate is the suggested follow-up.

## Clean bills (verified, no finding)

- **Route table vs contract.** Every route in `services/gateway/routes.go`
  matches `docs/contracts/ports-and-env.md` exactly — methods, paths and
  permissions, including the four public routes. Verified in-code
  (`TestRouteTableMatchesContract`, existing) and black-box: every protected
  route answers `401 missing_authorization` without a token while the public
  ones answer
  (`TestProtectedRoutesRejectAnonymous`).
- **Authorization mapping.** Per-route permissions are enforced;
  `admin:*` works; the `permAuthenticated` sentinel (`<authenticated>`, used
  by logout) can never collide with a real permission and correctly means
  "any valid token" (`TestPermissionEnforcedPerRoute`). The `perm != ""` vs
  sentinel confusion was checked by reading `authz.go`: public routes skip
  AuthN entirely, everything else requires the exact permission.
- **BOLA at the edge.** The gateway builds upstream identity itself:
  `x-user-id` gRPC metadata comes from the validated token, and a
  client-supplied `X-User-ID` header is ignored
  (`TestClientSuppliedUserIDHeaderIgnored`). Owner-scoping of job *reads*
  belongs to the jobs service (jobs auditor) — the gateway forwards the right
  identity for it.
- **Rate-limit atomicity.** Token take happens under one mutex; a 30-way
  concurrent burst at the bucket boundary never over-issued
  (`TestRateLimitConcurrentBurstExact`, run under `-race`). Per-user buckets
  are isolated (`TestRateLimitPerUserIsolation`).
- **CORS.** `Access-Control-Allow-Origin: *` with Bearer-header auth and no
  cookies is not exploitable for session riding — browsers never attach the
  credential automatically. The one fatal combination (`*` +
  `Allow-Credentials: true`) is pinned as never emitted
  (`TestCORSPreflightNeverAllowsCredentials`). Code comments already say to
  tighten to an origin list if the platform ever ships multi-tenant SaaS.
- **CSRF.** Immune by design: no cookie authentication exists anywhere in the
  edge (grep-verified); Bearer headers and `?token=` are never attached
  ambiently by browsers.
- **CRLF / response splitting.** Go's HTTP parser rejects control bytes in
  header values (obs-fold, bare CR, NUL all answered 400 in
  `TestRequestIDEchoCannotSplitResponse`), so the echoed `X-Request-ID`
  cannot split the response. The same ID inside the JSON error envelope is
  `encoding/json`-escaped. Logs go through slog's JSON handler, which escapes
  newlines — no log-line forging (`pkg/logger`, verified by reading).
- **Request smuggling.** Go's `net/http` rejects requests carrying both
  `Content-Length` and `Transfer-Encoding` with 400
  (`TestTransferEncodingPlusContentLengthRejected`); the stdlib
  `ReverseProxy` strips hop-by-hop headers. No desync surface at the edge.
- **Open redirect.** No `Location` header is built from user input anywhere
  in the edge; the only redirects are ServeMux path-clean 307s to same-host
  paths.
- **XSS.** All API and error responses are `application/json`, reflected
  input is JSON-escaped (`<` → `\u003c`; `TestReflectedInputIsJSONEscaped`),
  and EDGE-02 added `nosniff` + `default-src 'none'` on top. No user string
  is ever rendered into HTML by the edge.
- **Payload-size DoS.** Every JSON body endpoint shares the 1 MiB
  `decodeJSON` cap (`TestBodySizeCap`); GET routes read no body; the `/ws`
  handshake is a GET with no body, and post-upgrade frames are capped at
  32 KiB (`TestWSFrameSizeCapEnforced`).
- **JSON bombs.** `encoding/json` caps nesting depth at 10 000, so a
  50 000-deep job payload is rejected at decode time before the upstream is
  touched (`TestDeeplyNestedPayloadRejected`).
- **Algorithmic complexity.** The only regexes (`grpcMessagePrefix`,
  `anonNameRE`, `roomNameRE`) are anchored, linear and run on capped input
  (error strings / ≤64-char names / ≤128-char room names).
- **Slowloris.** `ReadHeaderTimeout` is 5 s in `internal/httpserver`
  (verified by reading; infra's file). Handler execution is bounded by the
  10 s timeout middleware, which also covers body reads on the capped 1 MiB
  bodies. No idle/write timeouts — acceptable given the above; noted for infra.
- **Cache poisoning / key confusion.** Auth cache keys are SHA-256 hashes of
  the token; limiter keys are prefixed (`user:` / `ip:`) and cannot collide;
  only *successful* validations are cached, so a bad token can never poison
  the cache (also covered by existing `TestAuthNFailuresAreNotCached`).
- **WS authentication.** 401 before the upgrade for missing/garbage/expired/
  wrong-secret tokens; anon tokens only when the flag is on; JWT algorithm
  pinned to HS256 with required `exp` and `sub` (existing
  `TestAuthRejected` covers this; re-verified by reading).
- **WS authorization.** `user:` rooms reject client join/leave/publish
  (`TestWSReservedRoomsRejectClients`); case variants like `User:x` are not
  the reserved prefix but also receive nothing (room names are exact
  strings). Clients must join a room before publishing to it.
- **WS message injection.** The server always overwrites sender identity — a
  client-supplied `from` field is dropped (`TestWSCannotForgeSenderIdentity`).
  Clients cannot emit `event` frames: the op is rejected, and a payload
  imitating a job event still arrives as `op=msg` with the real sender
  (`TestWSClientCannotForgeEvents`). Client frames can never reach the
  `raven:events:jobs` channel — the fanout only publishes to
  `raven:ws:fanout` / `raven:ws:presence`.
- **WS DoS.** Frame cap (32 KiB, close 1009), per-connection budget
  (20 msg/s, burst 40 → error frames), 256-frame outbound buffer with
  slow-consumer eviction, 30 s/60 s ping/pong deadlines. Tested:
  `TestWSFrameSizeCapEnforced`, `TestWSMessageRateLimitEnforced`, plus the
  existing `TestSlowConsumerDrop`.
- **Stack traces / internal detail.** The error envelope only ever carries
  `code` + client-safe `message`; raw gRPC errors (dial errors, internal
  IPs) are mapped to generic text unless they match the known-safe
  `<code>: message` shape (`TestUpstreamInternalsNeverLeak`). Panic recovery
  returns a static body.
- **HTTP parameter pollution.** Duplicate query params are first-value-wins
  (`r.URL.Query().Get`), deterministic, and the values only feed pagination /
  filters. No security impact.

## Limitations

- The gateway tests run against fake gRPC upstreams that implement the
  generated contracts; upstream *business-logic* authorization (e.g. whether
  `GET /api/jobs/{id}` is owner-scoped in the jobs service) is the jobs
  auditor's ground, not covered here.
- Slowloris and `/ready` claims rest on reading `internal/httpserver` and
  `internal/health` — both owned by the infra auditor — not on fixes here.
- Rate-limit and auth-cache behavior was verified single-process (the only
  supported topology today); multi-replica semantics are documented as
  EDGE-09/EDGE-10.
- The suite avoids wall-clock sleeps where possible, but rate-limit tests do
  depend on the configured refill rates being slow relative to test
  execution (1 token/s vs milliseconds of burst) — stable in practice, and
  the assertions carry slack where a refill tick could legitimately add a
  token.
