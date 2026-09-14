# Security audit — identity layer (auth / users / internal/auth)

Pre-release offensive-security pass over `services/auth`, `services/users` and
`internal/auth`, done before RAVEN goes open-source. Method: read the code,
model the token lifecycle, then attack it with executable tests. Every attack
lives in `tests/security/` (package `security`):

- `auth_test.go` — no infrastructure needed, runs in the default `go test ./...`.
- `auth_integration_test.go` — `//go:build integration`, runs the real gRPC
  services against real Postgres + Redis in testcontainers.

Verification commands used for this audit:

```sh
go build ./services/auth/... ./services/users/... ./internal/auth/...
go vet  ./services/auth/... ./services/users/... ./internal/auth/...
gofmt -l services/auth services/users internal/auth tests/security
go test       ./tests/security/ ./services/auth/... ./services/users/... ./internal/auth/
go test -race ./tests/security/ ./services/auth/... ./services/users/... ./internal/auth/
go test -tags=integration ./tests/integration/ ./tests/security/ -timeout 10m
```

Token lifecycle threat model, in one breath: `Register`/`Login` bcrypt-hash the
password into `users`, mint a 15-minute HS256 access JWT (random `jti`) plus a
32-byte `crypto/rand` refresh token (only its sha256 lands in `sessions`),
`Refresh` rotates the session under `SELECT ... FOR UPDATE` with reuse
detection, `Logout`/`RevokeToken` kill the session / push the `jti` into a
Redis denylist whose TTL is the token's remaining life, and the gateway calls
`ValidateToken` on every protected request.

---

## Real findings — found, reproduced, fixed

### AUTH-01 — high — Login timing oracle leaks which emails are registered

- **Impact:** the unknown-email path returned after a bare DB lookup (~2 ms)
  while the wrong-password path paid a full bcrypt compare (~200 ms at the
  production cost of 12). Any remote attacker could enumerate registered
  emails with a stopwatch — no logspam, no lockout, just latency. Measured
  pre-fix: wrong-password mean **202.2 ms** vs unknown-email mean **1.8 ms**
  (a 115× gap, unambiguous even through internet noise).
- **Scope:** `services/auth` `Login`; the error *messages* were already
  identical — only timing told the truth.
- **Reproduction:** register one user, then time `Login` for a known email
  with a wrong password vs a never-registered email, on a server running
  bcrypt cost 12.
- **Regression test:** `TestSecure_LoginTimingNoOracle` (integration; fails
  pre-fix, passes post-fix with means within 2×), backed by
  `TestSecure_LoginErrorMessagesIdentical` for the message side.
- **Fix:** `NewServer` pre-builds a dummy bcrypt hash at the configured cost;
  the unknown-email branch of `Login` runs a real compare against it before
  answering. Both paths now cost exactly one bcrypt compare. Post-fix
  measured: **202.8 ms vs 202.4 ms**.

### AUTH-02 — high — IDOR/BOLA: users service accepted updates and deletes for ANY user

- **Impact:** `UpdateUser` and `DeleteUser` performed zero authorization.
  Anyone able to reach the gRPC port — or any authenticated user if a gateway
  route ever under-protected — could rewrite or soft-delete any profile by
  uuid. The gateway forwards the authenticated caller as `x-user-id` metadata
  on every call; the users service simply ignored it.
- **Scope:** `services/users` `UpdateUser`, `DeleteUser`.
- **Reproduction:** register users A and B; call
  `UpdateUser(id=B, display_name="pwned")` with caller A's identity (or no
  identity at all) — it succeeded. Same for `DeleteUser`.
- **Regression tests:** `TestSecure_UsersUpdateIDOR`, `TestSecure_UsersDeleteIDOR`
  (integration; both fail pre-fix). Matrix pinned: non-admin cross-user →
  `PermissionDenied`, anonymous → `Unauthenticated`, forged random uuid →
  `PermissionDenied`, owner → OK, ADMIN → OK.
- **Fix:** owner-or-admin enforcement inside the service. `callerIdentity`
  reads and normalizes the forwarded `x-user-id`; `authorizeTarget` allows
  the call when caller == target, otherwise checks the caller's permissions
  (`users:write` / `users:delete`; ADMIN matches via the `admin:*` wildcard)
  against the shared RBAC tables, read fresh — no cache on the authorization
  path. Trust-boundary caveat: a caller with direct network access to the
  gRPC port can still spoof `x-user-id`. That is the platform's documented
  internal-trust model (the jobs service already trusts the same header for
  job ownership); hardening it (mTLS or signed internal tokens) is a
  platform-wide decision, not an identity-layer one.

### AUTH-03 — medium — bcrypt cost floor too weak: AUTH_BCRYPT_COST 4–9 silently produced weak hashes

- **Impact:** `HashPassword` only clamped costs *below* `bcrypt.MinCost` (4)
  up to the default 12. An explicit misconfiguration like
  `AUTH_BCRYPT_COST=5` sailed straight through, and the old comment claimed
  the opposite ("a misconfigured env var can never weaken production
  hashing"). Weak hashes turn a future database leak into instant password
  recovery.
- **Scope:** `internal/auth` `HashPassword` (used by both `services/auth` and
  `services/users`).
- **Reproduction:** `HashPassword("password-123", 5)` → `bcrypt.Cost(hash) == 5`.
- **Regression test:** `TestSecure_BcryptCostFloor` (unit; fails pre-fix for
  configured costs 4, 5, 9).
- **Fix:** new `MinBcryptCost = 10` floor. Costs below `bcrypt.MinCost`
  (unset/garbage env) still fall back to the production default 12;
  explicitly configured values in [4, 10) are raised to 10. Cost 10 stays
  fast enough for tests (~50–80 ms per hash), so no test escape hatch is
  needed — the floor holds everywhere.

### AUTH-04 — low — `include_deleted` honored for any caller: soft-deleted profiles exposed

- **Impact:** `ListUsers(IncludeDeleted: true)` was served to anyone. The
  code comments call the flag "tests and future admin tooling", but every
  authenticated user holds `users:read` and the gateway forwards the raw
  `?include_deleted=` query flag — so soft-deleted users' email, bio and
  avatar stayed readable by the whole platform.
- **Scope:** `services/users` `ListUsers`.
- **Reproduction:** delete a user, then list with `include_deleted=true` as
  a plain non-admin caller — the deleted row came back.
- **Regression test:** `TestSecure_UsersIncludeDeletedRequiresAdmin`
  (integration; fails pre-fix). Pins: non-admin → `PermissionDenied`,
  anonymous → `Unauthenticated`, ADMIN → sees the row flagged `deleted`.
- **Fix:** the flag now requires the `users:delete` permission (held by
  ADMIN and SERVICE only). Active-profile reads stay platform-public — see
  AUTH-14 for that design decision.

---

## Tested, NOT vulnerable

### AUTH-05 — info — JWT algorithm confusion / signature bypass

`ParseAccessToken` pins HS256 twice: `jwt.WithValidMethods(["HS256"])` *and* a
keyfunc that rejects anything whose method is not `*jwt.SigningMethodHMAC`.
Attacks thrown at it, all rejected with `KindUnauthorized` and
`ValidateToken{Valid:false}`:

- `alg=none` token with an empty signature;
- `alg=RS256` signed with an attacker RSA key (the classic confusion attack
  against HMAC-secret servers);
- `alg=ES256` signed with an attacker ECDSA key;
- `alg=HS384` signed with the *real* secret (in-family downgrade).

Tests: `TestSecure_JWTAlgorithmConfusion` (unit, service level included).
Dependency note: `github.com/golang-jwt/jwt/v5` v5.3.1 — current, no known
algorithm-confusion advisories.

### AUTH-06 — info — JWT claim tampering / expiration bypass

- Payload swapped to `roles:["ADMIN"], perms:["admin:*"]` while keeping the
  original signature → rejected (signature mismatch).
- Forged payload re-signed with a guessable secret (`dev-only-secret-change-me`)
  → rejected whenever the real secret differs. (The dev default itself is
  AUTH-15.)
- `exp` missing entirely → rejected (`WithExpirationRequired`).
- `exp` in the past → rejected with the dedicated `token_expired` code.
- Fresh tokens validate with no leeway false-positives (jwt/v5 applies no
  default leeway).

Tests: `TestSecure_JWTClaimTampering`, `TestSecure_JWTExpirationBypass`.

### AUTH-07 — info — refresh-token reuse, rotation race, session fixation

- **Rotation race:** 20 goroutines refreshing the *same* token concurrently →
  exactly **1** succeeds, 19 get `Unauthenticated`; verified under the race
  detector. The `SELECT ... FOR UPDATE` row lock serializes contenders; each
  loser sees the revoked row and trips reuse detection. Documented
  consequence: the losers' reuse detection revokes the *whole session family*
  — including the winner's fresh session — so a client race forces a fresh
  login. That is the safe direction: a doubly-presented token is
  indistinguishable from token theft.
- **Sequential reuse:** presenting a rotated-out token revokes all sessions
  of the user and writes a `security_refresh_reuse` audit row; the revocation
  commits even though the RPC fails (existing
  `TestAuthUsers_RefreshRotationReuseDetection` keeps covering this).
- **Expired / never-issued tokens** → `Unauthenticated` (expired sessions say
  so in the message).
- **Session fixation:** not applicable — tokens are server-minted from
  `crypto/rand`; clients never supply session identifiers.

Tests: `TestSecure_RefreshRotationConcurrency` (integration, `-race`-clean),
`TestSecure_RefreshExpiredAndUnknown`, `TestSecure_RefreshTokenShape`.

### AUTH-08 — info — session revocation / denylist

Positive path works: a revoked `jti` stops validating immediately, and the
Redis entry's TTL equals the token's remaining lifetime (bounded by the
15-minute access TTL), so the denylist cannot grow unbounded and revoked
tokens can never outlive their natural expiry. Test: `TestSecure_RevocationDenylist`.

The fail-open side of this is a *documented trade-off*, see AUTH-13.

### AUTH-09 — info — SQL injection

Every query in `services/auth` and `services/users` is pgx-parameterized
(`$n`); no string-built SQL anywhere in the identity layer. The one free-text
input — the `ListUsers` email filter — goes through `escapeLike` with
`ESCAPE '\'` kept in lockstep, and `ORDER BY` / pagination are static or
bound parameters. Payload battery (`' OR '1'='1`, `%`, `_`, `\`,
`'; DROP TABLE users; --`, …) matches zero rows; SQL metacharacters in emails
are rejected by validation, and in display names round-trip as inert data.
Tests: `TestSecure_SQLiEmailFilter`, `TestSecure_SQLiIdentifiers`.

### AUTH-10 — info — mass assignment

Not reachable by construction: the request protos (`RegisterRequest`,
`LoginRequest`, `CreateUserRequest`, `UpdateUserRequest`, …) carry only their
intended fields — no `roles`, `id`, `created_at`, `deleted` or
`password_hash` — and proto3 decoding drops unknown fields, so extra JSON
keys from a client have nowhere to land. Role assignment is hardcoded to
`USER` inside the registration transaction. The field lists are pinned so a
future proto edit can't smuggle a privileged field in silently.
Test: `TestSecure_ProtoMassAssignmentSurface`.

### AUTH-11 — info — refresh-token entropy, hashing, comparison timing

Tokens are 32 bytes of `crypto/rand`, base64url (43 chars), unique across
logins; only the sha256 hex (64 chars) is stored, so a database leak does not
leak usable tokens. Tests: `TestSecure_RefreshTokenShape`,
`TestSecure_RefreshTokenHashing`. Constant-time comparison is deliberately
*not* needed on this path: the presented token is hashed first and compared
by a unique-index lookup inside Postgres, so there is no byte-wise oracle on
stored hashes (standard practice for high-entropy tokens).

### AUTH-12 — info — pagination / integer abuse

`normalizePage` clamps to page ≥ 1 and 1 ≤ page_size ≤ 100; the offset
`(page-1)*pageSize` fits int64 even at `int32` max. Negative, zero and huge
values degrade to defaults or an empty page — no error, no wraparound.
Test: `TestSecure_PaginationBounds`.

---

## Documented design trade-offs (not bugs — do not inflate)

### AUTH-13 — info / documented — revocation denylist fails open when Redis is down

If Redis is unreachable, `ValidateToken` accepts cryptographically valid
tokens instead of taking the platform down with the cache. Exposure window is
bounded by the 15-minute access-token TTL; `RevokeToken` without Redis is a
successful no-op. This is the availability-over-revocation choice, made
explicit in code and pinned by tests so nobody "helpfully" flips it silently:
`TestSecure_DenylistFailOpen`, `TestSecure_RevokeWithoutRedis`.

### AUTH-14 — info / documented — active profiles are platform-public BY DESIGN

`GetUser` / `ListUsers` (active users) require no owner check at the service:
profiles are public to any authenticated platform user, like GitHub profiles.
The gateway enforces `users:read` on those routes, and the internal gRPC
network is the trust boundary. *Writes* are owner-or-admin (AUTH-02);
*deleted* profiles are admin-only (AUTH-04). If the product ever wants
private profiles, that is a product decision with a visibility flag — not a
patch.

### AUTH-15 — info / documented — hardcoded development JWT secret

`JWT_SECRET` falls back to `dev-only-secret-change-me` (cmd/auth, docker
Compose, k8s example manifest). The value is self-documenting and fine for
local dev, but nothing refuses to boot with it in a production deploy.
Recommendation for the platform owners: switch to `config.MustGet("JWT_SECRET")`
(or an explicit `ENV=prod` guard) before the first real deployment. Left
unfixed here because `cmd/` and `deployments/` are outside this audit's
write scope.

### AUTH-16 — info / documented — password reset does not exist (no stub to attack)

There is no reset RPC, table, or flow in the identity layer — the feature is
simply absent, so there is nothing to predict or replay. When it is built,
copy the refresh-token pattern: single-use, short-TTL, `crypto/rand` tokens,
sha256 at rest, generic "if this email exists…" responses (same
anti-enumeration rule as AUTH-01).

### AUTH-17 — info / documented — error disclosure / stack traces: reviewed clean

Both services map errors through `toStatus`, which serializes only
`code: client-safe message`; wrapped causes (driver errors, DSNs, internals)
never reach the wire, and foreign errors collapse to `Internal: internal error`.
The recovery interceptor turns panics into a generic Internal. Pinned by the
pre-existing `TestToStatus` (services/auth).

### AUTH-18 — info / documented — secret leakage in logs: reviewed clean

Grep + read-through of the identity layer: no password, token, hash, or
secret is ever logged. Failed-login audit metadata stores the attempted email
and a reason code in the internal `audit_logs` table — intentional, it is the
detection surface for enumeration attempts. RPC logging carries method,
duration and error only.

### AUTH-19 — info / documented — permission cache staleness; parameter pollution n/a

`CheckPermission` caches permissions in Redis for 30 s (`perms:` keys); a
permission revocation can take up to 30 s to propagate. Bounded, deliberate
(DB is the source of truth, cache is latency-only), and the AUTH-02 fix
deliberately does **not** use this cache on the write-authorization path.
HTTP parameter pollution is out of scope for these services: they speak gRPC
only; the REST edge belongs to the gateway audit.

---

## Summary

| ID | Title | Severity | Status |
|---|---|---|---|
| AUTH-01 | Login timing oracle (account enumeration) | high | fixed |
| AUTH-02 | IDOR/BOLA on users Update/Delete | high | fixed |
| AUTH-03 | bcrypt cost floor 4–9 accepted | medium | fixed |
| AUTH-04 | include_deleted honored for any caller | low | fixed |
| AUTH-05 | JWT algorithm confusion / signature bypass | info | not vulnerable |
| AUTH-06 | JWT claim tampering / expiration bypass | info | not vulnerable |
| AUTH-07 | Refresh reuse / rotation race / fixation | info | not vulnerable |
| AUTH-08 | Session revocation / denylist TTL | info | not vulnerable |
| AUTH-09 | SQL injection | info | not vulnerable |
| AUTH-10 | Mass assignment | info | not vulnerable |
| AUTH-11 | Token entropy / hashing / compare timing | info | not vulnerable |
| AUTH-12 | Pagination integer abuse | info | not vulnerable |
| AUTH-13 | Denylist fail-open on Redis outage | info | documented |
| AUTH-14 | Platform-public profiles (reads) | info | documented |
| AUTH-15 | Dev JWT secret default | info | documented |
| AUTH-16 | Password reset absent | info | documented |
| AUTH-17 | Error disclosure / stack traces | info | documented (clean) |
| AUTH-18 | Secret leakage in logs | info | documented (clean) |
| AUTH-19 | Perms cache staleness / HPP n/a | info | documented |

Audit-date note: the identity-layer gates above are green. The combined
`tests/security` package additionally contains sibling suites owned by the
gateway/jobs/broker audits; their red/green state is tracked in their own
registers.
