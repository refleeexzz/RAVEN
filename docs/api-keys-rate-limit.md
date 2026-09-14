# API keys & distributed rate limiting

Two gateway upgrades that ship together: real machine credentials (API
keys) and rate limits that actually hold when you run more than one gateway
replica.

## API keys

JWTs are great for humans clicking around a console — they expire in 15
minutes and ride a refresh flow. Scripts and CI hate that. API keys are the
machine-friendly credential: long-lived, scoped, revocable in one click.

### The key itself

```
rav_live_<43 base64url chars>
```

That's `rav_live_` plus 32 bytes from `crypto/rand` (256 bits of entropy,
52 chars total). The prefix makes a RAVEN key easy to spot in a config file
or a log line — same trick as `ghp_...` or `sk-...`.

### What we store (and what we never store)

Only the **hex SHA-256 of the full key** hits the database
(`api_keys.key_hash`, migration 000007). The plaintext leaves the edge
exactly once: in the `POST /api/keys` response. Lose it and there is no
recovery path — you revoke and re-issue, which is the point.

Why a fast hash and not bcrypt? bcrypt exists to slow down brute force on
**human** passwords (low entropy). A 256-bit random key can't be brute
forced, period — so we trade the KDF cost for an indexed hash-equality
lookup that keeps authentication at one cheap indexed read per request. The
miss path still runs a constant-time compare against a dummy hash so
"exists or not" doesn't leak through response shape.

Each row also keeps:

- `key_prefix` — the first 8 chars of the key body, in the clear, so the
  key list can show `rav_live_9f2kQp2m…` and you can tell keys apart.
  8 chars of 256 bits identify nothing.
- `scopes` — the key's whole permission set.
- `last_used_at` — best-effort, updated **async** (a queue + worker
  goroutine; a full queue drops the touch). It never blocks or fails a
  request.
- `revoked_at` — soft revoke. The row stays for the audit trail; every
  read path filters it out.

### Scopes are the whole game

A key's scopes **are** its permissions, nothing else. Issuable scopes are
exactly the platform permissions — `users:read`, `users:write`,
`users:delete`, `jobs:create`, `jobs:read`, `jobs:cancel` — validated at
creation time against `internal/auth`. `admin:*` is refused flat out: a
wildcard key would be a root credential that never expires, which is
precisely the thing API keys are supposed to fix.

When a request comes in with `Authorization: ApiKey rav_live_...`, the
gateway resolves the hash and builds the same identity a JWT would have
produced: owner id + scopes, forwarded to upstreams as `x-user-id` /
`x-user-perms` gRPC metadata. Authorization, owner-scoped job reads, audit
attribution and per-user rate limiting all work exactly like they do for
JWTs.

Two deliberate choices:

- **No auth cache on the key path.** JWT validations are cached for 30 s;
  key resolutions are not. The lookup is one indexed read, and immediate
  revocation is worth more than the saved millisecond — when you revoke a
  leaked key, it dies *now*.
- **`POST /api/keys` is JWT-only.** A key cannot mint more keys. If a key
  leaks, the blast radius stays exactly the scopes you gave it — it can't
  bootstrap itself into permanent self-renewing access.

### The endpoints

| Route | Who | What |
|-------|-----|------|
| `POST /api/keys` | JWT only | creates a key, shows the plaintext once |
| `GET /api/keys` | any credential | lists your active keys (never the hash) |
| `DELETE /api/keys/{id}` | owner, or `users:delete` admin | revokes, effective immediately |

A foreign key answers `404`, never `403` — no ownership oracle. Full
request/response shapes live in [api.md](api.md#api-keys).

Running without a database (`API_KEYS_DATABASE_URL`/`DATABASE_URL` unset)?
The feature is loudly absent: `/api/keys` and the `ApiKey` scheme answer
`503 api_keys_unavailable`, and JWT auth is completely unaffected. Same
degraded-mode philosophy as the audit trail.

## Distributed rate limiting

Before this, every gateway replica had its own in-memory token buckets:
three replicas meant 3 × 100 req/min for everyone. Fine for a demo, wrong
for real deployments. Now the default is one shared budget.

### The algorithm: token bucket in a Lua script

Buckets live in Redis (`raven:ratelimit:<key>` hashes), and one **Lua
script** does the whole update — read, refill, take, write back, set TTL —
atomically inside Redis. Why this over the alternatives:

- `INCR` + `EXPIRE` is a **fixed window**: a client can fire 2× the limit
  at a window boundary, and there's no way to compute a real `Retry-After`.
- A **sliding window** needs a sorted set per key: more memory, more
  round trips, more code.
- The **Lua token bucket** keeps the exact semantics we already had —
  smooth refill, precise `Retry-After`, one round trip — and because it
  runs atomically, racing replicas can't over-issue tokens. The clock is
  Redis `TIME`, so replicas with skewed clocks still share one timeline.

Limits are unchanged: 100 req/min with burst 20 (`RATE_LIMIT_RPM` /
`RATE_LIMIT_BURST`), keyed by user id when authenticated and by client IP
otherwise. Idle buckets expire after 10 minutes, same as the old janitor.

### Fail-open, on purpose

If Redis errors, the limiter doesn't take the API down with it. A tiny
circuit breaker opens for 30 s: requests go straight to the in-process
limiter (one probe after the cooldown, self-healing), a throttled warn hits
the logs, and `raven_gateway_rate_limit_fallback_total` counts every
fallback decision so the degradation is visible in Prometheus.

Why not fail-closed (503 while Redis is down)? Because rate limiting is a
**protective** control, not a correctness one. Turning a Redis blip into a
full public API outage is a self-inflicted DoS — the attacker doesn't even
need to send traffic, they just need your cache to hiccup. The fallback
still caps abuse at N_replicas × the limit, which is exactly the behavior
we shipped for months.

One subtlety the circuit breaker prevents: probing a dead Redis on every
request adds dial latency *before* the fallback answers, and that waiting
time refills the memory buckets mid-flight — quietly widening the
effective limit. Failing fast keeps both the latency and the math honest.

### Config

| Var | Default | Meaning |
|-----|---------|---------|
| `RATE_LIMIT_STORE` | `redis` | `redis` = shared buckets; `memory` = per-process (old behavior) |
| `RATE_LIMIT_RPM` | `100` | sustained requests per minute per key |
| `RATE_LIMIT_BURST` | `20` | bucket capacity |

`memory` mode is still right for single-node or air-gapped runs — no Redis
round trip, same semantics.

## Testing notes

- Unit: in-memory fake store for keys; an in-process Redis fake (one EVAL,
  no new dependency) for the limiter, including the circuit breaker.
- Integration (`-tags=integration`, testcontainers): migration 000007 up
  and down verbatim, the full key lifecycle through the gateway's
  production `Run`, two gateway replicas proving one shared Redis budget,
  and the memory-mode control.
