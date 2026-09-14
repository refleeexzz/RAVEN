# RAVEN API reference

The public surface of the platform. Everything below goes through the
gateway on `:8080` — it is the only application port you can reach from
outside the Docker network. Routes and permissions match
[contracts/ports-and-env.md](contracts/ports-and-env.md) and the gateway
route table in `services/gateway/routes.go`.

Base URL: `http://localhost:8080`

## Conventions

### Auth

Protected routes want `Authorization: Bearer <access_token>`. Access tokens
live 15 minutes; use `POST /api/auth/refresh` to get a new pair. The
permission column in each table below is the RBAC permission the route
checks after validating the token (`users:read`, `jobs:create`, ...). The
roles seed:

| Role | Permissions |
|------|-------------|
| `USER` (default at registration) | `users:read`, `jobs:create`, `jobs:read`, `jobs:cancel` |
| `WORKER` | `jobs:read` |
| `ADMIN` | `admin:*` (wildcard — everything) |
| `SERVICE` | every named permission |

So a freshly registered user can create and watch jobs, read users, and
cancel — but cannot create/update/delete users. That needs `users:write` /
`users:delete`, i.e. an admin.

There is a second credential scheme for machines: `Authorization: ApiKey
rav_live_...`. An API key authenticates as its owner with the key's
**scopes** as the permission set — a key with `["jobs:read"]` can list jobs
but gets `403` on `POST /api/jobs`. Keys are created over JWT auth at
`POST /api/keys` (see [API keys](#api-keys)); revocation takes effect
immediately (the key path is deliberately not cached).

### Error envelope

Every error, from any route, has the same shape:

```json
{
  "error": {
    "code": "job_not_found",
    "message": "job does not exist",
    "request_id": "0f7b2c..."
  }
}
```

`code` is a stable snake_case machine string. `message` is client-safe prose
(raw upstream errors are never forwarded — they can contain internal
addresses). `request_id` matches the `request_id` field in the gateway's
JSON logs.

Status mapping: invalid input → `400`, auth problems → `401`, missing
permission → `403`, missing resource → `404`, conflicts (e.g. cancelling a
running job) → `409`, rate limit → `429`, upstream down or circuit open →
`503`, anything else → `500`.

### Rate limiting

100 requests/minute with a burst of 20, token bucket, per user id when
authenticated and per client IP otherwise (`RATE_LIMIT_RPM` /
`RATE_LIMIT_BURST` env vars). Over the limit you get:

```http
HTTP/1.1 429 Too Many Requests
Retry-After: 1

{"error":{"code":"rate_limited","message":"too many requests, slow down and try again","request_id":"..."}}
```

`Retry-After` is whole seconds until at least one new token exists.

Buckets are **shared across gateway replicas**: with the default
`RATE_LIMIT_STORE=redis` the bucket state lives in Redis, updated by an
atomic Lua token-bucket script (clock: Redis `TIME`), so N replicas enforce
one budget per key — not N × the limit. `RATE_LIMIT_STORE=memory` restores
the old per-process behavior for single-node or air-gapped runs.

Fail-open: if Redis errors, the limiter degrades to in-process buckets for
30 s per replica (circuit breaker, self-healing) and counts
`raven_gateway_rate_limit_fallback_total`. Rate limiting is a protective
control, not a correctness one — a Redis blip must not take the API down.
See [api-keys-rate-limit.md](api-keys-rate-limit.md).

### Pagination

List endpoints take `?page=` (1-based, default 1) and `?page_size=`
(default 20, capped at 100) and answer with a `page` object:

```json
{"page": {"page": 1, "page_size": 20, "total": 137}}
```

## Auth

### `POST /api/auth/register`

Public. Creates a user with the `USER` role and an empty profile, in one
transaction.

Request:

```json
{
  "email": "me@example.com",
  "password": "correct horse battery",
  "display_name": "Me"
}
```

Rules: valid-looking email (max 254 chars), password 8–72 characters.
`display_name` optional.

Response `201`:

```json
{
  "user_id": "3f6b1c8e-....",
  "email": "me@example.com"
}
```

Errors: `400 email_invalid` / `password_too_short` / `password_too_long`,
`409` when the email is taken (case-insensitive — emails are `citext`).

### `POST /api/auth/login`

Public. Returns a token pair.

Request:

```json
{
  "email": "me@example.com",
  "password": "correct horse battery"
}
```

Response `200`:

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "refresh_token": "q9d8f7a...",
  "access_expires_at": 1760000900,
  "refresh_expires_at": 1760604800
}
```

The two `*_expires_at` fields are Unix timestamps. Unknown email and wrong
password return the same `401 invalid_credentials` — telling them apart
would leak which emails are registered.

### `POST /api/auth/refresh`

Public. Rotates the pair: the presented refresh token dies, a fresh pair
comes back.

Request:

```json
{ "refresh_token": "q9d8f7a..." }
```

Response `200`: same shape as login.

Errors: `401 refresh_token_invalid` (unknown), `401 refresh_token_expired`
(older than 7 days), and `401 refresh_token_reused` — presenting a token
whose session was already rotated. Reuse is treated as "this token family
may be stolen": **all sessions of that user are revoked** and a security
audit row is written.

### `POST /api/auth/logout`

Authenticated (any valid access token). Body carries the refresh token whose
session should die. Idempotent: unknown or already-revoked sessions still
return `200 {"ok": true}`, so clients can retry safely.

```json
{ "refresh_token": "q9d8f7a..." }
```

## API keys

Programmatic credentials for scripts and CI (migration 000007). A key is
shown **exactly once** at creation; only its SHA-256 hash is stored. All
three routes need any valid credential — but `POST` is **JWT-only**: an API
key cannot mint more API keys.

Key format: `rav_live_<43 base64url chars>` (256 bits of entropy). Scopes
are a subset of the platform permissions — `users:read`, `users:write`,
`users:delete`, `jobs:create`, `jobs:read`, `jobs:cancel`. `admin:*` is
never issuable: a key with a wildcard would be a root credential that never
expires, which is exactly what keys exist to avoid.

### `POST /api/keys` — authenticated (JWT only)

Request:

```json
{
  "name": "ci deploy bot",
  "scopes": ["jobs:create", "jobs:read"]
}
```

Response `201` — save `key`, it is never shown again:

```json
{
  "key": "rav_live_9f2kQ...",
  "api_key": {
    "id": "7c9e...",
    "name": "ci deploy bot",
    "prefix": "rav_live_9f2kQp2m",
    "scopes": ["jobs:create", "jobs:read"],
    "created_at": "2026-01-01T12:00:00Z",
    "last_used_at": null
  }
}
```

Errors: `400 name_required` / `scopes_required` / `scope_unknown` /
`scope_not_issuable` (`admin:*`), `403 api_key_cannot_create_keys` when an
API key tries to create keys.

### `GET /api/keys` — authenticated

Lists the caller's **active** keys (revoked ones disappear), newest first.
The hash is never returned. Owner-scoped for everyone, including admins.

Response `200`:

```json
{
  "api_keys": [
    {
      "id": "7c9e...",
      "name": "ci deploy bot",
      "prefix": "rav_live_9f2kQp2m",
      "scopes": ["jobs:read"],
      "created_at": "2026-01-01T12:00:00Z",
      "last_used_at": "2026-01-02T08:30:00Z"
    }
  ]
}
```

### `DELETE /api/keys/{id}` — authenticated

Revokes a key (soft: `revoked_at` is set, the row stays for the audit
trail). The owner revokes their own keys; an admin (`users:delete`) revokes
anyone's — that is the incident path for a leaked key. A key belonging to
someone else answers `404 api_key_not_found`, never `403`: no ownership
oracle. Revocation is effective immediately.

Response `200`: `{"ok": true}`

When the gateway runs without a keys database (`API_KEYS_DATABASE_URL` /
`DATABASE_URL` unset), all three endpoints and the `ApiKey` scheme answer
`503 api_keys_unavailable`; JWT auth is unaffected.

## Users

All routes require a token plus the listed permission.

### `GET /api/users` — `users:read`

Query params: `page`, `page_size`, `email_filter` (case-insensitive
substring, LIKE metacharacters are escaped), `include_deleted=true|false`
(default false — deletes are soft).

Response `200`:

```json
{
  "users": [
    {
      "id": "3f6b1c8e-....",
      "email": "me@example.com",
      "display_name": "Me",
      "bio": "",
      "avatar_url": "",
      "deleted": false,
      "created_at": 1759998000,
      "updated_at": 1759998000
    }
  ],
  "page": { "page": 1, "page_size": 20, "total": 1 }
}
```

### `GET /api/users/{id}` — `users:read`

Response `200`: one user object as above. `404 user_not_found` when the id
does not exist (or is soft-deleted and you did not ask for deleted rows).

### `POST /api/users` — `users:write`

Creates a user with a password — same validation as register. Handy for
admin-created accounts.

Request:

```json
{ "email": "other@example.com", "password": "12345678", "display_name": "Other" }
```

Response `201`: the user object.

### `PUT /api/users/{id}` — `users:write`

Updates profile fields only. Email, password and roles are **not** editable
here — identity belongs to the auth service.

Request:

```json
{ "display_name": "New Name", "bio": "hello", "avatar_url": "https://..." }
```

Response `200`: the updated user object.

### `DELETE /api/users/{id}` — `users:delete`

Soft delete: sets `deleted_at`, keeps the row. Response `200 {"ok": true}`.

## Jobs

### `POST /api/jobs` — `jobs:create`

Creates a job and queues it on the broker — right away, or later when
`scheduled_at` is set (see [docs/scheduling.md](scheduling.md)).

Headers: `Idempotency-Key: <string>` optional but recommended (max 255
chars). The same key always returns the same job: the gateway forwards the
header, the jobs service claims the key in Redis (24 h TTL), and a Postgres
unique index backstops it when Redis misses. Retrying a create after a
timeout is safe.

Request:

```json
{
  "type": "send_email",
  "payload": { "to": "me@example.com", "subject": "hi", "body": "from raven" },
  "priority": 5,
  "max_attempts": 4,
  "scheduled_at": 0
}
```

Rules:

- `type` must be one of `send_email`, `resize_image`, `webhook` (the types
  the worker actually has handlers for — anything else would die in the
  worker anyway, so we reject it early).
- `payload` must be a JSON object. Required fields per type:
  `send_email` needs `to`; `webhook` needs `url`; `resize_image` accepts any
  object.
- `priority` 1–9, default 5 (1 = most urgent; it picks the broker topic
  `jobs.p<priority>`). `max_attempts` 1–25, default 4.
- `scheduled_at` optional, Unix seconds. `0` or absent runs now; a future
  time creates the job in `SCHEDULED` and it is published only when the
  time comes; more than a minute in the past is rejected
  (`400 scheduled_at_in_past`).

Response `201`:

```json
{
  "id": "job_8d7f6e5c-....",
  "type": "send_email",
  "payload": { "to": "me@example.com", "subject": "hi", "body": "from raven" },
  "status": "QUEUED",
  "priority": 5,
  "attempts": 0,
  "max_attempts": 4,
  "created_at": 1759998000,
  "started_at": 0,
  "finished_at": 0,
  "error": "",
  "worker_id": "",
  "scheduled_at": 0
}
```

Timestamps are Unix seconds; `0` means "not set". Replayed jobs also carry
`replayed_from` with the source job id. If the broker is unreachable the
row is still written but marked `FAILED`, and you get
`503 broker_produce_failed` — the failure is loud, never silent.

### `GET /api/jobs` — `jobs:read`

Query params: `page`, `page_size`, `status` (accepts `queued`, `QUEUED` or
`JOB_STATUS_QUEUED`; one of `QUEUED PROCESSING SUCCESS FAILED RETRYING
CANCELLED DEAD SCHEDULED`), `type` (exact match). Ordered by priority
descending, then oldest first.

Response `200`:

```json
{
  "jobs": [ { "id": "job_...", "status": "SUCCESS", "...": "..." } ],
  "page": { "page": 1, "page_size": 20, "total": 42 }
}
```

### `GET /api/jobs/{id}` — `jobs:read`

Response `200`: one job object. `404 job_not_found`.

### `POST /api/jobs/{id}/cancel` — `jobs:cancel`

Moves `QUEUED`/`RETRYING`/`SCHEDULED` → `CANCELLED` atomically (cancelling
a scheduled job simply drops the schedule). If the job already moved on
(`PROCESSING` or a terminal state) you get `409 job_not_cancellable` with
the current status in the message. A cancelled job that is still sitting
on the broker is skipped by the worker's fence when delivered — cancel
never needs broker traffic.

Response `200`: the updated job object.

### `POST /api/jobs/{id}/requeue` — `jobs:create`

DLQ requeue: resurrects a `DEAD` job to `QUEUED`, resets attempts and
timestamps, and republishes it. `job_attempts` history is kept. Any other
status → `409 job_not_dead`. If the republish fails, the row is put back to
`DEAD` and you get `503 broker_produce_failed`.

### `POST /api/jobs/{id}/replay` — `jobs:create`

Clones the job into a NEW one: same `type`, `payload`, `priority` and
`max_attempts`, but a fresh id, zero attempts and no schedule. The clone
carries `replayed_from` with the source id for audit. Works on a job in
any state; replaying another user's job answers `404 job_not_found`.
Response `201`: the new job object.

### `GET /api/jobs/{id}/deliveries` — `jobs:read`

Webhook delivery history for one job (migration 000005), oldest first —
every attempt the worker made: successes, HTTP errors, transport errors and
egress-guard refusals. Owner-scoped exactly like `GET /api/jobs/{id}`: a
foreign job answers `404 job_not_found`. Query params: `page`, `page_size`.

Response `200`:

```json
{
  "deliveries": [
    {
      "id": 41,
      "job_id": "job_9f2k...",
      "attempt": 1,
      "url": "https://me.example/hook",
      "status_code": 200,
      "latency_ms": 83,
      "response_snippet": "{\"ok\":true}",
      "blocked": false,
      "error": "",
      "ts": 1767225600
    }
  ],
  "page": { "page": 1, "page_size": 20, "total": 1 }
}
```

`status_code` and `latency_ms` are `null` when no response came back (HTTP
error rows keep the status; transport errors and `blocked: true` refusals
never touched the wire). Full semantics in
[docs/webhook-delivery.md](webhook-delivery.md).

## Cron schedules

Recurring jobs, backed by `cron_schedules` — full semantics in
[docs/scheduling.md](scheduling.md).

### `POST /api/crons` — `jobs:create`

```json
{
  "name": "nightly-report",
  "cron_expr": "0 3 * * *",
  "type": "webhook",
  "payload": { "url": "https://example.com/hook", "event": "nightly" },
  "priority": 5,
  "enabled": true
}
```

`cron_expr` is the classic 5-field form `min hour dom month dow` (numbers,
`*`, `,`, `-`, `/`; no names). Job fields follow the same rules as
`POST /api/jobs`; invalid or never-firing expressions are rejected with
`400 cron_expr_invalid` / `400 cron_expr_never_fires`. Response `201`: the
schedule including `next_run_at`.

### `GET /api/crons` — `jobs:read`

Your schedules, paged like `GET /api/jobs` (`page`, `page_size`):

```json
{
  "crons": [ { "id": "cron_...", "cron_expr": "0 3 * * *", "next_run_at": 1767225600, "...": "..." } ],
  "page": { "page": 1, "page_size": 20, "total": 3 }
}
```

### `DELETE /api/crons/{id}` — `jobs:cancel`

Hard-deletes the schedule: it stops firing immediately. `404 cron_not_found`
for unknown or foreign ids.

### `GET /api/workers` — `jobs:read`

The live worker registry, read straight from Redis. Each worker heartbeats a
hash `worker:<id>` with a 15 s TTL every 5 s, so what you see is alive
right now:

```json
{
  "workers": [
    {
      "id": "worker-raven-worker-1-a3f2c1",
      "started_at": "2025-10-09T12:00:00Z",
      "last_heartbeat": "2025-10-09T12:04:35Z",
      "jobs_processed": "138",
      "in_flight": "2"
    }
  ]
}
```

(Values come from a Redis hash, so counters are strings.) If Redis is down
this route answers `503 worker_registry_unavailable`.

## WebSocket protocol

Connect: `GET /ws?token=<access_token>` (through the gateway, or directly on
`:8084` in dev). Auth failures are `401` **before** the upgrade. For local
demos, starting the service with `WS_ALLOW_ANONYMOUS=true` also accepts
`?token=anon-<name>` tokens.

One JSON object per text frame.

### Client → server

| Frame | Effect |
|-------|--------|
| `{"op":"join","room":"jobs"}` | join a room; server replies `joined` |
| `{"op":"leave","room":"jobs"}` | leave a room |
| `{"op":"msg","room":"jobs","data":{...}}` | broadcast `data` to the room |
| `{"op":"dm","to":"<userID>","data":{...}}` | direct message to one user |
| `{"op":"ping"}` | app-level ping; server replies `pong` |

Room names: 1–128 chars of `[A-Za-z0-9:._-]`, must start alphanumeric.
Rooms prefixed `user:` are reserved — every connection auto-joins
`user:<own id>` at connect (that is how job events addressed at you arrive),
and you cannot join/leave/publish to them directly.

### Server → client

| Frame | Meaning |
|-------|---------|
| `{"op":"joined","room":"jobs"}` | join confirmed |
| `{"op":"msg","room":"jobs","from":"<userID>","data":{...},"at":"..."}` | room message |
| `{"op":"msg","from":"<userID>","data":{...},"at":"..."}` | direct message (no `room`) |
| `{"op":"event","room":"jobs","data":{...},"at":"..."}` | platform event (job status changes land here) |
| `{"op":"presence","user":"<userID>","online":true,"at":"..."}` | someone came online / went offline |
| `{"op":"pong"}` | answer to `ping` |
| `{"op":"error","message":"..."}` | your last frame was bad |

Job status events (`data` of an `event` in the `jobs` room):

```json
{
  "type": "job_status",
  "job_id": "job_8d7f6e5c-....",
  "status": "SUCCESS",
  "worker_id": "worker-raven-worker-2-b9e1d4",
  "owner_id": "3f6b1c8e-....",
  "at": "2025-10-09T12:00:01.123Z"
}
```

The same event also goes to the `user:<owner_id>` room when an owner is
known. The gateway forwards the authenticated caller as `x-user-id` gRPC
metadata on every upstream call, and the jobs service stores it as
`owner_id`, so owner-targeted delivery works out of the box.

Limits: inbound frames are capped at 32 KiB, and each connection can send
20 messages/second with a burst of 40 — over the budget you get an `error`
frame, not a disconnect. Slow readers with a full 256-frame outbound buffer
are dropped to protect the hub. Keepalive is protocol-level ping/pong:
server pings every 30 s, drops you after 60 s of silence.

## Ops endpoints

Every service (gateway included) exposes these on its HTTP port, no auth:

| Endpoint | Returns |
|----------|---------|
| `GET /health` | `{"status":"ok"}` — liveness, always 200 while the process serves |
| `GET /ready` | 200 only when every dependency check passes (Postgres, Redis, broker, gRPC upstreams); body names the failing check |
| `GET /metrics` | Prometheus text format |

Extras: the broker's ops port (`:9101`) also has `GET /topics` (high-water
marks, committed offsets, lag per group — the console reads this), and both
the websocket service (`:8084`) and each worker (`:8085`) expose
`GET /debug/stats` with live counters (`connections`/`rooms`/`users` for
websocket; `in_flight`/`processed`/`pending_retries` for workers).
