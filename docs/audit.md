# Audit trail

RAVEN keeps an append-only audit trail of every state-changing call that
crosses the API edge: who did what, on which resource, how it ended, and
which trace it belongs to. It answers the questions you ask *after*
something weird happens — "who deleted this user?", "why are there forty
failed logins against this account at 3am?" — without ever slowing the API
down.

One sentence of architecture: the gateway middleware turns every mutation
into an event, drops it into an in-memory queue, and a single background
goroutine writes batches to Postgres. Reads go through a small admin-only
endpoint.

## What gets audited (and what doesn't)

**Every mutating call (`POST`/`PUT`/`DELETE`/`PATCH`) on `/api/*` is
audited.** Every read (`GET`) is not — reads are the bulk of traffic, they
change nothing, and the access log already covers them. Auditing them
would buy noise, not signal.

The action vocabulary is derived from the route, not from handler
goodwill:

| Route                              | Action               | Resource   |
| ---------------------------------- | -------------------- | ---------- |
| `POST /api/auth/register`          | `auth.register`      | `auth`     |
| `POST /api/auth/login`             | `auth.login.*`       | `auth`     |
| `POST /api/auth/refresh`           | `auth.refresh`       | `auth`     |
| `POST /api/auth/logout`            | `auth.logout`        | `auth`     |
| `POST /api/users`                  | `users.create`       | `user`     |
| `PUT /api/users/{id}`              | `users.update`       | `user`     |
| `DELETE /api/users/{id}`           | `users.delete`       | `user`     |
| `POST /api/jobs`                   | `jobs.create`        | `job`      |
| `POST /api/jobs/{id}/cancel`       | `jobs.cancel`        | `job`      |
| `POST /api/jobs/{id}/requeue`      | `jobs.requeue`       | `job`      |

Login is special: the outcome folds into the action itself
(`auth.login.success` / `auth.login.failure`), because failed-login
hunting is the single most common audit query. Unknown future mutations
still audit — a fallback derives `<resource>.<method>` from the URL, so a
forgotten mapping is a naming quirk, never a blind spot.

## Outcomes

| HTTP status                       | Outcome   | Meaning                                  |
| --------------------------------- | --------- | ---------------------------------------- |
| 2xx                               | `success` | the change happened                      |
| 401/403 on a protected route      | `denied`  | rejected by authn/authz — still recorded |
| everything else (4xx, 5xx)        | `failure` | the change did not happen                |

Denied attempts are *attributed*: the actor-enrichment middleware sits
right after AuthN, so a 403 still records **who** tried. A 401 has no
valid identity, so the actor is `anonymous`. On the public auth routes a
401 means bad credentials, so it counts as `failure`, not `denied`.

## Who is the actor

- Protected routes: the authenticated user id from the validated token.
- `POST /api/auth/register`: the new user id from the response.
- `POST /api/auth/login`: on success, the `sub` claim of the just-minted
  access token (parsed locally with `JWT_SECRET` — the token came from our
  own auth service seconds ago, so this is safe); on failure, the
  *attempted email*, which is exactly what a security review looks for.
- Fallbacks: `anonymous` for unauthenticated calls, `system` for
  platform-internal emitters.

## What never gets audited

Secrets. The middleware snapshots the request body, then **recursively
redacts** any key matching `password`, `passphrase`, `token`, `secret`,
`authorization`, `credential` or `api[-_]key` — at any nesting depth, job
payloads included — before anything reaches the database. The response
body is only mined for ids (so `jobs.create` can point at the new job); it
is never stored, because responses can carry fresh tokens.

Each event carries `trace_id` (the `X-Request-ID` minted or propagated by
the request-id middleware), so an audit row joins straight into logs and
traces.

## Anatomy of the write path

```
request ──> auditRecord middleware ──> buffered channel (cap AUDIT_BUFFER, default 4096)
                                              │
                                    writer goroutine (internal/audit)
                                              │
                                    batch insert: 100 rows or every 2s
                                              │
                                       audit_events (Postgres, migration 000006)
```

Design rules, in order of importance:

1. **Auditing never blocks the request path.** `Emit` is a non-blocking
   channel send. If the queue is full (database slow or down), the event
   is dropped, `raven_audit_dropped_total` goes up by one, and a warn is
   logged (first drop, then every 1000th — a dead database must not turn
   into a log flood). An audit trail that can take the API down is a bug,
   not a feature.
2. **Failed inserts are dropped, not retried.** `raven_audit_insert_errors_total`
   counts them, the error is logged, and the writer moves on. Piling up
   retry buffers in memory is how observability subsystems become outages.
3. **Shutdown drains.** When the server stops accepting requests and
   finishes the in-flight ones, the writer flushes whatever is queued,
   bounded by a 10s deadline. The gateway owns the pool and stops the
   writer only after `ListenAndServe` returns, so the last audited
   mutations still land.

The middleware pair sits in the chain like this:

```
RequestID → Logging → Recovery → metrics → otel → auditRecord →
Timeout(10s) → AuthN → auditActor → AuthZ → RateLimit → handler
```

`auditRecord` wraps the timeout, so a timed-out mutation is audited with
the 503 the client actually got. `auditActor` sits between AuthN and AuthZ
so denied requests are attributed.

## Configuration

| Env var             | Default       | Purpose                                              |
| ------------------- | ------------- | ---------------------------------------------------- |
| `AUDIT_DATABASE_URL`| `DATABASE_URL`| Postgres for the trail. Unset → auditing disabled.   |
| `AUDIT_BUFFER`      | `4096`        | In-memory queue size (events, not bytes).            |

When the database URL is missing or Postgres is unreachable at startup,
the gateway boots anyway (same degraded-mode philosophy as Redis): the
middleware becomes a pass-through and `GET /api/audit` answers `503`. When
auditing is on, `/ready` includes an `audit_postgres` check.

## Metrics

| Metric                          | Type    | Meaning                                   |
| ------------------------------- | ------- | ----------------------------------------- |
| `raven_audit_dropped_total`     | counter | events lost to a full queue               |
| `raven_audit_written_total`     | counter | events successfully inserted              |
| `raven_audit_insert_errors_total`| counter | failed batch inserts (batch dropped)     |

A steady non-zero drop rate means the database can't keep up with the
mutation rate — page someone, don't restart the gateway.

## Reading the trail

```
GET /api/audit?limit=50&action=auth.login.failure&actor=u-42&before_id=9182
```

Admin-only: the route requires `users:delete`, which the seed RBAC grants
only to `ADMIN` (via `admin:*`) and `SERVICE`. Non-admin tokens get 403,
missing tokens get 401 — and yes, both attempts are themselves audited.

Parameters:

- `limit` — page size, 1–500, default 50.
- `action` — exact action match, e.g. `auth.login.failure`.
- `actor` — exact actor id match (user id, email, `anonymous`).
- `before_id` — keyset cursor: only events with `id < before_id`.

Response:

```json
{
  "events": [
    {
      "id": 9183,
      "ts": "2026-09-14T03:12:07.44Z",
      "actor_id": "mallory@x.io",
      "action": "auth.login.failure",
      "resource_type": "auth",
      "outcome": "failure",
      "ip": "203.0.113.7",
      "user_agent": "curl/8.0",
      "trace_id": "b6f1d9c2-...",
      "detail": {"route": "POST /api/auth/login", "status": 401, "body": {"email": "mallory@x.io", "password": "[REDACTED]"}}
    }
  ],
  "next_before_id": 9183
}
```

`next_before_id` appears only when the page is full; feed it back as
`before_id` for the next (older) page. Newest events always come first.

## Schema

Migration `000006` creates `audit_events` (`bigserial` id, `ts`,
`actor_id`, `action`, `resource_type`, `resource_id`, `outcome`, `ip`,
`user_agent`, `trace_id`, `detail jsonb`) with indexes on `(ts DESC)`,
`(actor_id, ts DESC)` and `(action, ts DESC)`. The down migration drops
the table.

Note there are two audit tables on purpose: `audit_logs` (migration
000001) belongs to the auth service and tracks identity-internal events
like refresh-token reuse; `audit_events` is the platform-wide edge trail
described here.

## Testing

- `internal/audit` — writer unit tests with a fake database: non-blocking
  emits against a hung DB, batch-by-size and batch-by-interval flushes,
  failed inserts counted and dropped, graceful drain on shutdown.
- `services/gateway` — middleware tests through the real chain: outcome
  mapping, actor attribution (incl. login via token `sub`), redaction and
  body restore, panic → 500 still audited, timeout → audited as 503;
  handler tests for filters, cursors and the disabled-503.
- `tests/integration` (tag `integration`, needs Docker) — full path:
  register/login/mutations against real services, events polled from real
  Postgres, admin gate on the endpoint, migration 000006 down and back up.
