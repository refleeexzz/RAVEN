# RAVEN architecture

This doc is the full walkthrough: how a request flows through the platform,
who talks to whom and why, who owns which data, and what scales (and what
does not). Everything here matches the code as it exists today. When in
doubt, the contract file [contracts/ports-and-env.md](contracts/ports-and-env.md)
wins for ports, env vars, routes and topics.

## The cast

| Service | Role | Public | Internal |
|---------|------|--------|----------|
| gateway | REST edge, authN/Z, rate limit, resilience | `:8080` | dials auth/users/jobs over gRPC |
| auth | registration, login, tokens, RBAC | — | gRPC `:9081`, ops `:8081` |
| users | user CRUD, profiles | — | gRPC `:9082`, ops `:8082` |
| jobs | job lifecycle, idempotency, events | — | gRPC `:9083`, ops `:8083` |
| websocket | rooms, presence, push | `:8084` (usually reached via gateway `/ws`) | subscribes to Redis |
| worker | job execution pool | — | ops `:8085`, consumes broker |
| broker | message log | — | TCP `:9100`, ops `:9101` |

Infra: Postgres (5432), Redis (6379), Prometheus (9090), Grafana (3000),
Jaeger (16686 UI / 4317 OTLP).

## Request lifecycle: create a job, watch it succeed

The full path of the one thing the platform does end to end. Every step is
real code you can grep.

```text
client
  │  1. POST /api/jobs  (Bearer token, Idempotency-Key header, JSON body)
  ▼
gateway :8080
  │  middleware chain: RequestID → Logging → Recovery → metrics → otel
  │  → Timeout(10s) → AuthN → AuthZ(jobs:create) → RateLimit → handler
  │
  │  2. AuthN: auth.ValidateToken over gRPC (5s timeout; cached in memory
  │     for 30s, keyed by SHA-256 of the token)
  │  3. AuthZ: permission list from the token vs route's required perm
  ▼
jobs service (gRPC :9083)
  │  4. validate: known type? payload valid JSON? priority 1-10? attempts 1-25?
  │  5. idempotency: claim idem:<key> in Redis (NX, 24h); taken → return
  │     the existing job. Postgres unique index is the backstop.
  │  6. INSERT INTO jobs ... status QUEUED
  │  7. produce the job message to broker topic "jobs" (key = job id, so
  │     one job always lands on one partition)
  │     → produce fails: row marked FAILED, client gets 503. No silent loss.
  │  8. publish job_status event to Redis channel raven:events:jobs
  ▼
broker :9100
  │  9. append to the partition log, ack after append
  │     (durable after the next fsync: every 100ms or 256 records)
  ▼
worker (consumer group "workers")
  │  10. fetch → acquire a semaphore slot (8 per worker) → dispatch goroutine
  │  11. fence: UPDATE jobs SET status='PROCESSING', worker_id=...
  │      WHERE id=... AND status IN ('QUEUED','RETRYING')
  │      → matches nothing: cancelled or duplicate delivery, skip silently
  │  12. run the handler with a 30s timeout
  │      send_email (simulated 200-800ms) · resize_image (~300ms CPU) ·
  │      webhook (real HTTP POST, non-2xx retryable)
  │  13. success → job_attempts row + status SUCCESS (one transaction)
  │      failure → RETRYING + republish after backoff (100ms→250ms→500ms
  │      →1s→2s→4s→5s cap) or DEAD at max_attempts + copy to jobs.dlq
  │  14. every transition publishes a job_status event to Redis
  ▼
redis pub/sub channel raven:events:jobs
  │  15. the websocket service subscribes to this channel
  ▼
websocket service :8084
  │  16. fans the event out as {"op":"event","room":"jobs",...} to every
  │      connected client in the jobs room, on every replica
  ▼
client (console UI or any ws client) sees the job turn SUCCESS live
```

The client that created the job gets its `201 Created` after step 8 —
steps 9–16 happen asynchronously after that. Total wall time for a
`send_email` job is usually under two seconds.

### The gateway middleware chain, exactly

From `services/gateway/server.go`:

```text
API routes:     RequestID → Logging → Recovery → metrics → otel (if on)
                → Timeout(10s) → AuthN → AuthZ → RateLimit → handler
ops endpoints:  RequestID → Logging → Recovery → metrics        (no auth/timeout)
/ws proxy:      RequestID → Recovery                              (minimal)
```

`/ws` gets the minimal chain on purpose: the Logging/metrics/Timeout
middlewares wrap the `ResponseWriter` with recorders that do not implement
`http.Hijacker`, which would break the WebSocket upgrade. This bit me once;
now it has a comment and a test.

### Resilience parameters, exactly

These are constants in code, not aspirations:

| Knob | Value | Where |
|------|-------|-------|
| Gateway request timeout | 10 s (`/ws` exempt) | `services/gateway/server.go` |
| Per-upstream gRPC timeout | 5 s | `services/gateway/grpc_client.go` |
| Upstream retries | 2, backoff 100 ms then 250 ms, idempotent RPCs only, only on `Unavailable` | same |
| Circuit breaker | 5 consecutive transport failures → open 30 s → 1 half-open probe | `services/gateway/breaker.go` |
| Rate limit | 100 rpm, burst 20, token bucket, per user id or IP | `services/gateway/ratelimit.go` |
| Auth validation cache | 30 s TTL, 10k entries, SHA-256 token keys | `services/gateway/authn.go` |
| Request body cap | 1 MiB | `services/gateway/handlers_auth.go` |
| Access / refresh token | 15 min / 7 days, refresh rotates every use | `internal/auth/jwt.go`, `services/auth/tokens.go` |
| bcrypt cost | 12 | `internal/auth/credentials.go` |
| Worker concurrency / job timeout | 8 / 30 s | `services/worker/worker.go` |
| Worker retry backoff | 100 ms → 250 ms → 500 ms → 1 s → 2 s → 4 s → 5 s cap | `services/worker/backoff.go` |
| Broker fsync | every 100 ms or 256 records | `internal/broker/config.go` |
| Broker segment / sparse index | 64 MiB / one entry per 4 KiB | same |
| Broker session timeout | 10 s heartbeat gap → expel + rebalance | same |
| WS client rate limit | 20 msg/s, burst 40, per connection | `services/websocket/ratelimit.go` |
| WS presence TTL / refresh | 90 s / 30 s | `services/websocket/redis.go` |
| Worker registry TTL / refresh | 15 s / 5 s | `services/worker/registry.go` |

## Communication matrix

| From → To | Protocol | Why that protocol |
|-----------|----------|-------------------|
| client → gateway | HTTP/JSON (REST) | The public edge should be boring: curl-able, cacheable, debuggable, friendly to any language. |
| client → websocket service | WebSocket (via gateway `/ws` proxy) | Push needs a persistent connection; SSE can't do client→server frames and long-polling is a hack. |
| gateway → auth/users/jobs | gRPC (protobuf) | Internal calls are typed contracts. Compile-time checked, fast, and the proto files are documentation that can't drift. |
| jobs → broker | custom binary over TCP | The broker is the learning project; its protocol is built for the hot path (binary produce/fetch, JSON only for control ops). |
| worker → broker | same custom TCP | Same reason. Consumer groups are broker-side. |
| worker/jobs → Postgres | pgx (Postgres wire) | Source of truth for jobs and users. |
| auth/gateway/jobs/worker/websocket → Redis | Redis protocol (go-redis) | Ephemeral coordination: denylist, caches, pub/sub, presence, worker registry. |
| worker → jobs events → websocket | Redis pub/sub | Fanout without coupling: the worker never knows the websocket service exists. |
| services → Jaeger | OTLP gRPC | Traces, when `OTEL_ENABLED=true`. |
| Prometheus → every service | HTTP `/metrics` | Pull model; 15 s scrape interval. |

Why REST at the edge and gRPC inside: the edge speaks to strangers (browsers,
curl, scripts) and REST with JSON is the common denominator. Inside, we
control both ends, so we pay the proto tax and get typed contracts, real
deadlines, and streaming later if we want it. See [ADR 002](adr/002-use-grpc.md).

Why pub/sub for job events instead of the worker calling the websocket
service directly: the worker would need to know about websockets, handle
their failures, and slow down for them. A Redis channel costs us one hop of
latency and buys full decoupling. Publish failures are logged and dropped —
the event stream is a UI nicety, Postgres is the truth. See
[ADR 007](adr/007-event-driven-architecture.md).

## Data ownership

Each table and keyspace has exactly one owner. Other services may read, but
writes go through the owner (or, for the worker, through the owner's shared
SQL in `services/jobs/store.go` — one table, one package owning the
queries).

| Data | Storage | Owner | Who else touches it |
|------|---------|-------|---------------------|
| `users` (identity + credentials) | Postgres | auth (writes) | users (reads; updates `display_name` only) |
| `user_profiles` (bio, avatar) | Postgres | users | auth (creates the empty row at registration) |
| `roles`, `permissions`, `user_roles` | Postgres | auth | seeded by migration 000001 |
| `sessions` (refresh token hashes) | Postgres | auth | — |
| `audit_logs` | Postgres | auth | — |
| `jobs` | Postgres | jobs (the Go package) | worker (execution fields, via the same package) |
| `job_attempts` | Postgres | jobs | worker (inserts one row per attempt) |
| `idem:<key>` idempotency claims | Redis (24 h TTL) | jobs | — |
| `revoked:jti:<jti>` token denylist | Redis (token TTL) | auth | — |
| `perms:<user_id>` permission cache | Redis (30 s TTL) | auth | — |
| `worker:<id>` registry entries | Redis (15 s TTL) | worker | gateway (reads, `SCAN worker:*`) |
| `presence:user:<id>` | Redis (90 s TTL) | websocket | — |
| `raven:events:jobs` channel | Redis pub/sub | workers/jobs publish | websocket subscribes |
| `raven:ws:fanout`, `raven:ws:presence` | Redis pub/sub | websocket (inter-replica) | — |
| topic logs, `offsets.jsonl` | broker data dir | broker | — |

The shared `users` table is the one place we bent the "one owner" rule for
the demo. [ADR 008](adr/008-shared-users-table.md) explains the trade-off
and what a production system would do instead.

## Scaling story

What scales horizontally, and why it works:

- **gateway** — stateless. The rate limiter and auth cache are per-process
  (see limitations), so add replicas and put them behind a load balancer;
  k8s runs 3 and can HPA to 20 on CPU.
- **auth, users, jobs** — stateless gRPC services. Postgres is the shared
  state; scale the pods freely.
- **worker** — the scaling muscle. The consumer group (`workers`) spreads
  partitions across however many workers are alive. Compose runs 3, k8s
  runs 5 with an HPA to 20. Killing one triggers a rebalance and its
  partitions move to the survivors.
- **websocket** — replicas share fanout and presence over Redis pub/sub, so
  clients connected to different replicas still see each other's room
  messages.

What does not scale:

- **broker** — one node, one process, one disk. It is the honest bottleneck
  of the platform. Throughput is bounded by the single writer goroutine per
  partition and the disk's fsync rate. Replication is the roadmap item that
  would change this; today, if the broker dies, job creation fails fast with
  503 and workers reconnect when it comes back.
- **Postgres and Redis** — single instances. Fine for a learning platform;
  a real system would add replicas/failover here before anywhere else.

The practical ceiling: with the default 3 partitions on the `jobs` topic,
at most 3 workers actively consume at once (one partition per consumer in a
group). More workers than partitions just idle. That is a Kafka rule we kept
on purpose.

## Failure handling, in one paragraph

Retries where safe (idempotent gRPC reads), fail fast where not (job create
fails loudly instead of pretending), circuit breakers to stop piling onto a
sick upstream, fences to absorb the duplicates that at-least-once delivery
guarantees, and readiness probes so orchestrators stop sending traffic to
pods that can't serve. The full tour of what breaks and what you see when it
does is in [failure-scenarios.md](failure-scenarios.md).
