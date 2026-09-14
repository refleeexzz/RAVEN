# Platform Contract — ports, env vars, endpoints

> This file is the single source of truth that keeps every service
> compatible. If you change a port, an env var name or a route here,
> update every service that depends on it. Seriously.

## Module

- Go module: `github.com/raven/platform`
- Go version: 1.27
- One binary per service: `cmd/<service>/main.go`
- Service wiring lives in `services/<service>/`
- Shared libs: `pkg/` (logger, errors, metrics, tracing), `internal/`
  (config, health, httpserver, middleware, auth, database, messaging, gen)

## Ports

| Service   | Public/ops HTTP | gRPC  | Custom TCP | Notes                          |
|-----------|-----------------|-------|------------|--------------------------------|
| gateway   | 8080            | —     | —          | only public entrypoint         |
| auth      | 8081 (ops)      | 9081  | —          |                                |
| users     | 8082 (ops)      | 9082  | —          |                                |
| jobs      | 8083 (ops)      | 9083  | —          |                                |
| websocket | 8084            | —     | —          | public via gateway `/ws` proxy |
| worker    | 8085 (ops)      | —     | —          | no inbound traffic besides ops |
| broker    | 9101 (ops)      | —     | 9100       | custom binary protocol         |

Ops HTTP on every service exposes: `GET /health`, `GET /ready`,
`GET /metrics`.

Infrastructure (from Docker Compose): postgres 5432, redis 6379,
prometheus 9090, grafana 3000, jaeger 16686 (UI) / 4317 (OTLP gRPC).

## Environment variables

| Var              | Used by            | Default (local)                                   |
|------------------|--------------------|---------------------------------------------------|
| `HTTP_ADDR`      | all                | `:8080`-style per service                         |
| `GRPC_ADDR`      | auth, users, jobs  | `:9081`-style per service                         |
| `LOG_LEVEL`      | all                | `info`                                            |
| `DATABASE_URL`   | auth, users, jobs, worker, migrate | `postgres://raven:raven@localhost:5432/raven?sslmode=disable` |
| `REDIS_ADDR`     | auth, gateway, websocket, worker   | `localhost:6379`                    |
| `JWT_SECRET`     | auth, gateway, websocket | `dev-only-secret-change-me` (never in prod)  |
| `JWT_SECRET_PREVIOUS` | auth, gateway, websocket | empty — set only during a rotation window; verification fallback while signing uses `JWT_SECRET` (see docs/security/rotation.md) |
| `BROKER_ADDR`    | jobs, worker       | `localhost:9100`                                  |
| `BROKER_DATA_DIR`| broker             | `./data`                                          |
| `BROKER_MAX_CONNECTIONS` | broker   | `1024` (extra conns get BROKER_BUSY + close)      |
| `BROKER_IDLE_TIMEOUT` | broker        | `5m` (closes silent connections)                  |
| `BROKER_WRITE_TIMEOUT` | broker       | `30s` (max duration of one frame write)           |
| `OTEL_ENABLED`   | all                | `false`                                           |
| `OTEL_ENDPOINT`  | all                | `localhost:4317`                                  |
| `WORKER_CONCURRENCY` | worker         | `8`                                               |
| `WORKER_JOB_TIMEOUT` | worker         | `30s` (keep below the lease)                      |
| `WORKER_JOB_LEASE_MS` | worker        | `30000` (milliseconds; lease claimed with each job, renewed every lease/3) |
| `JOBS_SWEEP_INTERVAL` | jobs          | `15s` (`0` disables the stranded-job sweeper — do not disable in prod) |
| `JOBS_SCHEDULER_INTERVAL` | jobs      | `1s` (delayed-job dispatcher + cron scheduler tick; `0` disables both) |
| `JOBS_LEGACY_TOPIC_FANOUT` | jobs      | `true` (mirror execution publishes onto the legacy `jobs` topic until workers subscribe to `jobs.p1..p9`) |
| `API_KEYS_DATABASE_URL` | gateway          | falls back to `DATABASE_URL`; unset disables /api/keys + the `ApiKey` scheme (503) |

Service discovery inside Docker/K8s uses service names:
`http://auth`, `auth:9081`, `broker:9100`, ...

## Public REST routes (gateway :8080)

```
POST /api/auth/register
POST /api/auth/login
POST /api/auth/refresh
POST /api/auth/logout
GET  /api/users           (auth: users:read)
GET  /api/users/{id}      (auth: users:read)
POST /api/users           (auth: users:write)
PUT  /api/users/{id}      (auth: users:write)
DELETE /api/users/{id}    (auth: users:delete)
POST /api/jobs            (auth: jobs:create)  + Idempotency-Key header
GET  /api/jobs            (auth: jobs:read)
GET  /api/jobs/{id}       (auth: jobs:read)
POST /api/jobs/{id}/cancel (auth: jobs:cancel)
POST /api/jobs/{id}/requeue (auth: jobs:create) — DLQ requeue, DEAD jobs only
POST /api/jobs/{id}/replay (auth: jobs:create) — clone a job (fresh id, replayed_from audit)
POST /api/keys           (auth: any credential; JWT-only in practice — keys cannot mint keys)
GET  /api/keys           (auth: any credential) — the caller's active API keys (migration 000007)
DELETE /api/keys/{id}    (auth: any credential; owner or users:delete admin)
POST /api/crons           (auth: jobs:create) — cron schedule (5-field expr, migration 000004)
GET  /api/crons           (auth: jobs:read)
DELETE /api/crons/{id}    (auth: jobs:cancel)
GET  /api/workers         (auth: jobs:read) — live worker registry from Redis
GET  /api/health/services (public) — aggregated service health for the console
GET  /ws                  (websocket upgrade, auth via ?token=)
GET  /health /ready /metrics
```

## Worker registry (Redis)

Workers register themselves for discovery and the console UI:

- Key `worker:<id>` (hash): `id`, `started_at`, `last_heartbeat`,
  `jobs_processed`, `in_flight`. TTL 15 s, refreshed every 5 s.
- The gateway reads `SCAN worker:*` to answer `GET /api/workers`.

## Job model (jobs service ↔ worker ↔ broker)

Job JSON fields: `id, type, payload, status, priority, attempts,
max_attempts, created_at, started_at, finished_at, error, worker_id,
execution_generation`. Scheduling adds (migration 000004):
`scheduled_at` (delayed jobs) and `replayed_from` (replay audit).

Status flow: `QUEUED → PROCESSING → SUCCESS | FAILED → RETRYING →
SUCCESS | DEAD`, plus `CANCELLED` and `SCHEDULED` (delayed jobs; the
dispatcher flips `SCHEDULED → QUEUED` when `scheduled_at` comes due).

Leases (ADR 009, migration 000003): a claimed job carries `heartbeat_at` and
`lease_until`; the worker renews while executing; the jobs-service sweeper
requeues expired leases with `execution_generation + 1`, and every execution
write is fenced by the generation. Broker messages carry
`execution_generation`; `0` means "unknown" (pre-lease messages) and is
accepted as the row's current generation.

Broker topics: `jobs.p1`..`jobs.p9` (execution, routed by priority — p1 is
most urgent), `jobs` (legacy execution topic; still receives a mirror copy
while `JOBS_LEGACY_TOPIC_FANOUT` is on), `jobs.dlq` (dead letters),
`jobs.retry` (delayed retries).

## Live events (Redis pub/sub → websocket service)

Channel `raven:events:jobs` carries JSON:
`{"type":"job_status","job_id":"...","status":"...","worker_id":"...","at":"..."}`.
The websocket service fans these out to subscribed clients.
