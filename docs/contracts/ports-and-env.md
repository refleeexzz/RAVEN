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
| `JWT_SECRET`     | auth, gateway      | `dev-only-secret-change-me` (never in prod)       |
| `BROKER_ADDR`    | jobs, worker       | `localhost:9100`                                  |
| `BROKER_DATA_DIR`| broker             | `./data`                                          |
| `OTEL_ENABLED`   | all                | `false`                                           |
| `OTEL_ENDPOINT`  | all                | `localhost:4317`                                  |

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
GET  /ws                  (websocket upgrade, auth via ?token=)
GET  /health /ready /metrics
```

## Job model (jobs service ↔ worker ↔ broker)

Job JSON fields: `id, type, payload, status, priority, attempts,
max_attempts, created_at, started_at, finished_at, error, worker_id`.

Status flow: `QUEUED → PROCESSING → SUCCESS | FAILED → RETRYING →
SUCCESS | DEAD`, plus `CANCELLED`.

Broker topics: `jobs` (work), `jobs.dlq` (dead letters),
`jobs.retry` (delayed retries).

## Live events (Redis pub/sub → websocket service)

Channel `raven:events:jobs` carries JSON:
`{"type":"job_status","job_id":"...","status":"...","worker_id":"...","at":"..."}`.
The websocket service fans these out to subscribed clients.
