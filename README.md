# RAVEN — Distributed Systems Platform

RAVEN is a distributed systems learning platform built in Go. It is shaped
like a production system: an API gateway, a few gRPC services behind it, a
message broker, a worker pool, Postgres, Redis, and a real observability
stack. But it is not pretending to be production software. I built it to
learn how these pieces fit together by actually writing them — including a
small Kafka-style log broker from scratch, which was the fun part.

The thing it does, end to end: you register, log in, and submit jobs
(`send_email`, `resize_image`, `webhook`). The jobs service stores the job in
Postgres and publishes it to the broker. A pool of workers consumes the topic
through a consumer group, executes the job with bounded concurrency, retries
failures with exponential backoff, and sends hopeless jobs to a dead-letter
topic. Every status change is published to a Redis channel, and the WebSocket
service pushes it to your browser in real time. There is a console UI
(`web/console`) to watch all of this happen.

Everything in this README describes what exists and runs today. Where the
code makes a trade-off, I say so. Where something is missing, it is in
[Known limitations](#known-limitations) or [Roadmap](#roadmap) — not in the
feature list.

## Architecture

```text
                        ┌────────────────────────────────────────────┐
                        │                clients                     │
                        │     curl · web/console · your scripts      │
                        └───────────────────┬────────────────────────┘
                                            │ HTTP / WebSocket
                                            ▼
                               ┌─────────────────────────┐
                               │   gateway  :8080        │  only public port
                               │   REST edge             │  authN/authZ, rate
                               │   timeouts · retries ·  │  limit, circuit
                               │   circuit breakers      │  breakers
                               └───┬───────┬───────┬─────┘
                        gRPC 9081  │       │ gRPC 9082/9083
              ┌────────────────────┘       └───────┬───────────┐
              ▼                                    ▼           ▼
      ┌───────────────┐   gRPC 9082      ┌──────────────┐  ┌──────────────┐
      │  auth         │◄────users CRUD──►│  users       │  │  jobs        │
      │  :9081        │                  │  :9082       │  │  :9083       │
      │  tokens, RBAC │                  │  profiles    │  │  lifecycle   │
      └──────┬────────┘                  └──────┬───────┘  └───┬────┬─────┘
             │                                  │              │    │ publish
             │                Postgres 5432     │              │    ▼
             │             ┌────────────────┐   │              │ ┌───────────┐
             ├────────────►│  postgres      │◄──┴──────────────┤ │ broker    │
             │             │  users, jobs,  │                  │ │ TCP :9100 │
             │             │  sessions, ... │                  │ │ log-based │
             │             └────────────────┘                  │ └─────┬─────┘
             │                                                 │       │ consume
             │               Redis 6379                        │       │ (group "workers")
             │             ┌─────────────────┐                 │       ▼
             ├────────────►│  redis          │◄────────────────┤ ┌───────────┐
             │ denylist,   │  cache, pub/sub │   job events    │ │ worker ×N │
             │ perms cache │  presence,      │                 │ │ pool (sem │
             └─────────────┤  worker registry │◄───────────────┤ │ of 8/job  │
                           └───────┬─────────┘   registry beats│ │ timeout)  │
                                   │ fanout                    │ └─────┬─────┘
                                   ▼                           │       │ events
                          ┌─────────────────┐                  │       ▼
                          │ websocket :8084 │◄─────────────────┴── redis pub/sub
                          │ rooms, presence │                        (raven:events:jobs)
                          └─────────────────┘
                                   ▲
            browser connects via gateway /ws proxy (or :8084 directly in dev)

   observability: prometheus :9090 scrapes /metrics on every service,
   grafana :3000 (dashboard preloaded), jaeger :16686 (OTLP traces :4317)
```

Ops HTTP ports (not public): auth 8081, users 8082, jobs 8083, websocket
8084 (also its public port), worker 8085, broker 9101. Every one serves
`/health`, `/ready`, `/metrics`. The full port and env-var table lives in
[docs/contracts/ports-and-env.md](docs/contracts/ports-and-env.md).

## What's in the box

### Edge and API

- **API gateway** (`services/gateway`) — the single public entrypoint on
  `:8080`. REST in, gRPC out. Per-route permission checks, a token
  bucket rate limiter (100 req/min, burst 20, per user or per IP), a
  circuit breaker per upstream (5 consecutive transport failures → open
  for 30 s → one half-open probe), bounded retries for idempotent calls
  (2 retries, 100 ms then 250 ms, only on `Unavailable`), a 10 s request
  timeout, and a 5 s timeout per upstream call.
- **REST API** for auth (register/login/refresh/logout), users CRUD, and
  jobs (create/list/get/cancel/requeue). Full reference:
  [docs/api.md](docs/api.md).
- **WebSocket proxy** at `/ws` forwarding to the websocket service, with
  the upgrade-safe middleware treatment it needs.

### Identity

- **auth service** (`services/auth`, gRPC `:9081`) — registration, login,
  refresh-token rotation with reuse detection, token validation and
  revocation. Access tokens are HS256 JWTs, 15 min. Refresh tokens are
  opaque 256-bit strings, 7 days, stored as SHA-256 hashes only. Reusing a
  rotated refresh token revokes every session of that user. Passwords are
  bcrypt, cost 12.
- **RBAC** — roles `ADMIN`, `USER`, `WORKER`, `SERVICE`; permissions like
  `users:read`, `jobs:create`, plus the single wildcard `admin:*`. Seeded in
  migration 000001.

### Jobs and workers

- **jobs service** (`services/jobs`, gRPC `:9083`) — owns the job lifecycle:
  `QUEUED → PROCESSING → SUCCESS | FAILED → RETRYING → SUCCESS | DEAD`, plus
  `CANCELLED`. Idempotent create via the `Idempotency-Key` header (Redis
  fast path + Postgres unique index backstop). Manual DLQ requeue for DEAD
  jobs.
- **worker pool** (`services/worker`) — consumes topic `jobs` in consumer
  group `workers`. Bounded concurrency (semaphore of 8), 30 s per-job
  timeout, retry backoff 100 ms → 250 ms → 500 ms → 1 s → 2 s → 4 s → 5 s
  cap, dead-lettering after `max_attempts` (default 4). Three handler types:
  `send_email` (simulated SMTP, 200–800 ms), `resize_image` (CPU-shaped,
  ~300 ms of SHA-256 churn), `webhook` (real HTTP POST, non-2xx is
  retryable). Workers heartbeat into a Redis registry (15 s TTL, refreshed
  every 5 s) so `GET /api/workers` shows who is alive.
- **Duplicate protection** — the broker is at-least-once, so workers fence
  every delivery with an atomic `UPDATE ... WHERE status IN ('QUEUED',
  'RETRYING')`. A duplicate or cancelled job matches nothing and is skipped.

### The broker (built from scratch)

- Custom TCP binary protocol on `:9100`: framed requests with correlation
  ids, pipelining, stable error codes.
- Append-only segmented logs (64 MiB segments), CRC-32C per record, sparse
  index (one entry per 4 KiB), crash recovery that truncates the torn tail.
- fsync every 100 ms or 256 records, whichever comes first.
- Consumer groups with server-side coordination: range assignor, 10 s
  session timeout, generation-fenced offset commits, at-least-once delivery.
- Backpressure: a bounded per-partition produce queue (1024) that answers
  `BROKER_BUSY` instead of eating RAM.
- Full story: [docs/broker-internals.md](docs/broker-internals.md). It is
  single-node and has no retention — by design, for now.

### Realtime

- **websocket service** (`services/websocket`, `:8084`) — rooms, direct
  messages, presence (Redis-backed, 90 s TTL), and multi-replica fanout over
  Redis pub/sub. Per-connection rate limit (20 msg/s, burst 40). Job status
  events from workers land in the `jobs` room.
- **console** (`web/console`) — a React operations dashboard: overview,
  jobs, workers, broker, observability. Works with zero backend in demo
  mode (a built-in simulator), so you can hack on the UI offline.

### Observability and ops

- Prometheus metrics on every service (`/metrics`), a preloaded Grafana
  dashboard ("RAVEN Platform Status"), and end-to-end traces in Jaeger via
  OTLP when `OTEL_ENABLED=true` (the compose stack sets it).
- Structured JSON logs (`slog`) with request ids everywhere.
- Liveness and readiness probes on every service; readiness actually checks
  dependencies (Postgres, Redis, broker, gRPC upstreams).
- One-command local stack with Docker Compose, and a Kubernetes manifest
  set for Docker Desktop with HPAs, probes and rolling updates.

## Quickstart

You need Docker (with Compose) and nothing else to run the stack. To hack on
the code: Go 1.27+ and, for the console, Node.

```bash
cp .env.example .env     # optional — every var has a dev default
docker compose up -d
```

First run builds 8 images, so it takes a few minutes. Then:

```bash
curl http://localhost:8080/health
# {"status":"ok"}
```

Register and log in (password needs 8+ characters):

```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","password":"correct horse battery","display_name":"Me"}'

curl -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","password":"correct horse battery"}'
```

Login returns a token pair:

```json
{
  "access_token": "eyJhbGciOi...",
  "refresh_token": "dG9rZW4...",
  "access_expires_at": 1760000900,
  "refresh_expires_at": 1760604800
}
```

Create a job and watch it run:

```bash
TOKEN=<paste access_token here>

curl -X POST http://localhost:8080/api/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-first-job-v1' \
  -d '{"type":"send_email","payload":{"to":"me@example.com","subject":"hi","body":"from raven"}}'

curl http://localhost:8080/api/jobs -H "Authorization: Bearer $TOKEN"
```

Within a second or two the job moves `QUEUED → PROCESSING → SUCCESS`. To
watch live instead of polling, open the console (below) or connect a
WebSocket and join the `jobs` room:

```bash
# any ws client works; the token goes in the query string
websocat "ws://localhost:8080/ws?token=$TOKEN"
> {"op":"join","room":"jobs"}
< {"op":"joined","room":"jobs"}
< {"op":"event","room":"jobs","data":{"type":"job_status","job_id":"job_...","status":"SUCCESS",...},"at":"..."}
```

`GET /api/workers` shows the live worker pool. `docker compose down` stops
everything; add `-v` only if you want to delete the data volumes too.

## The console

```bash
cd web/console
npm install
npm run dev    # http://localhost:7100
```

The console probes the gateway at boot. If the stack is down, it switches to
**demo mode** automatically: a simulator runs fake jobs through their whole
lifecycle, one worker is flaky on purpose, and every button still works. It
is the fastest way to see what the UI does. Details:
[web/console/README.md](web/console/README.md).

## Project structure

```text
.
├── cmd/                  one main.go per binary (gateway, auth, users, jobs,
│                         websocket, broker, worker, migrate)
├── services/             service wiring and business logic, one dir per service
│   ├── gateway/          REST edge: routing, authN/Z, rate limit, breaker, proxy
│   ├── auth/             tokens, sessions, RBAC (gRPC :9081)
│   ├── users/            user CRUD and profiles (gRPC :9082)
│   ├── jobs/             job lifecycle, idempotency, events (gRPC :9083)
│   ├── websocket/        rooms, presence, Redis fanout (:8084)
│   ├── worker/           consumer, handlers, retries, registry (:8085 ops)
│   └── broker/           broker binary wiring (:9100 TCP, :9101 ops)
├── internal/
│   ├── broker/           the broker itself: protocol, storage, groups, client
│   ├── auth/             shared JWT/RBAC/password primitives
│   ├── config/           env parsing
│   ├── database/         pgx pool + tx helpers
│   ├── gen/              generated protobuf/gRPC code
│   ├── health/           /health and /ready
│   ├── httpserver/       shared HTTP server with graceful shutdown
│   └── middleware/       RequestID, Logging, Recovery, Timeout, Chain
├── pkg/                  logger, errors, metrics, tracing
├── proto/                the gRPC contracts (source of generated code)
├── migrations/           numbered SQL migrations (000001 auth/users, 000002 jobs)
├── deployments/
│   ├── docker/           one multi-stage Dockerfile per service
│   ├── kubernetes/       manifests for Docker Desktop k8s (HPA, probes, secrets)
│   ├── prometheus/       scrape config
│   └── grafana/          provisioning + the platform dashboard
├── tests/integration/    testcontainers-based end-to-end suites
├── web/console/          React operations dashboard (demo mode built in)
├── docs/                 architecture, API, failures, testing, ADRs, contracts
├── docker-compose.yml    the full local stack
└── Makefile              build, test, lint, proto, docker, k8s targets
```

## Testing

```bash
go test ./...                                    # unit tests
go test -race ./...                              # same, with the race detector
go test -coverprofile=coverage.out ./...         # coverage
go test -tags=integration ./tests/integration/... # needs Docker (testcontainers)
go test -run=^$ -bench=. -benchmem ./internal/broker/   # broker benchmarks
```

One Windows gotcha: `-race` needs a C compiler (gcc via MinGW/MSYS2). Without
it you get a cryptic `cgo: C compiler "gcc" not found` error. The
`Makefile` has `test`, `test-race`, `coverage`, `test-integration` and
`benchmark` targets. CI runs all of this on every push. Full guide:
[docs/testing.md](docs/testing.md).

## Benchmarks

Measured on my machine (Ryzen 5 5600X, Windows, Go 1.27), full TCP path,
1 KiB values, production fsync policy (100 ms / 256 records):

| Benchmark | What it measures | Result |
|-----------|------------------|--------|
| `BenchmarkProduce` | sync single-record produce | ~10.2k msg/s (~10 MB/s) |
| `BenchmarkProduceBatched` | micro-batching (5 ms / 64 msgs), parallel | ~2.8× aggregate throughput |

These are my numbers on my hardware, not a guarantee. Run
`go test -run=^$ -bench=. -benchmem ./internal/broker/` on yours. The
batched benchmark exists to show how much frame and request overhead the
micro-batching producer amortizes — same public API, one request per topic
per flush window.

## Observability

| What | Where |
|------|-------|
| Metrics | `/metrics` on every service's ops port, scraped by Prometheus (`:9090`) |
| Dashboard | Grafana `:3000` (admin/admin) → "RAVEN" folder → "RAVEN Platform Status" |
| Traces | Jaeger `:16686`; services export OTLP gRPC spans to `jaeger:4317` |
| Logs | JSON via `slog`, one line per request/RPC, request id included |

Notable service metrics: `raven_gateway_circuit_breaker_state` (0/1/2 per
upstream), `raven_gateway_upstream_duration_seconds`,
`raven_auth_tokens_validated_total`, `raven_jobs_processing`,
`raven_broker_messages_pending` (consumer lag, computed at scrape time),
`raven_websocket_connections`, `raven_worker_jobs_processed_total`.

## Known limitations

Being honest about what this is:

1. **The broker is a single node.** No replication. The disk dies, the data
   is gone. This is the biggest gap between RAVEN and a real system.
2. **No broker retention.** Logs grow forever; nothing compacts or deletes
   old segments.
3. **Rate limiting is per-process.** With 3 gateway replicas each one has
   its own buckets, so the effective limit is `replicas × 100 rpm`. Moving
   this to Redis is on the roadmap.
4. **The token revocation denylist fails open.** If Redis is down, auth
   accepts any cryptographically valid token. Exposure is bounded by the
   15-minute access token TTL, but it is a real trade-off.
5. **Jobs can get stranded in `PROCESSING`/`RETRYING` when a worker is
   hard-killed** (`kill -9`). Offsets commit at dispatch, so up to
   `concurrency` (8) in-flight jobs can be left with no message in flight.
   Graceful shutdowns (SIGTERM) drain cleanly. A sweeper for stranded jobs
   is a documented TODO in `services/worker/worker.go`.
6. **Broker group membership is in memory.** Committed offsets survive a
   restart; members just re-join. Cheap here, not how Kafka does it.
7. **No auth or encryption on the broker port** (`:9100`). It never leaves
   the internal network — the gateway does not expose it.
8. **Delivery is at-least-once end to end.** Consumers see duplicates
   sometimes. The jobs fence absorbs them; anything new you build on the
   broker has to do the same.

## Roadmap

What a v2 would add, in rough order of value:

- **Broker replication** — leader/follower partitions. Turns the broker from
  a teaching tool into something you could trust.
- **Stranded-job sweeper** — a periodic scan for jobs stuck in
  `PROCESSING`/`RETRYING` past a deadline, republishing them. The fence
  already makes this safe.
- **Redis-backed rate limiting** — one shared budget across gateway
  replicas.
- **Broker log retention** — delete or compact old segments.
- **Load-testing harness** — k6 or vegeta scenarios checked into the repo
  (ideas in [docs/testing.md](docs/testing.md)).

## Docs

| Doc | What it covers |
|-----|----------------|
| [docs/architecture.md](docs/architecture.md) | request lifecycle, who talks to whom and why, data ownership, scaling |
| [docs/api.md](docs/api.md) | every REST route, the error envelope, the WebSocket protocol |
| [docs/broker-internals.md](docs/broker-internals.md) | broker protocol, storage, consumer groups |
| [docs/failure-scenarios.md](docs/failure-scenarios.md) | what breaks, what you observe, how to reproduce it |
| [docs/testing.md](docs/testing.md) | unit, race, integration, benchmarks, chaos ideas |
| [docs/contracts/ports-and-env.md](docs/contracts/ports-and-env.md) | the platform contract: ports, env vars, routes, topics |
| [deployments/README.md](deployments/README.md) | running with Compose or Kubernetes |
| [web/console/README.md](web/console/README.md) | the console and demo mode |

### Architecture Decision Records

The "why" behind the big choices, each with honest downsides:

1. [ADR 001 — Go](docs/adr/001-use-go.md)
2. [ADR 002 — gRPC between services](docs/adr/002-use-grpc.md)
3. [ADR 003 — PostgreSQL](docs/adr/003-use-postgresql.md)
4. [ADR 004 — Redis for ephemeral coordination](docs/adr/004-use-redis.md)
5. [ADR 005 — custom message broker](docs/adr/005-custom-message-broker.md)
6. [ADR 006 — Kubernetes](docs/adr/006-kubernetes.md)
7. [ADR 007 — event-driven jobs](docs/adr/007-event-driven-architecture.md)
8. [ADR 008 — shared users table](docs/adr/008-shared-users-table.md)
