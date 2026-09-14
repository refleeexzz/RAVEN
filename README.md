# RAVEN — Distributed Systems Platform

RAVEN is a distributed systems learning platform built in Go. It is shaped
like a production system: an API gateway, gRPC services behind it, a message
broker written from scratch, a worker pool, Postgres, Redis, Kubernetes
manifests, and a real observability stack. But it is not pretending to be
production software. I built it to learn how these pieces fit together by
actually writing them — and then by breaking them on purpose.

The thing it does, end to end: you register, log in, and submit jobs
(`send_email`, `resize_image`, `webhook`). The jobs service stores the job in
Postgres and publishes it to the broker. A pool of workers consumes the topic
through a consumer group, executes with bounded concurrency, retries failures
with exponential backoff, and sends hopeless jobs to a dead-letter topic.
Every status change is pushed to your browser over WebSocket in real time,
and there is a console UI to watch it all. If a worker dies mid-job — even
SIGKILL on every worker at once — leases expire, a sweeper requeues the work,
and generation fencing stops the dead workers' late writes. That last part is
chaos-tested, not hoped for.

Repo: [github.com/refleeexzz/RAVEN](https://github.com/refleeexzz/RAVEN) —
Go module `github.com/refleeexzz/RAVEN`.

Everything in this README describes what exists and runs today. Where the
code makes a trade-off, I say so. Where something is missing, it is in
[Known limitations](#known-limitations) or [Roadmap](#roadmap) — not in the
feature list.

## Architecture

```text
                        ┌────────────────────────────────────────────┐
                        │                clients                     │
                        │     curl · console (:7100) · scripts       │
                        └───────────────────┬────────────────────────┘
                                            │ HTTP / WebSocket
                                            ▼
                               ┌─────────────────────────┐
                               │   gateway  :8080        │  CORS, authN/authZ,
                               │   REST edge             │  rate limit, circuit
                               │   timeouts · retries ·  │  breakers, /ws proxy
                               │   circuit breakers      │
                               └───┬───────┬───────┬─────┘
                        gRPC 9081  │       │ gRPC 9082/9083
              ┌────────────────────┘       └───────┬───────────┐
              ▼                                    ▼           ▼
      ┌───────────────┐   gRPC 9082      ┌──────────────┐  ┌──────────────┐
      │  auth         │◄────users CRUD──►│  users       │  │  jobs        │
      │  :9081        │                  │  :9082       │  │  :9083       │
      │  tokens, RBAC │                  │  profiles    │  │  lifecycle + │
      └──────┬────────┘                  └──────┬───────┘  │  sweeper     │
             │                                  │          └───┬────┬─────┘
             │                Postgres 5432     │              │    │ publish
             │             ┌────────────────┐   │              │    ▼
             ├────────────►│  postgres      │◄──┴──────────────┤ ┌───────────┐
             │             │  users, jobs,  │                  │ │ broker    │
             │             │  sessions,     │                  │ │ TCP :9100 │
             │             │  leases, ...   │                  │ │ 12 parts/ │
             │             └────────────────┘                  │ │ topic     │
             │                                                 │ └─────┬─────┘
             │               Redis 6379                        │       │ consume
             │             ┌─────────────────┐                 │       │ (group "workers")
             ├────────────►│  redis          │◄────────────────┤ ┌───────────┐
             │ denylist,   │  cache, pub/sub │   job events    │ │ worker ×N │
             │ perms cache │  presence,      │                 │ │ leases +  │
             └─────────────┤  worker registry │◄───────────────┤ │ heartbeats│
                           └───────┬─────────┘   registry beats│ └─────┬─────┘
                                   │ fanout                    │       │ events
                                   ▼                           │       ▼
                          ┌─────────────────┐                  └── redis pub/sub
                          │ websocket :8084 │                       (raven:events:jobs)
                          │ rooms, presence │
                          └─────────────────┘
                                   ▲
            browser connects via gateway /ws proxy (anonymous read-only
            in the dev cluster, JWT in production-shaped setups)

   observability: prometheus :9090 scrapes /metrics on every service,
   grafana :3000 (dashboard preloaded), jaeger :16686 — one trace covers
   gateway → gRPC → broker publish → worker execute → Postgres commit
```

Ops HTTP ports (not public): auth 8081, users 8082, jobs 8083, websocket
8084 (also its public port), worker 8085, broker 9101. Every one serves
`/health`, `/ready`, `/metrics`. The full port and env-var table lives in
[docs/contracts/ports-and-env.md](docs/contracts/ports-and-env.md).

## What's in the box

### Edge and API

- **API gateway** (`services/gateway`) — the single public entrypoint on
  `:8080`. REST in, gRPC out. CORS enabled for the console. Per-route
  permission checks, a token bucket rate limiter (100 req/min, burst 20, per
  user or per IP), a circuit breaker per upstream (5 consecutive transport
  failures → open for 30 s → one half-open probe), bounded retries for
  idempotent calls (2 retries, 100 ms then 250 ms, only on `Unavailable`),
  a 10 s request timeout, and a 5 s timeout per upstream call.
- **REST API** for auth (register/login/refresh/logout), users CRUD, jobs
  (create/list/get/cancel/DLQ requeue), `GET /api/workers` (live registry
  from Redis), and `GET /api/health/services` (aggregated health that powers
  the console's service grid). Full reference: [docs/api.md](docs/api.md).
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
  `users:read`, `jobs:create`, plus the single wildcard `admin:*`.

### Jobs, workers, and stranded-job recovery

- **jobs service** (`services/jobs`, gRPC `:9083`) — owns the job lifecycle:
  `QUEUED → PROCESSING → SUCCESS | FAILED → RETRYING → SUCCESS | DEAD`, plus
  `CANCELLED`. Idempotent create via the `Idempotency-Key` header (Redis
  fast path + Postgres unique index backstop). Manual DLQ requeue for DEAD
  jobs. It also runs the **recovery sweeper**: one jobs replica at a time
  (elected with a Postgres advisory lock) scans for jobs whose lease expired
  and requeues them.
- **worker pool** (`services/worker`) — consumes topic `jobs` in consumer
  group `workers`. Bounded concurrency (semaphore of 8), 30 s per-job
  timeout, retry backoff 100 ms → 250 ms → 500 ms → 1 s → 2 s → 4 s → 5 s
  cap, dead-lettering after `max_attempts` (default 4). Handlers:
  `send_email` (simulated), `resize_image` (CPU-shaped), `webhook` (real
  HTTP POST). Workers heartbeat into a Redis registry (15 s TTL) so
  `GET /api/workers` shows who is alive.
- **Job leases and fencing** (migration 000003) — a running job carries
  `lease_until`, `heartbeat_at` and an `execution_generation`. The worker
  renews the lease while it executes. If the worker dies, the lease expires,
  the sweeper moves the job to `RETRYING` and republishes it, and the next
  worker takes over with a bumped generation — any late write from the old
  (zombie) worker is rejected by generation fencing. Proven live: SIGKILL on
  **all** workers mid-flight → 12/12 jobs recovered to `SUCCESS`. The
  details are in [docs/adr/009-job-leases.md](docs/adr/009-job-leases.md).

### The broker (built from scratch)

- Custom TCP binary protocol on `:9100`: framed requests with correlation
  ids, pipelining, stable error codes.
- Append-only segmented logs (64 MiB segments), CRC-32C per record, sparse
  index (one entry per 4 KiB), crash recovery that truncates the torn tail
  and rebuilds corrupt or missing indexes.
- fsync every 100 ms or 256 records, whichever comes first (tunable —
  benchmarks below show exactly what that knob costs).
- Consumer groups with server-side coordination: range assignor, 10 s
  session timeout, generation-fenced offset commits, at-least-once delivery.
- Topics default to **12 partitions** — 3 was the measured bottleneck for
  consumer parallelism, so the default moved up.
- Hardened after the chaos campaign: connection cap
  (`BROKER_MAX_CONNECTIONS`, default 1024), idle and write deadlines
  (`BROKER_IDLE_TIMEOUT`, `BROKER_WRITE_TIMEOUT`), and backpressure via a
  bounded per-partition produce queue (`BROKER_BUSY` instead of eating RAM).
- War story: in k8s we hit a **rebalance storm** — group generations were
  moving without real membership changes, so consumers kept re-joining and
  throughput collapsed. The fix was to only bump the generation when
  membership actually changes. Details in
  [docs/broker-internals.md](docs/broker-internals.md), which has the full
  internals doc. The broker is still single-node with no retention — by
  design, for now.

### Realtime

- **websocket service** (`services/websocket`, `:8084`) — rooms, direct
  messages, presence (Redis-backed), and multi-replica fanout over Redis
  pub/sub. Per-connection rate limit (20 msg/s, burst 40). Job status events
  land in the `jobs` room. The dev cluster allows anonymous read-only
  connections (`WS_ALLOW_ANONYMOUS`) so the console works without a login.
- **console** (`web/console`) — a React operations dashboard: overview,
  jobs, workers, broker, observability, and a **Test Lab** page with
  self-serve scenarios: a quick end-to-end check, a mini load test, a
  fail-on-purpose DLQ demo, and a break-it-yourself chaos guide. It also
  runs a built-in simulator (demo mode) when no backend is reachable.

### Observability and ops

- Prometheus metrics on every service (`/metrics`), a preloaded Grafana
  dashboard, and **end-to-end traces in Jaeger**: one trace covers gateway →
  auth/jobs gRPC → broker publish (the traceparent travels in the record
  headers) → worker execute → Postgres commit.
- Structured JSON logs (`slog`) with request ids everywhere.
- Liveness and readiness probes on every service; readiness actually checks
  dependencies (Postgres, Redis, broker, gRPC upstreams).
- Docker Compose for the laptop and a Kubernetes manifest set with HPAs,
  probes, secrets and rolling updates. The HPAs are capped at
  `maxReplicas: 8` — learned the hard way: uncapped autoscaling once
  starved the single Docker Desktop node and Postgres got evicted. Eight is
  the honest ceiling for this cluster.

## Quickstart

The full platform runs on Docker Desktop's Kubernetes. You need Docker
Desktop (with Kubernetes enabled) and nothing else. To hack on the code: Go
1.27+ and, for the console, Node.

```bash
kubectl apply -f deployments/kubernetes/namespace.yaml   # namespace first
kubectl apply -f deployments/kubernetes/                 # the rest
# or simply: make k8s-up
```

Wait for the pods, then open the console — it runs in the cluster (nginx,
2 replicas, LoadBalancer), so there is no port-forward to babysit:

```text
http://localhost:7100     console
http://localhost:3000     Grafana (admin/admin)
http://localhost:16686    Jaeger
http://localhost:9090     Prometheus
```

Sign in with the demo account (dev cluster only, do not reuse this anywhere):

```text
e2e@raven.dev / supersecret123
```

Or create your own account through the register endpoint:

```bash
curl -X POST http://localhost:8080/api/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","password":"correct horse battery","display_name":"Me"}'

curl -X POST http://localhost:8080/api/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","password":"correct horse battery"}'
```

Login returns a token pair (`access_token` 15 min, `refresh_token` 7 days).
Create a job and watch it run:

```bash
TOKEN=<paste access_token here>

curl -X POST http://localhost:8080/api/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: my-first-job-v1' \
  -d '{"type":"send_email","payload":{"to":"me@example.com","subject":"hi","body":"from raven"}}'

curl http://localhost:8080/api/jobs -H "Authorization: Bearer $TOKEN"
curl http://localhost:8080/api/health/services   # platform health grid
```

Within a second or two the job moves `QUEUED → PROCESSING → SUCCESS`. The
console shows it live (it joins the `jobs` WebSocket room for you), and the
Test Lab page has one-click scenarios if you want to see failures, retries
and the DLQ on purpose. `GET /api/workers` shows the live worker pool.

Tear down with `make k8s-down`. (A Docker Compose stack also exists —
`docker compose up -d` — but the k8s flow above is the one with the console,
the LoadBalancers and the tested failure behavior.)

## Project structure

```text
.
├── cmd/                  one main.go per binary (gateway, auth, users, jobs,
│                         websocket, broker, worker, migrate)
├── services/             service wiring and business logic, one dir per service
│   ├── gateway/          REST edge: routing, authN/Z, rate limit, breaker, proxy
│   ├── auth/             tokens, sessions, RBAC (gRPC :9081)
│   ├── users/            user CRUD and profiles (gRPC :9082)
│   ├── jobs/             job lifecycle, idempotency, sweeper, events (gRPC :9083)
│   ├── websocket/        rooms, presence, Redis fanout (:8084)
│   ├── worker/           consumer, handlers, leases, retries, registry
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
├── migrations/           numbered SQL migrations (000001 auth/users,
│                         000002 jobs, 000003 job leases)
├── deployments/
│   ├── docker/           one multi-stage Dockerfile per service
│   ├── kubernetes/       manifests incl. the console (nginx, LoadBalancer),
│   │                     HPAs, probes, secrets
│   ├── prometheus/       scrape config
│   └── grafana/          provisioning + dashboards
├── tests/
│   ├── integration/      testcontainers-based end-to-end suites
│   └── chaos/            6 codified failure scenarios (kill workers, restart
│                         broker/redis/postgres, ...) with measured results
├── web/console/          React operations dashboard + Test Lab
├── docs/                 architecture, API, broker internals, failures, chaos
│                         results, benchmarks, testing, ADRs, contracts
├── docker-compose.yml    the Compose variant of the stack
└── Makefile              build, test, lint, proto, docker, k8s targets
```

## Testing

```bash
go test ./...                                         # unit tests
go test -race ./...                                   # same, with race detector — green
go test -coverprofile=coverage.out ./...              # coverage
go test -tags=integration ./tests/integration/...     # 10/10, testcontainers
go test -run=^$ -bench=. -benchmem ./internal/broker/ # broker benchmarks
```

Where things stand today: **106 broker tests** (protocol, storage, recovery,
consumer groups, hardening), the full suite is green with `-race`, the
integration suites pass 10/10 against real Postgres + Redis in
testcontainers, and the chaos suite passes 6/6.

One Windows gotcha: `-race` needs a C compiler (gcc via MinGW/MSYS2). Without
it you get a cryptic `cgo: C compiler "gcc" not found` error. The `Makefile`
has `test`, `test-race`, `coverage`, `test-integration` and `benchmark`
targets. Full guide: [docs/testing.md](docs/testing.md).

## Chaos testing

The failure scenarios are not just documentation — they are codified in
`tests/chaos/` (6 scenarios: kill workers mid-job, restart broker, restart
Redis, restart Postgres, kill consumers, kill multiple workers). Measured
results live in [docs/chaos-results.md](docs/chaos-results.md). Headline:
across all 6 scenarios, **zero data loss, zero duplicate executions**, and
recovery in 10–17 s per scenario. The 12/12 stranded-job recovery mentioned
above is one of these runs.

## Benchmarks

Measured on my machine (Ryzen 5 5600X, Windows, 2026-09-14) — full results
and methodology in [docs/benchmarks.md](docs/benchmarks.md):

| Scenario | Throughput |
|----------|-----------|
| Peak produce (100 producers × 128 B) | **65,357 msg/s** |
| Produce, 100 producers × 1 KB | ~60,000 msg/s |
| Single-producer sync (1 KB) | ~8,500 msg/s |
| Consume (fetch) | up to ~380,000 msg/s |

What the fsync knob costs (same produce path):

| fsync policy | Throughput |
|--------------|-----------|
| `always` | ~1,700 msg/s |
| periodic (default: 100 ms / 256 records) | ~8,600 msg/s |
| disabled | ~9,000 msg/s |

The original spec goal for the broker was 50k msg/s. Beaten — 65,357 at the
peak. These are my numbers on my hardware, not a guarantee; the suite is
reproducible, so run it on yours.

## Observability

| What | Where |
|------|-------|
| Metrics | `/metrics` on every service's ops port, scraped by Prometheus (`:9090`) |
| Dashboards | Grafana `:3000` (admin/admin), preloaded, LoadBalancer in k8s |
| Traces | Jaeger `:16686`; one trace covers gateway → gRPC → broker publish → worker → Postgres commit |
| Logs | JSON via `slog`, one line per request/RPC, request id included |

Notable metrics beyond the usual HTTP counters/histograms:

- Broker: `raven_broker_consumer_lag`, `raven_broker_partition_offset`,
  `raven_broker_consumer_offset`, `raven_broker_active_connections`,
  `raven_broker_append_latency`, `raven_broker_disk_usage`,
  `raven_broker_segment_count`.
- Jobs: `raven_jobs_queued`, `raven_jobs_processing`, `raven_jobs_retrying`,
  `raven_jobs_dead` gauges; sweeper counters.
- Worker: `raven_worker_retries_total`, `raven_worker_fenced_writes_total`
  (zombie writes rejected by generation fencing),
  `raven_worker_jobs_processed_total`, `raven_worker_in_flight`.
- Gateway: `raven_gateway_circuit_breaker_state` (0/1/2 per upstream),
  `raven_gateway_upstream_duration_seconds`, auth cache hits/misses,
  rate-limited total.

## Known limitations

Being honest about what this is:

1. **The broker is a single node.** No replication. The disk dies, the data
   is gone. This is the biggest gap between RAVEN and a real system, and
   fixing it is milestone 3 on the roadmap (the whole P1 track).
2. **No broker retention.** Logs grow forever; nothing compacts or deletes
   old segments yet (P3).
3. **Rate limiting is per-process.** With N gateway replicas each one has
   its own buckets, so the effective limit is `N × 100 rpm`.
4. **The token revocation denylist fails open.** If Redis is down, auth
   accepts any cryptographically valid token. Exposure is bounded by the
   15-minute access token TTL, but it is a real trade-off.
5. **Crash-loop blind spot.** A job that keeps crashing workers (think OOM
   on a poison payload) never burns attempts — the worker dies before it can
   record a failure, the sweeper requeues the job, and the loop repeats.
   The reasoning and the candidate fixes are in
   [docs/adr/009-job-leases.md](docs/adr/009-job-leases.md).
6. **No auth or encryption on the broker port** (`:9100`). It never leaves
   the cluster network (P3 covers TLS).
7. **Delivery is at-least-once end to end.** Consumers see duplicates
   sometimes. The job fence and generation fencing absorb them; anything new
   you build on the broker has to do the same.

## Roadmap

The tracked plan lives in
[RAVEN_IMPLEMENTATION_ROADMAP.md](RAVEN_IMPLEMENTATION_ROADMAP.md), with
checked items marking what shipped. Where it stands:

- **P0 — correctness and reliability: DONE.** Protocol hardening, storage
  recovery, consumer groups, stranded-job recovery (leases + fencing +
  sweeper), worker failure tests, chaos suite.
- **P2 — engineering proof: DONE.** Benchmark suite with measured results,
  consumer-lag metrics, advanced observability, distributed tracing.
- **P1 — distributed broker: NEXT.** Replication, acknowledgement modes,
  leader election, membership, partition reassignment, network-partition
  testing. This is what turns the broker from a teaching tool into something
  you could trust.
- **P3 — hardening: after that.** TLS, broker auth, retention, compaction.
- **P4 — next ideas**: Redis-backed rate limiting, KEDA-style autoscaling on
  consumer lag, cron-style job scheduling, JSON Schema per job type,
  priority-aware fetches, OpenAPI + generated console client, and more — the
  list is at the end of the roadmap doc.

## Docs

| Doc | What it covers |
|-----|----------------|
| [docs/architecture.md](docs/architecture.md) | request lifecycle, who talks to whom and why, data ownership, scaling |
| [docs/api.md](docs/api.md) | every REST route, the error envelope, the WebSocket protocol |
| [docs/broker-internals.md](docs/broker-internals.md) | broker protocol, storage, consumer groups, the rebalance-storm fix |
| [docs/failure-scenarios.md](docs/failure-scenarios.md) | what breaks, what you observe, how to reproduce it |
| [docs/chaos-results.md](docs/chaos-results.md) | measured results of the 6 chaos scenarios |
| [docs/benchmarks.md](docs/benchmarks.md) | full benchmark methodology and numbers |
| [docs/testing.md](docs/testing.md) | unit, race, integration, benchmarks, chaos |
| [docs/contracts/ports-and-env.md](docs/contracts/ports-and-env.md) | the platform contract: ports, env vars, routes, topics |
| [RAVEN_IMPLEMENTATION_ROADMAP.md](RAVEN_IMPLEMENTATION_ROADMAP.md) | the milestone plan (P0–P4) with shipped items checked |
| [deployments/README.md](deployments/README.md) | running with Compose or Kubernetes |
| [web/console/README.md](web/console/README.md) | the console, Test Lab and demo mode |

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
9. [ADR 009 — job leases and fencing](docs/adr/009-job-leases.md)
