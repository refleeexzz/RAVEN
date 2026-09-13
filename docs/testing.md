# Testing RAVEN

How to run every suite, what each one covers, and the gotchas. The Makefile
wraps all of this (`make test`, `make test-race`, `make coverage`,
`make test-integration`, `make benchmark`); the raw commands below are what
those targets run.

Requires Go 1.27+. Integration tests additionally need a Docker daemon.

## Unit tests

```bash
go test ./...
```

Plain unit tests, no Docker, no network. They live next to the code they
test (`*_test.go` in the same package) and cover the parts where logic is
dense:

- **broker** (`internal/broker/...`) — wire protocol round-trips, record and
  segment storage, crash recovery, the range assignor, consumer-group
  coordination, backpressure (`BROKER_BUSY`), goroutine-leak checks
  (`go.uber.org/goleak`), and an end-to-end in-process broker test.
- **gateway** — the circuit breaker state machine, the token-bucket limiter,
  the auth cache, error mapping, and the route table itself (so a route
  added without a permission fails the test).
- **auth / users / jobs / worker / websocket** — password and email
  validation, permission matching, job state-machine guards, the retry
  backoff schedule (the exact 100 ms → 250 ms → 500 ms → 1 s → 2 s → 4 s →
  5 s sequence is pinned by a test), handler behavior, and the WebSocket
  protocol/hub logic.

A note on style: the hot pure functions are written to be testable without
I/O (clock injection points, `now func() time.Time` fields), which is why
most of these suites run in milliseconds.

## Race detector

```bash
go test -race ./...
```

**Windows gotcha:** `-race` needs cgo, and cgo needs a C compiler. On a
fresh Windows machine this fails with `cgo: C compiler "gcc" not found`.
Install gcc via MSYS2/MinGW (`pacman -S mingw-w64-ucrt-x86_64-gcc`), make
sure it is on `PATH`, and the flag works. CI runs this on Ubuntu where gcc
is already there, so a green CI does not prove your Windows box can run it.

The race detector matters most for the broker (per-partition writer
goroutines + concurrent fetches), the websocket hub (conn pumps hitting
shared maps), and the worker (semaphore + timers + shutdown flushing). It
has caught real bugs in all three.

## Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

CI uploads `coverage.out` as a build artifact on every run (14-day
retention).

## Integration tests

```bash
go test -tags=integration ./tests/integration/... -timeout 10m
```

These spin up **real Postgres and Redis in testcontainers**, apply the real
migration files, and run the actual services in-process. Every test skips
cleanly when Docker is unavailable. There are two suites:

- **`auth_users_test.go`** — the auth + users services end to end against
  real Postgres (migration 000001 applied) and real Redis: registration,
  login, refresh rotation and reuse detection, token validation, RBAC, and
  the users CRUD over gRPC.
- **`jobs_worker_test.go`** — the whole job system: an in-process broker,
  real Postgres + Redis, the jobs gRPC service, and a real worker wired
  through `services/worker.Run`. It creates jobs over gRPC and watches them
  go `QUEUED → PROCESSING → SUCCESS` through the actual broker, including
  failure/retry/DLQ paths.

The same suites run in CI on every push and PR to `main`.

## Benchmarks

```bash
go test -run=^$ -bench=. -benchmem ./internal/broker/
```

Two benchmarks exist, both driving the full stack (TCP frame → dispatch →
partition writer → append) with the production fsync policy:

- `BenchmarkProduce` — synchronous single-record produce, 1 KiB values.
- `BenchmarkProduceBatched` — same thing through the micro-batching producer
  (flush every 5 ms or 64 messages), parallel producers.

On my machine (Ryzen 5 5600X, Windows, Go 1.27) the first lands around
**10.2k msg/s (~10 MB/s)** and the second around **2.8× that aggregate**.
Your numbers will differ; they are a regression detector and a demo of what
batching buys, not a spec. Numbers on SSDs move with the fsync policy —
set `BROKER_FSYNC_MS` very low and watch throughput collapse, which is the
whole point of the tunable.

## Load testing (not built yet)

There is no load-test harness in the repo today. If you want to hammer the
edge, the obvious starting points:

- **k6** or **vegeta** against `POST /api/auth/login` and `POST /api/jobs`
  — the login path exercises bcrypt (cost 12, so it is CPU-heavy on
  purpose), and job creation exercises gateway → gRPC → Postgres → broker.
- Watch `raven_gateway_rate_limited_total` climb once you pass 100 rpm per
  gateway process — that is the limiter doing its job, not a bug.
- For the broker specifically, the benchmarks above are more honest than a
  synthetic HTTP load test.

Marking this clearly: **roadmap, not done.**

## Chaos on the local stack

No chaos framework either — but `docker compose` plus the failure modes in
[failure-scenarios.md](failure-scenarios.md) gets you surprisingly far:

```bash
docker kill raven-worker-1        # hard-kill one worker (stranding window)
docker compose stop redis         # degraded mode across the platform
docker compose stop postgres      # readiness 503s, gateway breakers open
docker compose stop broker        # job creation 503s, workers reconnect-loop
docker compose stop worker        # graceful drain, nothing strands
```

Each of those has a "what you should observe" section in the failure doc.
Run them while the console UI is open and watch the dashboard react — that
is the fun version of this test plan.
