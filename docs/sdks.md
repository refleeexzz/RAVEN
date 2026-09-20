# RAVEN SDKs

Official client libraries for the RAVEN public REST API. Three languages,
one feature set, zero third-party dependencies in any of them.

| SDK | Path | Package | Runtime | Deps |
|-----|------|---------|---------|------|
| Go | [sdk/go](../sdk/go) | `github.com/refleeexzz/RAVEN/sdk/go` (package `raven`) | Go 1.23+ | stdlib only |
| Python | [sdk/python](../sdk/python) | `raven-sdk` (package `raven`) | Python 3.9+ | stdlib only (`urllib.request`) |
| JavaScript | [sdk/javascript](../sdk/javascript) | `raven-sdk` | Node.js 18+ (native `fetch`) | none |

Each SDK ships a README with a quick start and a runnable example under
`examples/`. Wire shapes follow [api.md](api.md) — what the API returns is
what you get.

## Feature parity

Same capabilities across all three. Names differ per language idiom
(`CreateJob` / `create_job` / `createJob`).

| Capability | Go | Python | JavaScript |
|------------|----|--------|------------|
| Register / Login / Refresh / Logout | ✓ | ✓ | ✓ |
| Bearer JWT auth | ✓ | ✓ | ✓ |
| `ApiKey rav_live_...` auth | ✓ | ✓ | ✓ |
| Automatic access-token refresh (proactive, before expiry) | ✓ | ✓ | ✓ |
| One silent refresh + retry on a surprise `401` | ✓ | ✓ | ✓ |
| Jobs: submit / list / get / cancel / requeue / replay | ✓ | ✓ | ✓ |
| Job deliveries (webhook history) | ✓ | ✓ | ✓ |
| Watch a job to a terminal state | `WatchJob` (blocking), `JobWatch` (channel) | `watch_job` (blocking), `iter_job` (generator) | `watchJob` (blocking), `iterateJob` (async generator) |
| Crons: create / list / delete | ✓ | ✓ | ✓ |
| API keys: create / list / revoke | ✓ | ✓ | ✓ |
| Workers: list live registry | ✓ | ✓ | ✓ |
| Aggregated service health | ✓ | ✓ | ✓ |
| Audit trail list (admin) | ✓ | ✓ | ✓ |
| Typed errors with `code` + `message` + `request_id` | `*raven.Error` | `RavenError` | `RavenError` (+ `TransportError`, `TimeoutError`) |
| Auto `Idempotency-Key` on every mutating call | ✓ | ✓ | ✓ |
| Per-call idempotency-key override | `WithIdempotencyKey` | `idempotency_key=` | `idempotencyKey` |
| Configurable base URL | `WithBaseURL` | `base_url=` | `baseUrl` |
| Configurable timeout (default 10 s) | `WithTimeout` | `timeout=` | `timeoutMs` |
| Context / cancellation support | `context.Context` on every method | `timeout` per request | `AbortSignal` on watch/iterate |

## Quick starts side by side

**Go**

```go
c := raven.New(raven.WithBaseURL("http://localhost:8080"))
if _, err := c.Login(ctx, "me@example.com", "correct horse battery"); err != nil { log.Fatal(err) }
payload, _ := raven.NewJobPayload(map[string]any{"url": "https://me.example/hook"})
job, _ := c.CreateJob(ctx, raven.CreateJobRequest{Type: "webhook", Payload: payload})
final, _ := c.WatchJob(ctx, job.ID, 2*time.Second)
```

**Python**

```python
from raven import Client
client = Client(base_url="http://localhost:8080")
client.login("me@example.com", "correct horse battery")
job = client.create_job(type="webhook", payload={"url": "https://me.example/hook"})
final = client.watch_job(job.id, interval=2.0, timeout=120)
```

**JavaScript**

```js
import { RavenClient } from 'raven-sdk';
const client = new RavenClient({ baseUrl: 'http://localhost:8080' });
await client.login('me@example.com', 'correct horse battery');
const job = await client.createJob({ type: 'webhook', payload: { url: 'https://me.example/hook' } });
const final = await client.watchJob(job.id, { intervalMs: 2000, timeoutMs: 120_000 });
```

## Error handling side by side

All three expose the same three fields from the gateway's error envelope
(`code`, `message`, `request_id`) plus the HTTP status.

**Go**

```go
if raven.IsCode(err, "job_not_found") { /* ... */ }
```

**Python**

```python
try:
    client.get_job("job_nope")
except RavenError as err:
    assert err.code == "job_not_found"
```

**JavaScript**

```js
try {
  await client.getJob('job_nope');
} catch (err) {
  if (RavenError.isCode(err, 'job_not_found')) { /* ... */ }
}
```

## Running the tests

Each suite runs against a mock gateway — no live stack needed.

```bash
cd sdk/go         && go test ./...
cd sdk/python     && python -m unittest discover
cd sdk/javascript && node --test
```

## Known limitations

- **No WebSocket streaming.** The realtime protocol (`/ws`, see
  [api.md](api.md#websocket-protocol)) is not wrapped by any SDK yet; job
  watching is done by polling the REST API (`watch*`, 2 s default
  interval). A streaming helper may land in a future version.
- **Users CRUD is not exposed** in the SDKs (it is an admin surface; the
  REST routes exist and can be called directly if needed).
- The Go module lives at `sdk/go` with its own `go.mod` so it never
  pollutes the main module's dependency graph.
