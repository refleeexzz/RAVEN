# raven-sdk — RAVEN Python SDK

The official Python client for the [RAVEN](../../README.md) distributed jobs
platform. **Zero dependencies** — standard library only (`urllib.request`).
Works on Python 3.9+.

```bash
pip install ./sdk/python        # from the repo checkout
# or just drop the raven/ package on your PYTHONPATH
```

## Quick start

```python
from raven import Client, RavenError

client = Client(base_url="http://localhost:8080")
client.login("me@example.com", "correct horse battery")
# The token pair now lives on the client and refreshes itself
# when the access token nears expiry.

job = client.create_job(
    type="webhook",
    payload={"url": "https://me.example/hook", "event": "deploy.done"},
)
print(job.id, job.status)

final = client.watch_job(job.id, interval=2.0, timeout=120)
print(final.status)  # SUCCESS | FAILED | CANCELLED | DEAD
```

See [examples/basic.py](examples/basic.py) for a longer tour.

## Authentication — two flavors

| Flavor | How | Notes |
|--------|-----|-------|
| Email + password (JWT) | `client.login(email, password)` | Access tokens live 15 min; the client rotates the pair automatically before they expire, and once more on a surprise `401`. |
| API key | `Client(api_key="rav_live_...")` | Static machine credential; nothing to refresh. Create keys with `client.create_api_key`. |

Persist the session across restarts by saving `client.tokens` (a
`TokenPair` dataclass) and passing it back as `Client(tokens=saved)` — the
stored refresh token rotates on every refresh, so always persist the latest
pair.

## Capabilities

- **Auth**: `register`, `login`, `refresh`, `logout`
- **Jobs**: `create_job`, `list_jobs`, `get_job`, `cancel_job`,
  `requeue_job`, `replay_job`, `job_deliveries`, `watch_job` (blocking) /
  `iter_job` (generator that yields each status change)
- **Cron schedules**: `create_cron`, `list_crons`, `delete_cron`
- **API keys**: `create_api_key`, `list_api_keys`, `revoke_api_key`
- **Ops**: `list_workers`, `health_services`, `list_audit_events` (admin)

All models are `@dataclass`es with type hints — your editor autocompletes
`job.status`, `cron.next_run_at`, `delivery.status_code` and friends.

## Idempotency

Every mutating call automatically sends an `Idempotency-Key` header (random
UUID hex). When you retry a create after a network timeout, pin the key so
the retry returns the *same* job instead of a duplicate:

```python
job = client.create_job(
    "webhook", {"url": "https://me.example/hook"},
    idempotency_key="deploy-2026-01-01-001",
)
```

## Errors

API failures raise `RavenError` with the stable machine `code`, the
client-safe `message`, the `request_id` from the gateway logs and the HTTP
`status_code`:

```python
from raven import RavenError

try:
    job = client.get_job("job_nope")
except RavenError as err:
    if err.code == "job_not_found":
        print("no such job")
    print(err.code, err.message, err.request_id, err.status_code)
```

Transport problems (DNS, refused connection, timeout) raise `RavenError`
with code `transport_error`.

## Configuration

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `base_url` | `http://localhost:8080` | Point at another gateway. |
| `timeout` | `10.0` | Per-request timeout in seconds. |
| `api_key` | — | `Authorization: ApiKey rav_live_...` on every call. |
| `tokens` | — | Restore a persisted session. |
| `idempotency_key_factory` | `uuid4().hex` | Custom key generator (tests). |
| `opener` | — | Custom `urllib.request.OpenerDirector` (proxies, tests). |

## Not included (yet)

WebSocket streaming (`/ws`) is not part of this SDK — `watch_job` polls the
REST API instead. A streaming helper may land in a future version.

## Development

```bash
python -m unittest discover
```

The tests spin up a mock gateway with `http.server` in a thread; no live
stack needed.
