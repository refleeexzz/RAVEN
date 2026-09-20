# raven-sdk — RAVEN JavaScript SDK

The official JavaScript client for the [RAVEN](../../README.md) distributed
jobs platform. **Zero dependencies**, native `fetch`, Node.js >= 18. ESM
only (`"type": "module"`).

```bash
# from the repo checkout
npm install ./sdk/javascript
# or import directly: import { RavenClient } from './sdk/javascript/src/index.js'
```

## Quick start

```js
import { RavenClient } from 'raven-sdk';

const client = new RavenClient({ baseUrl: 'http://localhost:8080' });
await client.login('me@example.com', 'correct horse battery');
// The token pair now lives on the client and refreshes itself
// when the access token nears expiry.

const job = await client.createJob({
  type: 'webhook',
  payload: { url: 'https://me.example/hook', event: 'deploy.done' },
});
console.log(job.id, job.status);

const final = await client.watchJob(job.id, { intervalMs: 2000, timeoutMs: 120_000 });
console.log(final.status); // SUCCESS | FAILED | CANCELLED | DEAD
```

See [examples/basic.js](examples/basic.js) for a longer tour.

## Authentication — two flavors

| Flavor | How | Notes |
|--------|-----|-------|
| Email + password (JWT) | `await client.login(email, password)` | Access tokens live 15 min; the client rotates the pair automatically before they expire, and once more on a surprise `401`. |
| API key | `new RavenClient({ apiKey: 'rav_live_...' })` | Static machine credential; nothing to refresh. Create keys with `client.createApiKey`. |

Persist the session across restarts by saving `client.tokens` and passing
it back as `new RavenClient({ tokens: saved })` — the stored refresh token
rotates on every refresh, so always persist the latest pair.

## Capabilities

- **Auth**: `register`, `login`, `refresh`, `logout`
- **Jobs**: `createJob`, `listJobs`, `getJob`, `cancelJob`, `requeueJob`,
  `replayJob`, `jobDeliveries`, `watchJob` (blocking) / `iterateJob`
  (async generator yielding each status change)
- **Cron schedules**: `createCron`, `listCrons`, `deleteCron`
- **API keys**: `createApiKey`, `listApiKeys`, `revokeApiKey`
- **Ops**: `listWorkers`, `healthServices`, `listAuditEvents` (admin)

Response bodies keep the API's snake_case wire shape (`job.max_attempts`,
`cron.next_run_at`) — what you read in
[docs/api.md](../../docs/api.md) is what you get. Token pairs are the one
exception: they come back camelCase (`tokens.accessToken`).

## Idempotency

Every mutating call automatically sends an `Idempotency-Key` header (random
UUID hex). When you retry a create after a network timeout, pin the key so
the retry returns the *same* job instead of a duplicate:

```js
const job = await client.createJob({
  type: 'webhook',
  payload: { url: 'https://me.example/hook' },
  idempotencyKey: 'deploy-2026-01-01-001',
});
```

## Errors

API failures throw `RavenError` with the stable machine `code`, the
client-safe `message`, the `requestId` from the gateway logs and the HTTP
`statusCode`. Transport problems throw `TransportError`; an exceeded
timeout throws `TimeoutError`. Both extend `RavenError`.

```js
import { RavenError, TimeoutError } from 'raven-sdk';

try {
  await client.getJob('job_nope');
} catch (err) {
  if (RavenError.isCode(err, 'job_not_found')) {
    console.log('no such job');
  }
  console.log(err.code, err.message, err.requestId, err.statusCode);
}
```

## Configuration

| Option | Default | Purpose |
|--------|---------|---------|
| `baseUrl` | `http://localhost:8080` | Point at another gateway. |
| `timeoutMs` | `10000` | Per-request timeout (AbortSignal). |
| `apiKey` | — | `Authorization: ApiKey rav_live_...` on every call. |
| `tokens` | — | Restore a persisted session. |
| `idempotencyKeyFactory` | `crypto.randomUUID` | Custom key generator (tests). |
| `fetchImpl` | global `fetch` | Custom fetch (proxies, tests). |

## Not included (yet)

WebSocket streaming (`/ws`) is not part of this SDK — `watchJob` /
`iterateJob` poll the REST API instead. A streaming helper may land in a
future version.

## Development

```bash
node --test
```

The tests spin up a mock gateway with `node:http`; no live stack needed.
