// Tests for the RAVEN JavaScript SDK, running against a mock gateway from
// node:http — no live stack needed. Run with: node --test

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';

import { RavenClient, RavenError, TimeoutError, TransportError, isTerminalStatus } from '../src/index.js';

const jobJSON = (id = 'job_1', status = 'QUEUED', extra = {}) => ({
  id,
  type: 'webhook',
  payload: { url: 'https://x.example' },
  status,
  priority: 5,
  attempts: 0,
  max_attempts: 4,
  created_at: 1759998000,
  started_at: 0,
  finished_at: 0,
  error: '',
  worker_id: '',
  scheduled_at: 0,
  ...extra,
});

/** Spin up a mock gateway. routes: Map<"METHOD /path", (req, body) => [status, payload]>. */
async function startMock(routes) {
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString();
      const body = raw ? JSON.parse(raw) : null;
      const key = `${req.method} ${req.url.split('?')[0]}`;
      const handler = routes.get(key);
      if (!handler) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: { code: 'not_mocked', message: `no mock for ${key}`, request_id: 'mock' } }));
        return;
      }
      const [status, payload] = handler(req, body);
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(typeof payload === 'string' ? payload : JSON.stringify(payload));
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  return { server, baseUrl, close: () => new Promise((r) => server.close(r)) };
}

describe('auth', () => {
  let mock;
  before(async () => {
    const routes = new Map([
      ['POST /api/auth/register', (req, body) => {
        assert.equal(body.email, 'me@example.com');
        assert.equal(body.display_name, 'Me');
        return [201, { user_id: 'u-1', email: 'me@example.com' }];
      }],
      ['POST /api/auth/login', () => [200, {
        access_token: 'at-1', refresh_token: 'rt-1',
        access_expires_at: 9999999999, refresh_expires_at: 9999999999,
      }]],
      ['POST /api/auth/logout', (req, body) => {
        assert.equal(body.refresh_token, 'rt-gone');
        return [200, { ok: true }];
      }],
    ]);
    mock = await startMock(routes);
  });
  after(() => mock.close());

  it('registers a user', async () => {
    const client = new RavenClient({ baseUrl: mock.baseUrl });
    const resp = await client.register({ email: 'me@example.com', password: '12345678', displayName: 'Me' });
    assert.equal(resp.user_id, 'u-1');
  });

  it('logs in and stores a camelCase token pair', async () => {
    const client = new RavenClient({ baseUrl: mock.baseUrl });
    const pair = await client.login('me@example.com', '12345678');
    assert.equal(pair.accessToken, 'at-1');
    assert.equal(client.tokens.refreshToken, 'rt-1');
    assert.equal(client.tokens.accessExpiresAt, 9999999999);
  });

  it('logs out and clears tokens', async () => {
    const client = new RavenClient({ baseUrl: mock.baseUrl });
    client.tokens = { accessToken: 'at', refreshToken: 'rt-gone', accessExpiresAt: 9999999999, refreshExpiresAt: 9999999999 };
    await client.logout();
    assert.equal(client.tokens, null);
  });
});

describe('token refresh', () => {
  it('refreshes an expired access token before the request', async () => {
    let refreshCalls = 0;
    let bearerSeen;
    const routes = new Map([
      ['POST /api/auth/refresh', (req, body) => {
        refreshCalls += 1;
        assert.equal(body.refresh_token, 'rt-old');
        return [200, {
          access_token: 'at-new', refresh_token: 'rt-new',
          access_expires_at: 9999999999, refresh_expires_at: 9999999999,
        }];
      }],
      ['GET /api/jobs/job_1', (req) => {
        bearerSeen = req.headers.authorization;
        return [200, jobJSON('job_1', 'SUCCESS')];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({
        baseUrl: mock.baseUrl,
        tokens: {
          accessToken: 'at-old', refreshToken: 'rt-old',
          accessExpiresAt: Math.floor(Date.now() / 1000) - 60, // stale
          refreshExpiresAt: 9999999999,
        },
      });
      const job = await client.getJob('job_1');
      assert.equal(job.status, 'SUCCESS');
      assert.equal(refreshCalls, 1);
      assert.equal(bearerSeen, 'Bearer at-new');
      assert.equal(client.tokens.refreshToken, 'rt-new');
    } finally {
      await mock.close();
    }
  });

  it('retries once on a surprise 401', async () => {
    let jobCalls = 0;
    const routes = new Map([
      ['POST /api/auth/refresh', () => [200, {
        access_token: 'at-new', refresh_token: 'rt-new',
        access_expires_at: 9999999999, refresh_expires_at: 9999999999,
      }]],
      ['GET /api/jobs/job_1', () => {
        jobCalls += 1;
        if (jobCalls === 1) {
          return [401, { error: { code: 'token_expired', message: 'expired', request_id: 'r1' } }];
        }
        return [200, jobJSON('job_1', 'SUCCESS')];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({
        baseUrl: mock.baseUrl,
        tokens: { accessToken: 'at-old', refreshToken: 'rt-old', accessExpiresAt: 9999999999, refreshExpiresAt: 9999999999 },
      });
      const job = await client.getJob('job_1');
      assert.equal(job.status, 'SUCCESS');
      assert.equal(jobCalls, 2);
    } finally {
      await mock.close();
    }
  });
});

describe('jobs', () => {
  it('sends an auto Idempotency-Key on create and honors overrides', async () => {
    const keysSeen = [];
    const routes = new Map([
      ['POST /api/jobs', (req, body) => {
        keysSeen.push(req.headers['idempotency-key']);
        assert.equal(body.payload.url, 'https://x.example');
        assert.equal(body.priority, 3);
        return [201, jobJSON()];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const job = await client.createJob({ type: 'webhook', payload: { url: 'https://x.example' }, priority: 3 });
      assert.equal(job.id, 'job_1');
      await client.createJob({
        type: 'webhook', payload: { url: 'https://x.example' }, priority: 3,
        idempotencyKey: 'retry-42',
      });
      assert.ok(keysSeen[0], 'first key auto-generated');
      assert.equal(keysSeen[1], 'retry-42');
    } finally {
      await mock.close();
    }
  });

  it('lists jobs with filters', async () => {
    const routes = new Map([
      ['GET /api/jobs', (req) => {
        const query = new URL(req.url, 'http://x').searchParams;
        assert.equal(query.get('status'), 'failed');
        assert.equal(query.get('page'), '2');
        return [200, { jobs: [jobJSON('job_9', 'FAILED')], page: { page: 2, page_size: 20, total: 1 } }];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const { jobs, page } = await client.listJobs({ status: 'failed', page: 2 });
      assert.equal(jobs.length, 1);
      assert.equal(page.total, 1);
      assert.ok(isTerminalStatus(jobs[0].status));
    } finally {
      await mock.close();
    }
  });

  it('cancel / requeue / replay hit the right paths', async () => {
    const paths = [];
    const routes = new Map([
      ['POST /api/jobs/job_1/cancel', (req) => { paths.push(req.url); return [200, jobJSON('job_1', 'CANCELLED')]; }],
      ['POST /api/jobs/job_1/requeue', (req) => { paths.push(req.url); return [200, jobJSON('job_1', 'QUEUED')]; }],
      ['POST /api/jobs/job_1/replay', (req) => { paths.push(req.url); return [201, jobJSON('job_2', 'QUEUED', { replayed_from: 'job_1' })]; }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      assert.equal((await client.cancelJob('job_1')).status, 'CANCELLED');
      assert.equal((await client.requeueJob('job_1')).status, 'QUEUED');
      const replay = await client.replayJob('job_1');
      assert.equal(replay.id, 'job_2');
      assert.equal(replay.replayed_from, 'job_1');
      assert.deepEqual(paths, ['/api/jobs/job_1/cancel', '/api/jobs/job_1/requeue', '/api/jobs/job_1/replay']);
    } finally {
      await mock.close();
    }
  });

  it('decodes deliveries with nullable fields', async () => {
    const routes = new Map([
      ['GET /api/jobs/job_1/deliveries', () => [200, {
        deliveries: [
          { id: 41, job_id: 'job_1', attempt: 1, url: 'https://x.example', status_code: 200, latency_ms: 83, response_snippet: '{}', blocked: false, error: '', ts: 1767225600 },
          { id: 42, job_id: 'job_1', attempt: 2, url: 'https://x.example', status_code: null, latency_ms: null, response_snippet: '', blocked: true, error: 'egress guard', ts: 1767225601 },
        ],
        page: { page: 1, page_size: 20, total: 2 },
      }]],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const { deliveries } = await client.jobDeliveries('job_1');
      assert.equal(deliveries.length, 2);
      assert.equal(deliveries[0].status_code, 200);
      assert.equal(deliveries[1].status_code, null);
      assert.equal(deliveries[1].blocked, true);
    } finally {
      await mock.close();
    }
  });

  it('watchJob polls until a terminal state', async () => {
    let calls = 0;
    const routes = new Map([
      ['GET /api/jobs/job_1', () => {
        calls += 1;
        return [200, jobJSON('job_1', calls < 3 ? 'PROCESSING' : 'SUCCESS')];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const final = await client.watchJob('job_1', { intervalMs: 5, timeoutMs: 5000 });
      assert.equal(final.status, 'SUCCESS');
      assert.ok(calls >= 3);
    } finally {
      await mock.close();
    }
  });

  it('watchJob times out when the job never terminates', async () => {
    const routes = new Map([
      ['GET /api/jobs/job_1', () => [200, jobJSON('job_1', 'PROCESSING')]],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      await assert.rejects(
        client.watchJob('job_1', { intervalMs: 5, timeoutMs: 60 }),
        (err) => err instanceof RavenError && err.code === 'watch_timeout',
      );
    } finally {
      await mock.close();
    }
  });

  it('iterateJob yields status transitions', async () => {
    let calls = 0;
    const sequence = ['QUEUED', 'PROCESSING', 'SUCCESS'];
    const routes = new Map([
      ['GET /api/jobs/job_1', () => {
        calls += 1;
        return [200, jobJSON('job_1', sequence[Math.min(calls - 1, 2)])];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const seen = [];
      for await (const job of client.iterateJob('job_1', { intervalMs: 5 })) {
        seen.push(job.status);
      }
      assert.deepEqual(seen, ['QUEUED', 'PROCESSING', 'SUCCESS']);
    } finally {
      await mock.close();
    }
  });
});

describe('crons, keys and ops', () => {
  it('cron lifecycle', async () => {
    const routes = new Map([
      ['POST /api/crons', (req, body) => {
        assert.equal(body.cron_expr, '0 3 * * *');
        return [201, {
          id: 'cron_1', name: 'nightly', cron_expr: '0 3 * * *', type: 'webhook',
          payload: { url: 'https://x.example' }, priority: 5, enabled: true,
          next_run_at: 1767225600, last_run_at: 0, created_at: 1767220000,
        }];
      }],
      ['GET /api/crons', () => [200, {
        crons: [{ id: 'cron_1', name: 'n', cron_expr: '0 3 * * *', type: 'webhook', payload: {}, priority: 5, enabled: true, next_run_at: 1, last_run_at: 0, created_at: 1 }],
        page: { page: 1, page_size: 20, total: 1 },
      }]],
      ['DELETE /api/crons/cron_1', () => [200, { ok: true }]],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const cron = await client.createCron({
        name: 'nightly', cronExpr: '0 3 * * *', type: 'webhook',
        payload: { url: 'https://x.example' },
      });
      assert.equal(cron.next_run_at, 1767225600);
      assert.equal((await client.listCrons()).page.total, 1);
      await client.deleteCron('cron_1');
    } finally {
      await mock.close();
    }
  });

  it('api key lifecycle + ApiKey auth header', async () => {
    const authHeaders = [];
    const routes = new Map([
      ['POST /api/keys', (req, body) => {
        assert.deepEqual(body.scopes, ['jobs:read']);
        return [201, {
          key: 'rav_live_secret',
          api_key: { id: 'k1', name: 'ci', prefix: 'rav_live_9f2k', scopes: ['jobs:read'], created_at: '2026-01-01T12:00:00Z', last_used_at: null },
        }];
      }],
      ['GET /api/keys', (req) => {
        authHeaders.push(req.headers.authorization);
        return [200, { api_keys: [{ id: 'k1', name: 'ci', prefix: 'rav_live_9f2k', scopes: ['jobs:read'], created_at: '2026-01-01T12:00:00Z', last_used_at: '2026-01-02T08:30:00Z' }] }];
      }],
      ['DELETE /api/keys/k1', (req) => {
        authHeaders.push(req.headers.authorization);
        return [200, { ok: true }];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const created = await client.createApiKey({ name: 'ci', scopes: ['jobs:read'] });
      assert.equal(created.key, 'rav_live_secret');
      assert.equal(created.api_key.prefix, 'rav_live_9f2k');

      const keyClient = new RavenClient({ baseUrl: mock.baseUrl, apiKey: 'rav_live_abc' });
      const { api_keys: keys } = await keyClient.listApiKeys();
      assert.equal(keys[0].last_used_at, '2026-01-02T08:30:00Z');
      await keyClient.revokeApiKey('k1');
      assert.deepEqual(authHeaders, ['ApiKey rav_live_abc', 'ApiKey rav_live_abc']);
    } finally {
      await mock.close();
    }
  });

  it('workers + health + audit', async () => {
    const routes = new Map([
      ['GET /api/workers', () => [200, { workers: [{ id: 'w-1', started_at: '2025-10-09T12:00:00Z', last_heartbeat: '2025-10-09T12:04:35Z', jobs_processed: '138', in_flight: '2' }] }]],
      ['GET /api/health/services', () => [200, {
        checked_at: '2026-01-01T12:00:00Z',
        services: [
          { name: 'gateway', status: 'ok', latency_ms: 0, detail: 'self' },
          { name: 'worker_pool', status: 'degraded', latency_ms: 5, detail: 'no workers registered' },
        ],
      }]],
      ['GET /api/audit', (req) => {
        const query = new URL(req.url, 'http://x').searchParams;
        assert.equal(query.get('action'), 'job.cancel');
        assert.equal(query.get('limit'), '10');
        return [200, {
          events: [{ id: 7, ts: '2026-01-01T12:00:00Z', actor_id: 'u-1', action: 'job.cancel', resource_type: 'job', resource_id: 'job_1', outcome: 'allowed' }],
          next_before_id: 7,
        }];
      }],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      const { workers } = await client.listWorkers();
      assert.equal(workers[0].jobs_processed, '138');

      const report = await client.healthServices();
      assert.equal(report.services.length, 2);
      assert.equal(report.services[1].status, 'degraded');

      const audit = await client.listAuditEvents({ action: 'job.cancel', limit: 10 });
      assert.equal(audit.events[0].outcome, 'allowed');
      assert.equal(audit.next_before_id, 7);
    } finally {
      await mock.close();
    }
  });
});

describe('errors', () => {
  it('decodes the standard error envelope', async () => {
    const routes = new Map([
      ['GET /api/jobs/job_nope', () => [404, { error: { code: 'job_not_found', message: 'job does not exist', request_id: 'req-123' } }]],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      await assert.rejects(client.getJob('job_nope'), (err) => {
        assert.ok(err instanceof RavenError);
        assert.equal(err.code, 'job_not_found');
        assert.equal(err.message, 'job does not exist');
        assert.equal(err.requestId, 'req-123');
        assert.equal(err.statusCode, 404);
        assert.ok(RavenError.isCode(err, 'job_not_found'));
        assert.match(String(err), /req-123/);
        return true;
      });
    } finally {
      await mock.close();
    }
  });

  it('falls back on non-envelope error bodies', async () => {
    const routes = new Map([
      ['GET /api/jobs/job_1', () => [502, '<html>proxy exploded</html>']],
    ]);
    const mock = await startMock(routes);
    try {
      const client = new RavenClient({ baseUrl: mock.baseUrl });
      await assert.rejects(client.getJob('job_1'), (err) => {
        assert.equal(err.code, 'http_502');
        assert.equal(err.statusCode, 502);
        return true;
      });
    } finally {
      await mock.close();
    }
  });

  it('raises TransportError when nothing listens', async () => {
    const client = new RavenClient({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 1000 });
    await assert.rejects(client.listWorkers(), (err) => err instanceof TransportError);
  });

  it('raises TimeoutError when the server is too slow', async () => {
    const server = http.createServer((req, res) => {
      setTimeout(() => {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end('{"workers":[]}');
      }, 300);
    });
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    try {
      const client = new RavenClient({
        baseUrl: `http://127.0.0.1:${server.address().port}`,
        timeoutMs: 50,
      });
      await assert.rejects(client.listWorkers(), (err) => err instanceof TimeoutError && err.code === 'timeout');
    } finally {
      await new Promise((r) => server.close(r));
    }
  });
});
