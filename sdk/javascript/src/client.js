/**
 * Core client for the RAVEN JavaScript SDK.
 * Zero dependencies — uses the global fetch (Node.js >= 18).
 */

import { randomUUID } from 'node:crypto';
import { RavenError, TimeoutError, TransportError } from './errors.js';

export const DEFAULT_BASE_URL = 'http://localhost:8080';
export const DEFAULT_TIMEOUT_MS = 10_000;

/** Refresh this early before the stated expiry so a request never flies
 * with a token that dies mid-flight. */
const REFRESH_SKEW_MS = 30_000;

const TERMINAL_STATUSES = new Set(['SUCCESS', 'FAILED', 'CANCELLED', 'DEAD']);

/** True when a job status is one it will never leave on its own. */
export function isTerminalStatus(status) {
  return TERMINAL_STATUSES.has(status);
}

function newIdempotencyKey() {
  return randomUUID().replaceAll('-', '');
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Client for the RAVEN gateway's public REST API.
 *
 * @example
 * const client = new RavenClient({ baseUrl: 'http://localhost:8080' });
 * await client.login('me@example.com', 'correct horse battery');
 * const job = await client.createJob({ type: 'webhook', payload: { url: 'https://me.example/hook' } });
 * const final = await client.watchJob(job.id);
 */
export class RavenClient {
  /**
   * @param {object} [options]
   * @param {string} [options.baseUrl] Gateway address (trailing slash trimmed).
   * @param {string} [options.apiKey] Static machine credential (rav_live_...).
   *   When set, the client sends `Authorization: ApiKey <key>` and never refreshes.
   * @param {TokenPair} [options.tokens] Existing token pair, e.g. persisted
   *   from an earlier session. Refreshed automatically near expiry.
   * @param {number} [options.timeoutMs] Per-request timeout (default 10000).
   * @param {() => string} [options.idempotencyKeyFactory] Override key
   *   generation for mutating requests (tests).
   * @param {typeof fetch} [options.fetchImpl] Custom fetch (tests, proxies).
   */
  constructor({
    baseUrl = DEFAULT_BASE_URL,
    apiKey,
    tokens,
    timeoutMs = DEFAULT_TIMEOUT_MS,
    idempotencyKeyFactory = newIdempotencyKey,
    fetchImpl = globalThis.fetch,
  } = {}) {
    this.baseUrl = baseUrl.replace(/\/+$/, '');
    this.apiKey = apiKey;
    this.tokens = tokens ?? null;
    this.timeoutMs = timeoutMs;
    this._idemKeyFn = idempotencyKeyFactory;
    this._fetch = fetchImpl;
    /** @type {Promise<void> | null} serializes concurrent refreshes */
    this._refreshing = null;
  }

  // ------------------------------------------------------------------
  // Request plumbing
  // ------------------------------------------------------------------

  /**
   * Perform one request and decode the JSON body.
   * @param {string} method
   * @param {string} path
   * @param {object} [options]
   * @param {unknown} [options.body]
   * @param {Record<string, string | number | undefined>} [options.query]
   * @param {boolean} [options.mutating] Send an Idempotency-Key header.
   * @param {string} [options.idempotencyKey] Pin the key (retry-safe).
   * @param {boolean} [options.retried] Internal: already retried once on 401.
   */
  async _request(method, path, { body, query, mutating = false, idempotencyKey, retried = false } = {}) {
    await this._ensureFreshToken();

    let url = this.baseUrl + path;
    if (query) {
      const params = new URLSearchParams();
      for (const [k, v] of Object.entries(query)) {
        if (v !== undefined && v !== null && v !== '') params.set(k, String(v));
      }
      const qs = params.toString();
      if (qs) url += `?${qs}`;
    }

    const headers = {
      Accept: 'application/json',
      'User-Agent': 'raven-js-sdk/1.0',
    };
    if (body !== undefined) headers['Content-Type'] = 'application/json';
    if (mutating) headers['Idempotency-Key'] = idempotencyKey ?? this._idemKeyFn();
    if (this.apiKey) {
      headers.Authorization = `ApiKey ${this.apiKey}`;
    } else if (this.tokens?.accessToken) {
      headers.Authorization = `Bearer ${this.tokens.accessToken}`;
    }

    let response;
    try {
      response = await this._fetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(this.timeoutMs),
      });
    } catch (err) {
      if (err?.name === 'TimeoutError' || err?.name === 'AbortError') {
        throw new TimeoutError(`${method} ${path} exceeded ${this.timeoutMs}ms`, err);
      }
      throw new TransportError(`${method} ${path} failed: ${err?.message ?? err}`, err);
    }

    const text = await response.text();

    // One silent retry: the access token died between the freshness check
    // and the server validation (clock skew).
    if (response.status === 401 && !retried && !this.apiKey && this.tokens?.refreshToken) {
      await this._refreshNow();
      return this._request(method, path, { body, query, mutating, idempotencyKey, retried: true });
    }

    if (response.status < 200 || response.status >= 300) {
      throw decodeError(response.status, text);
    }

    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      throw new RavenError({
        code: 'bad_response',
        message: `${method} ${path} returned invalid JSON`,
        statusCode: response.status,
      });
    }
  }

  // ------------------------------------------------------------------
  // Token lifecycle
  // ------------------------------------------------------------------

  /** Refresh the pair when the access token is expired or close to it. */
  async _ensureFreshToken() {
    if (this.apiKey || !this.tokens?.refreshToken) return;
    if (Date.now() + REFRESH_SKEW_MS >= this.tokens.accessExpiresAt * 1000) {
      await this._refreshNow();
    }
  }

  /** Rotate the token pair. Concurrent callers share one round-trip. */
  async _refreshNow() {
    if (!this.tokens?.refreshToken) {
      throw new RavenError({ code: 'no_refresh_token', message: 'client has no refresh token' });
    }
    this._refreshing ??= (async () => {
      let response;
      try {
        response = await this._fetch(`${this.baseUrl}/api/auth/refresh`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Accept: 'application/json',
            'User-Agent': 'raven-js-sdk/1.0',
          },
          body: JSON.stringify({ refresh_token: this.tokens.refreshToken }),
          signal: AbortSignal.timeout(this.timeoutMs),
        });
      } catch (err) {
        throw new TransportError(`token refresh failed: ${err?.message ?? err}`, err);
      }
      const text = await response.text();
      if (response.status < 200 || response.status >= 300) {
        throw decodeError(response.status, text);
      }
      this.tokens = tokenPairFromJSON(JSON.parse(text));
    })();
    try {
      await this._refreshing;
    } finally {
      this._refreshing = null;
    }
  }

  // ------------------------------------------------------------------
  // Auth
  // ------------------------------------------------------------------

  /**
   * Create a USER-role account. Does not log the user in — call login next.
   * @param {{email: string, password: string, displayName?: string}} input
   */
  async register({ email, password, displayName }) {
    const body = { email, password };
    if (displayName) body.display_name = displayName;
    return this._request('POST', '/api/auth/register', { body });
  }

  /**
   * Exchange credentials for a token pair and store it on the client.
   * @param {string} email @param {string} password
   * @returns {Promise<TokenPair>}
   */
  async login(email, password) {
    const data = await this._request('POST', '/api/auth/login', { body: { email, password } });
    this.tokens = tokenPairFromJSON(data);
    return this.tokens;
  }

  /**
   * Rotate the stored refresh token for a fresh pair. Normally automatic;
   * exposed for explicit session management.
   * @returns {Promise<TokenPair>}
   */
  async refresh() {
    await this._refreshNow();
    return this.tokens;
  }

  /** Kill the session behind the stored refresh token and clear it locally. */
  async logout() {
    if (this.tokens?.refreshToken) {
      await this._request('POST', '/api/auth/logout', {
        body: { refresh_token: this.tokens.refreshToken },
      });
    }
    this.tokens = null;
  }

  // ------------------------------------------------------------------
  // Jobs
  // ------------------------------------------------------------------

  /**
   * Queue a job. An Idempotency-Key is generated automatically; pass
   * `idempotencyKey` to pin it when retrying after a timeout — the same
   * key always returns the same job.
   *
   * @param {object} input
   * @param {string} input.type One of send_email | resize_image | webhook.
   * @param {Record<string, unknown>} input.payload Must be a JSON object.
   * @param {number} [input.priority] 1–9, default 5 (1 = most urgent).
   * @param {number} [input.maxAttempts] 1–25, default 4.
   * @param {number} [input.scheduledAt] Unix seconds in the future; omit to run now.
   * @param {string} [input.idempotencyKey]
   * @returns {Promise<Job>}
   */
  async createJob({ type, payload, priority, maxAttempts, scheduledAt, idempotencyKey }) {
    const body = { type, payload };
    if (priority !== undefined) body.priority = priority;
    if (maxAttempts !== undefined) body.max_attempts = maxAttempts;
    if (scheduledAt !== undefined) body.scheduled_at = scheduledAt;
    return this._request('POST', '/api/jobs', { body, mutating: true, idempotencyKey });
  }

  /**
   * Page through jobs.
   * @param {object} [filter]
   * @param {string} [filter.status] QUEUED | PROCESSING | SUCCESS | FAILED |
   *   RETRYING | CANCELLED | DEAD | SCHEDULED (any casing).
   * @param {string} [filter.type] Exact match.
   * @param {number} [filter.page] @param {number} [filter.pageSize]
   * @returns {Promise<{jobs: Job[], page: Page}>}
   */
  async listJobs({ status, type, page, pageSize } = {}) {
    return this._request('GET', '/api/jobs', { query: { status, type, page, page_size: pageSize } });
  }

  /** Fetch one job by id. @param {string} id @returns {Promise<Job>} */
  async getJob(id) {
    return this._request('GET', `/api/jobs/${encodeURIComponent(id)}`);
  }

  /**
   * Move a QUEUED/RETRYING/SCHEDULED job to CANCELLED. Jobs that already
   * moved on answer 409 job_not_cancellable.
   * @param {string} id @param {{idempotencyKey?: string}} [opts] @returns {Promise<Job>}
   */
  async cancelJob(id, { idempotencyKey } = {}) {
    return this._request('POST', `/api/jobs/${encodeURIComponent(id)}/cancel`, {
      mutating: true,
      idempotencyKey,
    });
  }

  /**
   * Resurrect a DEAD job to QUEUED and republish it. Any other status
   * answers 409 job_not_dead.
   * @param {string} id @param {{idempotencyKey?: string}} [opts] @returns {Promise<Job>}
   */
  async requeueJob(id, { idempotencyKey } = {}) {
    return this._request('POST', `/api/jobs/${encodeURIComponent(id)}/requeue`, {
      mutating: true,
      idempotencyKey,
    });
  }

  /**
   * Clone a job into a brand-new one (fresh id, zero attempts,
   * replayed_from pointing at the source). Works on any state.
   * @param {string} id @param {{idempotencyKey?: string}} [opts] @returns {Promise<Job>}
   */
  async replayJob(id, { idempotencyKey } = {}) {
    return this._request('POST', `/api/jobs/${encodeURIComponent(id)}/replay`, {
      mutating: true,
      idempotencyKey,
    });
  }

  /**
   * Webhook delivery history of a job, oldest first.
   * @param {string} id @param {{page?: number, pageSize?: number}} [opts]
   * @returns {Promise<{deliveries: Delivery[], page: Page}>}
   */
  async jobDeliveries(id, { page, pageSize } = {}) {
    return this._request('GET', `/api/jobs/${encodeURIComponent(id)}/deliveries`, {
      query: { page, page_size: pageSize },
    });
  }

  /**
   * Async generator yielding a snapshot whenever the job's status changes,
   * then the terminal snapshot, and stopping. Polls every `intervalMs`.
   * @param {string} id @param {{intervalMs?: number, signal?: AbortSignal}} [opts]
   * @yields {Job}
   */
  async *iterateJob(id, { intervalMs = 2000, signal } = {}) {
    let lastStatus;
    for (;;) {
      signal?.throwIfAborted();
      const job = await this.getJob(id);
      if (job.status !== lastStatus || isTerminalStatus(job.status)) {
        yield job;
        lastStatus = job.status;
      }
      if (isTerminalStatus(job.status)) return;
      await sleep(intervalMs);
    }
  }

  /**
   * Poll the job until it reaches a terminal state and return the final
   * snapshot. Throws a RavenError with code "watch_timeout" when
   * `timeoutMs` elapses first, or "aborted" if the signal fires.
   *
   * @param {string} id
   * @param {{intervalMs?: number, timeoutMs?: number, signal?: AbortSignal}} [opts]
   * @returns {Promise<Job>}
   */
  async watchJob(id, { intervalMs = 2000, timeoutMs, signal } = {}) {
    const deadline = timeoutMs === undefined ? undefined : Date.now() + timeoutMs;
    for (;;) {
      signal?.throwIfAborted();
      const job = await this.getJob(id);
      if (isTerminalStatus(job.status)) return job;
      if (deadline !== undefined && Date.now() >= deadline) {
        throw new RavenError({
          code: 'watch_timeout',
          message: `job ${id} did not reach a terminal state in ${timeoutMs}ms`,
        });
      }
      await sleep(intervalMs);
    }
  }

  // ------------------------------------------------------------------
  // Cron schedules
  // ------------------------------------------------------------------

  /**
   * Register a recurring schedule. `cronExpr` is the classic 5-field form
   * "min hour dom month dow". The response includes the computed
   * next_run_at.
   *
   * @param {object} input
   * @param {string} input.name @param {string} input.cronExpr
   * @param {string} input.type @param {Record<string, unknown>} input.payload
   * @param {number} [input.priority] @param {boolean} [input.enabled]
   * @param {string} [input.idempotencyKey]
   * @returns {Promise<Cron>}
   */
  async createCron({ name, cronExpr, type, payload, priority, enabled, idempotencyKey }) {
    const body = { name, cron_expr: cronExpr, type, payload };
    if (priority !== undefined) body.priority = priority;
    if (enabled !== undefined) body.enabled = enabled;
    return this._request('POST', '/api/crons', { body, mutating: true, idempotencyKey });
  }

  /**
   * Page through the caller's schedules.
   * @param {{page?: number, pageSize?: number}} [opts]
   * @returns {Promise<{crons: Cron[], page: Page}>}
   */
  async listCrons({ page, pageSize } = {}) {
    return this._request('GET', '/api/crons', { query: { page, page_size: pageSize } });
  }

  /**
   * Hard-delete a schedule: it stops firing immediately. Unknown or foreign
   * ids answer 404 cron_not_found.
   * @param {string} id @param {{idempotencyKey?: string}} [opts]
   */
  async deleteCron(id, { idempotencyKey } = {}) {
    return this._request('DELETE', `/api/crons/${encodeURIComponent(id)}`, {
      mutating: true,
      idempotencyKey,
    });
  }

  // ------------------------------------------------------------------
  // API keys
  // ------------------------------------------------------------------

  /**
   * Mint a new API key. JWT-only route: an API key cannot create more keys
   * (403 api_key_cannot_create_keys). Save `result.key` immediately — it is
   * shown exactly once.
   *
   * @param {object} input
   * @param {string} input.name
   * @param {string[]} input.scopes Subset of the platform permissions.
   * @param {string} [input.idempotencyKey]
   * @returns {Promise<{key: string, api_key: APIKey}>}
   */
  async createApiKey({ name, scopes, idempotencyKey }) {
    return this._request('POST', '/api/keys', {
      body: { name, scopes },
      mutating: true,
      idempotencyKey,
    });
  }

  /**
   * The caller's active keys, newest first. Revoked keys disappear; the
   * secret hash is never returned.
   * @returns {Promise<{api_keys: APIKey[]}>}
   */
  async listApiKeys() {
    return this._request('GET', '/api/keys');
  }

  /**
   * Soft-revoke a key, effective immediately. A key belonging to someone
   * else answers 404 api_key_not_found — no ownership oracle.
   * @param {string} id @param {{idempotencyKey?: string}} [opts]
   */
  async revokeApiKey(id, { idempotencyKey } = {}) {
    return this._request('DELETE', `/api/keys/${encodeURIComponent(id)}`, {
      mutating: true,
      idempotencyKey,
    });
  }

  // ------------------------------------------------------------------
  // Ops
  // ------------------------------------------------------------------

  /**
   * The live worker registry, read straight from Redis: every listed worker
   * heartbeat within the last 15 seconds.
   * @returns {Promise<{workers: Worker[]}>}
   */
  async listWorkers() {
    return this._request('GET', '/api/workers');
  }

  /**
   * The aggregated health grid the gateway computes by probing every
   * service. Public route.
   * @returns {Promise<HealthReport>}
   */
  async healthServices() {
    return this._request('GET', '/api/health/services');
  }

  /**
   * Page the platform audit trail, newest first. Admin-only
   * (users:delete). `beforeId` is the keyset cursor from a previous page's
   * `next_before_id`.
   *
   * @param {object} [filter]
   * @param {string} [filter.action] @param {string} [filter.actor]
   * @param {number} [filter.limit] @param {number} [filter.beforeId]
   * @returns {Promise<{events: AuditEvent[], next_before_id?: number}>}
   */
  async listAuditEvents({ action, actor, limit, beforeId } = {}) {
    return this._request('GET', '/api/audit', {
      query: { action, actor, limit, before_id: beforeId },
    });
  }
}

/** Build the error from a non-2xx response body. */
function decodeError(status, text) {
  try {
    const envelope = JSON.parse(text);
    if (envelope?.error?.code) {
      return new RavenError({
        code: envelope.error.code,
        message: envelope.error.message ?? '',
        requestId: envelope.error.request_id,
        statusCode: status,
      });
    }
  } catch {
    // not the standard envelope — fall through
  }
  return new RavenError({ code: `http_${status}`, message: `HTTP ${status}`, statusCode: status });
}

/** @param {Record<string, unknown>} data @returns {TokenPair} */
function tokenPairFromJSON(data) {
  return {
    accessToken: data.access_token,
    refreshToken: data.refresh_token,
    accessExpiresAt: Number(data.access_expires_at),
    refreshExpiresAt: Number(data.refresh_expires_at),
  };
}

/**
 * @typedef {object} TokenPair Camel-case twin of the wire shape, because
 * this is JavaScript. `accessExpiresAt` / `refreshExpiresAt` are Unix seconds.
 * @property {string} accessToken @property {string} refreshToken
 * @property {number} accessExpiresAt @property {number} refreshExpiresAt
 */

/**
 * @typedef {object} Job Wire shape as returned by the API (snake_case).
 * @property {string} id @property {string} type @property {object} payload
 * @property {string} status @property {number} priority @property {number} attempts
 * @property {number} max_attempts @property {number} created_at
 * @property {number} started_at @property {number} finished_at
 * @property {string} error @property {string} worker_id
 * @property {number} scheduled_at @property {string} [replayed_from]
 */

/**
 * @typedef {object} Page @property {number} page @property {number} page_size
 * @property {number} total
 */

/**
 * @typedef {object} Delivery One webhook delivery attempt. `status_code`
 * and `latency_ms` are null when no response ever came back.
 */

/**
 * @typedef {object} Cron A recurring schedule; times are Unix seconds.
 */

/**
 * @typedef {object} APIKey Key metadata — the secret is only shown once,
 * at creation.
 */

/**
 * @typedef {object} Worker Live worker from the Redis registry; counters
 * arrive as strings.
 */

/**
 * @typedef {object} HealthReport @property {string} checked_at
 * @property {Array<{name: string, status: string, latency_ms: number, detail?: string}>} services
 */

/**
 * @typedef {object} AuditEvent One row of the platform audit trail.
 */
