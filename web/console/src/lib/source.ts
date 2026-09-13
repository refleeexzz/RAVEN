import { getJson, postJson, probeGateway } from "./api";
import { config } from "./config";
import { LiveSocket } from "./live";
import { parsePrometheus, pickInteresting } from "./metrics";
import { promInstant, promRange } from "./prometheus";
import { DemoWorld } from "./simulator";
import type {
  ConnState,
  CreateJobInput,
  Health,
  Job,
  JobEvent,
  JobsPage,
  JobsQuery,
  JobStatus,
  MetricSample,
  SeriesPoint,
  ServiceHealth,
  Snapshot,
  TopicInfo,
  WorkerInfo,
} from "./types";
import { JOB_STATUSES, JOB_TYPE_META } from "./types";

// A Source feeds the store with snapshots + events and executes mutations.
// Two implementations: LiveSource (real gateway + broker + websocket) and
// DemoSource (in-memory simulator with the same shapes).

export interface SourceHandlers {
  onEvent: (e: JobEvent) => void;
  onSnapshot: (s: Snapshot) => void;
  onConn: (c: ConnState) => void;
}

export interface Source {
  readonly kind: "live" | "demo";
  start(h: SourceHandlers): void;
  stop(): void;
  queryJobs(q: JobsQuery): Promise<JobsPage>;
  getJob(id: string): Promise<Job | null>;
  createJob(input: CreateJobInput): Promise<Job>;
  cancelJob(id: string): Promise<void>;
  requeueJob(id: string): Promise<void>;
  metricsSummary(): Promise<MetricSample[]>;
}

// ─── shared helpers ──────────────────────────────────────────────────────────

export function filterJobs(jobs: Job[], q: JobsQuery): JobsPage {
  let items = jobs.filter((j) => (q.dlq ? j.status === "DEAD" : j.status !== "DEAD"));
  if (q.statuses.length > 0) items = items.filter((j) => q.statuses.includes(j.status));
  if (q.type !== "all") items = items.filter((j) => j.type === q.type);
  if (q.search.trim()) {
    const needle = q.search.trim().toLowerCase();
    items = items.filter(
      (j) => j.id.toLowerCase().includes(needle) || (j.worker_id ?? "").toLowerCase().includes(needle),
    );
  }
  items = [...items].sort((a, b) => b.created_at.localeCompare(a.created_at));
  const total = items.length;
  const start = (q.page - 1) * q.pageSize;
  return { items: items.slice(start, start + q.pageSize), total, page: q.page, pageSize: q.pageSize };
}

function asRecord(v: unknown): Record<string, unknown> | null {
  return typeof v === "object" && v !== null ? (v as Record<string, unknown>) : null;
}

function pick(r: Record<string, unknown>, ...keys: string[]): unknown {
  for (const k of keys) if (r[k] !== undefined && r[k] !== null) return r[k];
  return undefined;
}

/** Accept both snake_case and Go-style CamelCase field names. */
export function normalizeJob(raw: unknown): Job | null {
  const r = asRecord(raw);
  if (!r) return null;
  const id = pick(r, "id", "ID");
  const statusRaw = String(pick(r, "status", "Status") ?? "").toUpperCase();
  const status = (JOB_STATUSES as string[]).includes(statusRaw) ? (statusRaw as JobStatus) : null;
  if (typeof id !== "string" || !status) return null;
  const typeRaw = String(pick(r, "type", "Type") ?? "webhook");
  const type = (typeRaw in JOB_TYPE_META ? typeRaw : "webhook") as Job["type"];
  const payload = asRecord(pick(r, "payload", "Payload")) ?? {};
  const str = (v: unknown): string | null => (typeof v === "string" ? v : null);
  const num = (v: unknown, d = 0): number => (typeof v === "number" && Number.isFinite(v) ? v : d);
  return {
    id,
    type,
    payload,
    status,
    priority: num(pick(r, "priority", "Priority"), 5),
    attempts: num(pick(r, "attempts", "Attempts"), 0),
    max_attempts: num(pick(r, "max_attempts", "MaxAttempts"), 3),
    created_at: str(pick(r, "created_at", "CreatedAt")) ?? new Date().toISOString(),
    started_at: str(pick(r, "started_at", "StartedAt")),
    finished_at: str(pick(r, "finished_at", "FinishedAt")),
    error: str(pick(r, "error", "Error")),
    worker_id: str(pick(r, "worker_id", "WorkerID")),
    idempotency_key: str(pick(r, "idempotency_key", "IdempotencyKey")),
  };
}

function normalizeJobList(data: unknown): Job[] {
  const r = asRecord(data);
  const arr: unknown[] = Array.isArray(data)
    ? data
    : ((r?.jobs ?? r?.items ?? r?.data ?? []) as unknown[]);
  if (!Array.isArray(arr)) return [];
  return arr.map(normalizeJob).filter((j): j is Job => j !== null);
}

function normalizeTopics(data: unknown, prevRates: Map<string, number[]>): TopicInfo[] {
  const r = asRecord(data);
  const arr: unknown[] = Array.isArray(data) ? data : ((r?.topics ?? r?.data ?? []) as unknown[]);
  if (!Array.isArray(arr)) return [];
  const now = Date.now();
  return arr
    .map((raw) => {
      const t = asRecord(raw);
      if (!t || typeof t.name !== "string") return null;
      const parts = Array.isArray(t.partitions) ? t.partitions : [];
      const groups = Array.isArray(t.consumer_groups) ? t.consumer_groups : [];
      const partitions = parts
        .map((p, i) => {
          const pr = asRecord(p);
          if (!pr) return null;
          const hwm = Number(pick(pr, "high_watermark", "hwm", "HighWatermark") ?? 0);
          return { id: Number(pick(pr, "id", "ID") ?? i), high_watermark: Number.isFinite(hwm) ? hwm : 0 };
        })
        .filter((p): p is { id: number; high_watermark: number } => p !== null);
      const consumer_groups = groups
        .map((g) => {
          const gr = asRecord(g);
          if (!gr) return null;
          const committed = Array.isArray(gr.committed) ? gr.committed.map(Number) : [];
          const lag = Number(gr.lag ?? 0);
          return {
            name: String(gr.name ?? "unknown"),
            committed,
            lag: Number.isFinite(lag) ? lag : 0,
          };
        })
        .filter((g): g is { name: string; committed: number[]; lag: number } => g !== null);
      // client-side msgs/sec estimate from high-water mark growth
      const hwmTotal = partitions.reduce((a, p) => a + p.high_watermark, 0);
      const prev = prevRates.get(`${t.name}::hwm`) ?? [hwmTotal, now];
      const dt = (now - prev[1]) / 1000;
      const rate = dt > 0 ? Math.max(0, (hwmTotal - prev[0]) / dt) : 0;
      prevRates.set(`${t.name}::hwm`, [hwmTotal, now]);
      const history = prevRates.get(t.name) ?? [];
      if (dt > 0 && prev[1] !== now) history.push(rate);
      if (history.length > 90) history.shift();
      prevRates.set(t.name, history);
      return { name: t.name, partitions, consumer_groups, rate: [...history] };
    })
    .filter((t): t is TopicInfo => t !== null);
}

// ─── Live source ─────────────────────────────────────────────────────────────

const SERVICE_DEFS: Array<[string, string]> = [
  ["gateway", "API gateway"],
  ["auth", "Auth service"],
  ["users", "User service"],
  ["jobs", "Job service"],
  ["broker", "Message broker"],
  ["worker", "Worker pool"],
  ["websocket", "WebSocket service"],
];

export class LiveSource implements Source {
  readonly kind = "live" as const;
  private socket: LiveSocket | null = null;
  private timers: Array<ReturnType<typeof setInterval>> = [];
  private handlers: SourceHandlers | null = null;

  private jobsCache: Job[] = [];
  private workersCache: WorkerInfo[] = [];
  private topicsCache: TopicInfo[] = [];
  private topicRates = new Map<string, number[]>();
  private wsStats: { connections: number; rooms: number; users: number } | null = null;
  private series: SeriesPoint[] = [];

  // Service health comes from GET /api/health/services (public, gateway-side
  // probes). Null until the first successful poll; stale cache expires after
  // 30s and the grid falls back to neutral "unknown" tiles.
  private servicesCache: ServiceHealth[] | null = null;
  private servicesLastOkAt = 0;
  private servicesError: string | null = null;

  // Overview metrics come from Prometheus HTTP API queries. promTotals holds
  // the last good instant values; chartSeries the last good 5m range query.
  private promTotals: {
    rps: number | null;
    err: number | null;
    p99: number | null;
    active: number | null;
    queue: number | null;
    ws: number | null;
  } | null = null;
  private promLastOkAt = 0;
  private chartSeries: Array<{ t: number; rps: number }> = [];

  constructor(private getToken: () => string | null) {}

  start(h: SourceHandlers): void {
    this.handlers = h;
    h.onConn("connecting");
    this.socket = new LiveSocket(config.wsUrl, this.getToken, h.onEvent, h.onConn);
    this.socket.start();

    void this.pollJobs();
    void this.pollStats();
    void this.pollTopics();
    void this.pollMetrics();
    void this.pollHealth();
    this.timers.push(setInterval(() => void this.pollJobs(), 3000));
    this.timers.push(setInterval(() => void this.pollStats(), 3000));
    this.timers.push(setInterval(() => void this.pollTopics(), 5000));
    this.timers.push(setInterval(() => void this.pollMetrics(), 2000));
    this.timers.push(setInterval(() => void this.pollHealth(), 5000));
    this.timers.push(setInterval(() => this.pushSnapshot(), 2000));
  }

  stop(): void {
    this.socket?.stop();
    this.timers.forEach(clearInterval);
    this.timers = [];
  }

  private async pollJobs(): Promise<void> {
    try {
      const data = await getJson<unknown>(`${config.gatewayUrl}/api/jobs`, this.getToken());
      this.jobsCache = normalizeJobList(data);
      // opportunistic: a workers registry may exist
      try {
        const w = await getJson<unknown>(`${config.gatewayUrl}/api/workers`, this.getToken());
        const arr = Array.isArray(w) ? w : (asRecord(w)?.workers as unknown[] | undefined);
        if (Array.isArray(arr)) this.workersCache = arr.map((x) => this.normalizeWorker(x)).filter((x): x is WorkerInfo => x !== null);
      } catch {
        // not exposed — workers page falls back to event-derived data
      }
    } catch {
      // gateway unreachable this tick — caches keep last good values
    }
  }

  private async pollStats(): Promise<void> {
    try {
      const s = await getJson<{ connections?: number; rooms?: number; users?: number }>(
        `${config.realtimeUrl}/debug/stats`,
      );
      this.wsStats = { connections: s.connections ?? 0, rooms: s.rooms ?? 0, users: s.users ?? 0 };
    } catch {
      this.wsStats = null;
    }
  }

  private async pollTopics(): Promise<void> {
    try {
      const data = await getJson<unknown>(`${config.brokerUrl}/topics`);
      this.topicsCache = normalizeTopics(data, this.topicRates);
    } catch {
      // broker stats unreachable this tick
    }
  }

  /** Service health: public gateway endpoint, no auth needed. */
  private async pollHealth(): Promise<void> {
    try {
      const data = await getJson<unknown>(`${config.gatewayUrl}/api/health/services`);
      const parsed = this.normalizeHealthServices(data);
      if (parsed) {
        this.servicesCache = parsed;
        this.servicesLastOkAt = Date.now();
        this.servicesError = null;
      } else {
        this.servicesError = "unexpected /api/health/services response shape";
      }
    } catch (err) {
      this.servicesError = err instanceof Error ? err.message : "health endpoint unreachable";
    }
  }

  /** Overview metrics: instant PromQL for the cards, range PromQL for the chart. */
  private async pollMetrics(): Promise<void> {
    try {
      const [rps, errNum, errDen, p99s, active, queue, ws] = await Promise.all([
        promInstant("sum(rate(raven_gateway_http_requests_total[1m]))"),
        promInstant('sum(rate(raven_gateway_http_requests_total{status=~"5.."}[5m]))'),
        promInstant("sum(rate(raven_gateway_http_requests_total[5m]))"),
        promInstant("histogram_quantile(0.99, sum(rate(raven_gateway_http_request_duration_seconds_bucket[5m])) by (le))"),
        promInstant("sum(raven_jobs_processing)"),
        promInstant("sum(raven_broker_messages_pending)"),
        promInstant("sum(raven_websocket_connections)"),
      ]);
      const now = Date.now();
      // Division-by-zero guard: no traffic in the window → 0% errors.
      const err = errDen !== null && errDen > 0 ? (errNum ?? 0) / errDen : 0;
      this.promTotals = { rps, err, p99: p99s === null ? null : p99s * 1000, active, queue, ws };
      this.promLastOkAt = now;
      if (rps !== null) {
        this.series.push({ t: now, rps, err, p99: p99s === null ? 0 : p99s * 1000 });
        if (this.series.length > 300) this.series.shift();
      }
      try {
        const points = await promRange("sum(rate(raven_gateway_http_requests_total[1m]))", now - 5 * 60_000, now, 5);
        if (points.length > 0) this.chartSeries = points.map((p) => ({ t: p.t, rps: p.v }));
      } catch {
        // range query failed — keep last good chart series
      }
    } catch {
      // Prometheus unreachable — cards keep last good values with a stale
      // note; before the first success they show the designed empty state.
    }
  }

  /** Maps /api/health/services names onto the 7 grid cards. */
  private normalizeHealthServices(data: unknown): ServiceHealth[] | null {
    const r = asRecord(data);
    const arr = r?.services;
    if (!Array.isArray(arr)) return null;
    // endpoint name → card id (worker_pool renders as the "Worker pool" card)
    const nameToId: Record<string, string> = {
      gateway: "gateway",
      auth: "auth",
      users: "users",
      jobs: "jobs",
      broker: "broker",
      websocket: "websocket",
      worker_pool: "worker",
    };
    const byId = new Map<string, ServiceHealth>();
    for (const raw of arr) {
      const s = asRecord(raw);
      if (!s || typeof s.name !== "string") continue;
      const id = nameToId[s.name];
      if (!id) continue;
      const st = String(s.status ?? "");
      const status: Health = st === "ok" || st === "degraded" || st === "down" ? st : "unknown";
      const lat = Number(s.latency_ms);
      byId.set(id, {
        id,
        name: SERVICE_DEFS.find(([d]) => d === id)?.[1] ?? s.name,
        status,
        latency_ms: Number.isFinite(lat) ? lat : null,
        detail: typeof s.detail === "string" && s.detail ? s.detail : "reported by gateway",
      });
    }
    return SERVICE_DEFS.map(
      ([id, name]) =>
        byId.get(id) ?? { id, name, status: "unknown" as Health, latency_ms: null, detail: "not reported" },
    );
  }

  private services(): ServiceHealth[] {
    const fresh = this.servicesCache !== null && Date.now() - this.servicesLastOkAt < 30_000;
    if (fresh && this.servicesCache) return this.servicesCache;
    // Endpoint failing or never answered: neutral unknown, never red "down".
    const note = this.servicesError ?? "waiting for first probe";
    return SERVICE_DEFS.map(([id, name]) => ({
      id,
      name,
      status: "unknown" as Health,
      latency_ms: null,
      detail: note,
    }));
  }

  private pushSnapshot(): void {
    if (!this.handlers) return;
    const active = this.jobsCache.filter((j) => j.status === "PROCESSING").length;
    const queued = this.jobsCache.filter((j) => j.status === "QUEUED" || j.status === "RETRYING").length;
    const prom = this.promTotals;
    const promStale = this.promLastOkAt > 0 && Date.now() - this.promLastOkAt >= 15_000;
    this.handlers.onSnapshot({
      at: Date.now(),
      totals: {
        // Prometheus values; null before the first success → "—", never garbage.
        rps: prom?.rps ?? null,
        err_rate: prom?.err ?? null,
        p99_ms: prom?.p99 ?? null,
        active_jobs: prom?.active ?? active,
        queue_depth: prom?.queue ?? queued,
        ws_connections: prom?.ws ?? this.wsStats?.connections ?? null,
        ws_rooms: this.wsStats?.rooms ?? null,
        ws_users: this.wsStats?.users ?? null,
        metrics_stale: promStale,
      },
      services: this.services(),
      workers: this.workersCache,
      topics: this.topicsCache,
      series: [...this.series],
      chart: [...this.chartSeries],
    });
  }

  private normalizeWorker(raw: unknown): WorkerInfo | null {
    const r = asRecord(raw);
    if (!r) return null;
    const id = pick(r, "id", "ID");
    if (typeof id !== "string") return null;
    const active = pick(r, "active_jobs", "ActiveJobs");
    return {
      id,
      last_heartbeat: String(pick(r, "last_heartbeat", "LastHeartbeat") ?? new Date().toISOString()),
      started_at: String(pick(r, "started_at", "StartedAt") ?? new Date().toISOString()),
      jobs_processed: Number(pick(r, "jobs_processed", "JobsProcessed") ?? 0),
      jobs_failed: Number(pick(r, "jobs_failed", "JobsFailed") ?? 0),
      concurrency: Number(pick(r, "concurrency", "Concurrency") ?? 0),
      active_jobs: Array.isArray(active) ? active.map(String) : [],
    };
  }

  async queryJobs(q: JobsQuery): Promise<JobsPage> {
    if (q.page === 1) void this.pollJobs(); // refresh on fresh queries
    return filterJobs(this.jobsCache, q);
  }

  async getJob(id: string): Promise<Job | null> {
    try {
      const data = await getJson<unknown>(`${config.gatewayUrl}/api/jobs/${encodeURIComponent(id)}`, this.getToken());
      const job = normalizeJob(data);
      if (job) return job;
    } catch {
      // fall through to cache
    }
    return this.jobsCache.find((j) => j.id === id) ?? null;
  }

  async createJob(input: CreateJobInput): Promise<Job> {
    const res = await postJson<{ id?: string; status?: string }>(
      `${config.gatewayUrl}/api/jobs`,
      { type: input.type, payload: input.payload, priority: input.priority },
      { token: this.getToken(), idempotencyKey: input.idempotency_key },
    );
    await this.pollJobs();
    const existing = res.id ? this.jobsCache.find((j) => j.id === res.id) : undefined;
    return (
      existing ?? {
        id: res.id ?? "unknown",
        type: input.type,
        payload: input.payload,
        status: ((res.status ?? "QUEUED").toUpperCase() as JobStatus),
        priority: input.priority,
        attempts: 0,
        max_attempts: 3,
        created_at: new Date().toISOString(),
        started_at: null,
        finished_at: null,
        error: null,
        worker_id: null,
        idempotency_key: input.idempotency_key,
      }
    );
  }

  async cancelJob(id: string): Promise<void> {
    await postJson(`${config.gatewayUrl}/api/jobs/${encodeURIComponent(id)}/cancel`, {}, { token: this.getToken() });
    await this.pollJobs();
  }

  async requeueJob(id: string): Promise<void> {
    await postJson(`${config.gatewayUrl}/api/jobs/${encodeURIComponent(id)}/requeue`, {}, { token: this.getToken() });
    await this.pollJobs();
  }

  async metricsSummary(): Promise<MetricSample[]> {
    const res = await fetch(`${config.gatewayUrl}/metrics`, { signal: AbortSignal.timeout(3000) });
    if (!res.ok) throw new Error(`GET /metrics returned ${res.status}`);
    const samples = pickInteresting(parsePrometheus(await res.text()));
    if (samples.length === 0) throw new Error("No recognizable series in /metrics output");
    return samples;
  }
}

// ─── Demo source ─────────────────────────────────────────────────────────────

export class DemoSource implements Source {
  readonly kind = "demo" as const;
  private world = new DemoWorld();

  start(h: SourceHandlers): void {
    h.onConn("simulated");
    this.world.onEvent(h.onEvent);
    this.world.onSnapshot(h.onSnapshot);
    // push one snapshot immediately so first paint already has data
    h.onSnapshot(this.world.currentSnapshot());
    this.world.start();
  }

  stop(): void {
    this.world.stop();
  }

  async queryJobs(q: JobsQuery): Promise<JobsPage> {
    return this.world.queryJobs(q);
  }

  async getJob(id: string): Promise<Job | null> {
    return this.world.getJob(id);
  }

  async createJob(input: CreateJobInput): Promise<Job> {
    // simulate a touch of network latency
    await new Promise((r) => setTimeout(r, 250));
    return this.world.createJob(input);
  }

  async cancelJob(id: string): Promise<void> {
    await new Promise((r) => setTimeout(r, 200));
    this.world.cancelJob(id);
  }

  async requeueJob(id: string): Promise<void> {
    await new Promise((r) => setTimeout(r, 200));
    this.world.requeueJob(id);
  }

  async metricsSummary(): Promise<MetricSample[]> {
    return this.world.metricsSummary();
  }
}

export { probeGateway };
