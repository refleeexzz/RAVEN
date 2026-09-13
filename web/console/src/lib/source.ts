import { getJson, postJson, probeGateway } from "./api";
import { config } from "./config";
import { LiveSocket } from "./live";
import { parsePrometheus, pickInteresting } from "./metrics";
import { DemoWorld } from "./simulator";
import type {
  ConnState,
  CreateJobInput,
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
import { clamp } from "./utils";

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
  private lastMetrics: { at: number; total: number; errors: number } | null = null;
  private probeMs = new Map<string, number | null>();

  constructor(private getToken: () => string | null) {}

  start(h: SourceHandlers): void {
    this.handlers = h;
    h.onConn("connecting");
    this.socket = new LiveSocket(config.wsUrl, this.getToken(), h.onEvent, h.onConn);
    this.socket.start();

    void this.pollJobs();
    void this.pollStats();
    void this.pollTopics();
    void this.pollMetrics();
    this.timers.push(setInterval(() => void this.pollJobs(), 3000));
    this.timers.push(setInterval(() => void this.pollStats(), 3000));
    this.timers.push(setInterval(() => void this.pollTopics(), 5000));
    this.timers.push(setInterval(() => void this.pollMetrics(), 2000));
    this.timers.push(setInterval(() => this.pushSnapshot(), 2000));
  }

  stop(): void {
    this.socket?.stop();
    this.timers.forEach(clearInterval);
    this.timers = [];
  }

  private async pollJobs(): Promise<void> {
    const t0 = performance.now();
    try {
      const data = await getJson<unknown>(`${config.gatewayUrl}/api/jobs`, this.getToken());
      this.jobsCache = normalizeJobList(data);
      this.probeMs.set("gateway", performance.now() - t0);
      // opportunistic: a workers registry may exist
      try {
        const w = await getJson<unknown>(`${config.gatewayUrl}/api/workers`, this.getToken());
        const arr = Array.isArray(w) ? w : (asRecord(w)?.workers as unknown[] | undefined);
        if (Array.isArray(arr)) this.workersCache = arr.map((x) => this.normalizeWorker(x)).filter((x): x is WorkerInfo => x !== null);
      } catch {
        // not exposed — workers page falls back to event-derived data
      }
    } catch {
      this.probeMs.set("gateway", null);
    }
  }

  private async pollStats(): Promise<void> {
    const t0 = performance.now();
    try {
      const s = await getJson<{ connections?: number; rooms?: number; users?: number }>(
        `${config.realtimeUrl}/debug/stats`,
      );
      this.wsStats = { connections: s.connections ?? 0, rooms: s.rooms ?? 0, users: s.users ?? 0 };
      this.probeMs.set("websocket", performance.now() - t0);
    } catch {
      this.wsStats = null;
      this.probeMs.set("websocket", null);
    }
  }

  private async pollTopics(): Promise<void> {
    const t0 = performance.now();
    try {
      const data = await getJson<unknown>(`${config.brokerUrl}/topics`);
      this.topicsCache = normalizeTopics(data, this.topicRates);
      this.probeMs.set("broker", performance.now() - t0);
    } catch {
      this.probeMs.set("broker", null);
    }
  }

  private async pollMetrics(): Promise<void> {
    try {
      const res = await fetch(`${config.gatewayUrl}/metrics`, { signal: AbortSignal.timeout(2500) });
      if (!res.ok) return;
      const samples = parsePrometheus(await res.text());
      const total = samples
        .filter((s) => /http_requests_total$/.test(s.name))
        .reduce((a, s) => a + s.value, 0);
      const errors = samples
        .filter((s) => /http_requests_total$/.test(s.name) && /^5/.test(s.labels.code ?? s.labels.status ?? ""))
        .reduce((a, s) => a + s.value, 0);
      const p99Sample = samples.find(
        (s) => /request_duration/.test(s.name) && (s.labels.quantile === "0.99" || s.labels.le === "+Inf"),
      );
      const now = Date.now();
      if (this.lastMetrics && total >= this.lastMetrics.total) {
        const dt = (now - this.lastMetrics.at) / 1000;
        const rps = dt > 0 ? (total - this.lastMetrics.total) / dt : 0;
        const err = total > this.lastMetrics.total ? clamp((errors - this.lastMetrics.errors) / Math.max(1, total - this.lastMetrics.total), 0, 1) : 0;
        this.series.push({
          t: now,
          rps,
          err,
          p99: p99Sample ? p99Sample.value * 1000 : 0,
        });
        if (this.series.length > 300) this.series.shift();
      }
      this.lastMetrics = { at: now, total, errors };
    } catch {
      // /metrics not exposed — the overview chart shows its empty state
    }
  }

  private services(): ServiceHealth[] {
    const gw = this.probeMs.get("gateway");
    const broker = this.probeMs.get("broker");
    const ws = this.probeMs.get("websocket");
    const processing = this.jobsCache.filter((j) => j.status === "PROCESSING").length;
    return SERVICE_DEFS.map(([id, name]) => {
      if (id === "gateway") {
        return { id, name, status: gw == null ? "down" : "ok", latency_ms: gw != null ? Math.round(gw * 10) / 10 : null, detail: "probe · GET /api/jobs" };
      }
      if (id === "broker") {
        return { id, name, status: broker == null ? "down" : "ok", latency_ms: broker != null ? Math.round(broker * 10) / 10 : null, detail: "probe · GET /topics" };
      }
      if (id === "websocket") {
        return { id, name, status: ws == null ? "down" : "ok", latency_ms: ws != null ? Math.round(ws * 10) / 10 : null, detail: "probe · GET /debug/stats" };
      }
      if (id === "worker") {
        const ok = gw != null;
        return { id, name, status: ok ? "ok" : "down", latency_ms: null, detail: processing > 0 ? `derived · ${processing} jobs processing` : "derived · idle" };
      }
      // auth / users / jobs sit behind the gateway
      return { id, name, status: gw == null ? "down" : "ok", latency_ms: null, detail: "via gateway" };
    });
  }

  private pushSnapshot(): void {
    if (!this.handlers) return;
    const active = this.jobsCache.filter((j) => j.status === "PROCESSING").length;
    const queued = this.jobsCache.filter((j) => j.status === "QUEUED" || j.status === "RETRYING").length;
    const last = this.series[this.series.length - 1];
    this.handlers.onSnapshot({
      at: Date.now(),
      totals: {
        rps: last?.rps ?? 0,
        err_rate: last?.err ?? 0,
        p99_ms: last ? last.p99 : null,
        active_jobs: active,
        queue_depth: queued,
        ws_connections: this.wsStats?.connections ?? null,
        ws_rooms: this.wsStats?.rooms ?? null,
        ws_users: this.wsStats?.users ?? null,
      },
      services: this.services(),
      workers: this.workersCache,
      topics: this.topicsCache,
      series: [...this.series],
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
