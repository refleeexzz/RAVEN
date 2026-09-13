import type {
  CreateJobInput,
  Job,
  JobEvent,
  JobsPage,
  JobsQuery,
  JobStatus,
  JobType,
  MetricSample,
  SeriesPoint,
  ServiceHealth,
  Snapshot,
  TopicInfo,
  WorkerInfo,
} from "./types";
import { JOB_TYPE_META } from "./types";
import { clamp } from "./utils";

// ─── Demo simulator ──────────────────────────────────────────────────────────
// A small world model that produces the same shapes the real gateway does:
// jobs moving through QUEUED → PROCESSING → SUCCESS/FAILED → RETRYING → DEAD,
// broker high-water marks and consumer lag, worker heartbeats, req/sec series.
// Deterministic (seeded RNG) so a fresh load always looks sane.

function mulberry32(seed: number): () => number {
  let a = seed;
  return () => {
    a += 0x6d2b79f5;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const HEX = "0123456789abcdef";

function randId(rng: () => number, alphabet: string, n: number): string {
  let out = "";
  for (let i = 0; i < n; i++) out += alphabet[Math.floor(rng() * alphabet.length)];
  return out;
}

const EMAILS = [
  "marina.costa@acme.io",
  "pedro.alves@conta.dev",
  "julia.mendes@loja.com.br",
  "rafael.torres@acme.io",
  "camila.rocha@fintech.app",
  "lucas.ferreira@conta.dev",
  "beatriz.lima@loja.com.br",
  "thiago.santos@acme.io",
];

const WEBHOOK_HOSTS = [
  "https://hooks.acme.io/raven/events",
  "https://api.conta.dev/webhooks/raven",
  "https://integrations.loja.com.br/listeners/jobs",
  "https://hooks.fintech.app/events",
];

const IMAGE_URLS = [
  "https://cdn.acme.io/uploads/hero_4k.png",
  "https://cdn.acme.io/uploads/banner_blackfriday.jpg",
  "https://cdn.conta.dev/assets/avatar_raw.tiff",
  "https://cdn.loja.com.br/catalog/sku-8841.png",
];

const ERRORS = [
  "smtp relay: 421 timeout after 3000ms",
  "imagemagick: unsupported color profile 'cmyk-8'",
  "webhook endpoint returned 503",
  "connection reset by peer (broker partition 1)",
  "context deadline exceeded after 5s",
  "dns: lookup failed for callback host",
];

interface TopicState extends TopicInfo {
  baseRate: number;
}

interface ServiceState extends ServiceHealth {
  baseLatency: number;
  incidentTicks: number;
}

export class DemoWorld {
  private rng = mulberry32(20240611);
  private tickCount = 0;
  private timer: ReturnType<typeof setInterval> | null = null;

  jobs: Job[] = [];
  workers: WorkerInfo[] = [];
  private busy = new Map<string, Map<string, number>>(); // workerId -> jobId -> ticks left
  private retryAt = new Map<string, number>(); // jobId -> tick when it goes back to QUEUED
  topics: TopicState[] = [];
  services: ServiceState[] = [];
  series: SeriesPoint[] = [];

  private requestsTotal = 0;
  private requests5xx = 0;
  private wsBase = 58;

  private eventListeners = new Set<(e: JobEvent) => void>();
  private snapshotListeners = new Set<(s: Snapshot) => void>();

  constructor() {
    this.seedWorkers();
    this.seedTopics();
    this.seedServices();
    // Pre-run ~2 minutes silently, backdated, so the first paint already has
    // a realistic history (spread timestamps on the chart, older jobs, etc).
    const t0 = Date.now();
    for (let i = 0; i < 120; i++) this.tick(true, t0 - (120 - i) * 1000);
  }

  // ── lifecycle ────────────────────────────────────────────────────────────

  start(): void {
    if (this.timer !== null) return;
    this.timer = setInterval(() => this.tick(false), 1000);
  }

  stop(): void {
    if (this.timer !== null) clearInterval(this.timer);
    this.timer = null;
  }

  onEvent(cb: (e: JobEvent) => void): void {
    this.eventListeners.add(cb);
  }

  onSnapshot(cb: (s: Snapshot) => void): void {
    this.snapshotListeners.add(cb);
  }

  // ── mutations (same contract as the REST API) ────────────────────────────

  createJob(input: CreateJobInput): Job {
    const job: Job = {
      id: `job_01J${randId(this.rng, CROCKFORD, 10)}`,
      type: input.type,
      payload: input.payload,
      status: "QUEUED",
      priority: input.priority,
      attempts: 0,
      max_attempts: 3,
      created_at: new Date().toISOString(),
      started_at: null,
      finished_at: null,
      error: null,
      worker_id: null,
      idempotency_key: input.idempotency_key,
    };
    this.jobs.unshift(job);
    this.emit(job, "QUEUED");
    return job;
  }

  cancelJob(id: string): void {
    const job = this.jobs.find((j) => j.id === id);
    if (!job) throw new Error(`Job ${id} not found`);
    if (job.status !== "QUEUED" && job.status !== "RETRYING") {
      throw new Error(`Only queued jobs can be cancelled (job is ${job.status.toLowerCase()})`);
    }
    this.retryAt.delete(id);
    job.status = "CANCELLED";
    job.finished_at = new Date().toISOString();
    this.emit(job, "CANCELLED");
  }

  requeueJob(id: string): void {
    const job = this.jobs.find((j) => j.id === id);
    if (!job) throw new Error(`Job ${id} not found`);
    if (job.status !== "DEAD") throw new Error("Only dead-letter jobs can be requeued");
    this.poison.delete(id); // operator requeue implies the underlying issue is fixed
    job.status = "QUEUED";
    job.error = null;
    job.finished_at = null;
    job.started_at = null;
    job.worker_id = null;
    this.emit(job, "QUEUED");
  }

  queryJobs(q: JobsQuery): JobsPage {
    let items = this.jobs.filter((j) => (q.dlq ? j.status === "DEAD" : j.status !== "DEAD"));
    if (q.statuses.length > 0) items = items.filter((j) => q.statuses.includes(j.status));
    if (q.type !== "all") items = items.filter((j) => j.type === q.type);
    if (q.search.trim()) {
      const needle = q.search.trim().toLowerCase();
      items = items.filter(
        (j) => j.id.toLowerCase().includes(needle) || (j.worker_id ?? "").includes(needle),
      );
    }
    items = [...items].sort((a, b) => b.created_at.localeCompare(a.created_at));
    const total = items.length;
    const start = (q.page - 1) * q.pageSize;
    return { items: items.slice(start, start + q.pageSize), total, page: q.page, pageSize: q.pageSize };
  }

  getJob(id: string): Job | null {
    return this.jobs.find((j) => j.id === id) ?? null;
  }

  metricsSummary(): MetricSample[] {
    const s = this.series[this.series.length - 1];
    const lag = this.topics.reduce(
      (acc, t) => acc + t.consumer_groups.reduce((a, g) => a + g.lag, 0),
      0,
    );
    return [
      { name: "raven_http_requests_total", labels: { service: "gateway", code: "200" }, value: Math.round(this.requestsTotal) },
      { name: "raven_http_requests_total", labels: { service: "gateway", code: "5xx" }, value: Math.round(this.requests5xx) },
      { name: "raven_http_request_duration_seconds", labels: { service: "gateway", quantile: "0.5" }, value: s.p99 * 0.32 / 1000 },
      { name: "raven_http_request_duration_seconds", labels: { service: "gateway", quantile: "0.95" }, value: s.p99 * 0.68 / 1000 },
      { name: "raven_http_request_duration_seconds", labels: { service: "gateway", quantile: "0.99" }, value: s.p99 / 1000 },
      { name: "raven_broker_consumer_lag", labels: { broker: "main" }, value: lag },
      { name: "raven_ws_connections", labels: { service: "websocket" }, value: this.wsConnections() },
      { name: "go_goroutines", labels: { service: "gateway" }, value: 160 + Math.floor(this.rng() * 60) },
    ];
  }

  /** Public snapshot of the current world state (same shape as live polls). */
  currentSnapshot(): Snapshot {
    return this.snapshot();
  }

  // ── internals ────────────────────────────────────────────────────────────

  private seedWorkers(): void {
    const conc = [8, 8, 4, 4, 8, 4];
    for (let i = 0; i < 6; i++) {
      const id = `worker-${randId(this.rng, HEX, 4)}`;
      this.workers.push({
        id,
        last_heartbeat: new Date().toISOString(),
        started_at: new Date(Date.now() - (2 + this.rng() * 40) * 3600_000).toISOString(),
        jobs_processed: Math.floor(this.rng() * 1800) + 200,
        jobs_failed: Math.floor(this.rng() * 40),
        concurrency: conc[i],
        active_jobs: [],
      });
      this.busy.set(id, new Map());
    }
  }

  private seedTopics(): void {
    const defs: Array<[string, number, string[], number]> = [
      ["jobs", 3, ["workers-main", "workers-priority"], 6],
      ["jobs.retry", 2, ["workers-main"], 1.5],
      ["jobs.dlq", 1, ["dlq-replayer"], 0.12],
      ["notifications", 2, ["notifier"], 3],
      ["events.audit", 1, ["audit-sink"], 5],
    ];
    for (const [name, parts, groups, baseRate] of defs) {
      const hwmStart = Math.floor(40_000 + this.rng() * 60_000);
      this.topics.push({
        name,
        partitions: Array.from({ length: parts }, (_, id) => ({
          id,
          high_watermark: hwmStart + Math.floor(this.rng() * 800),
        })),
        consumer_groups: groups.map((g) => ({
          name: g,
          committed: Array.from({ length: parts }, () => hwmStart - Math.floor(this.rng() * 400)),
          lag: 0,
        })),
        rate: [],
        baseRate,
      });
    }
  }

  private seedServices(): void {
    const defs: Array<[string, string, number]> = [
      ["gateway", "API gateway", 6],
      ["auth", "Auth service", 9],
      ["users", "User service", 7],
      ["jobs", "Job service", 11],
      ["broker", "Message broker", 4],
      ["worker", "Worker pool", 18],
      ["websocket", "WebSocket service", 5],
    ];
    this.services = defs.map(([id, name, baseLatency]) => ({
      id,
      name,
      status: "ok",
      latency_ms: baseLatency,
      detail: "simulated probe",
      baseLatency,
      incidentTicks: 0,
    }));
  }

  /** Jobs that always fail every attempt — they feed the DLQ demo data. */
  private poison = new Set<string>();

  private emit(job: Job, status: JobStatus): void {
    if (this.eventListeners.size === 0) return;
    const e: JobEvent = {
      type: "job_status",
      job_id: job.id,
      job_type: job.type,
      status,
      worker_id: job.worker_id,
      error: job.error,
      at: new Date().toISOString(),
    };
    this.eventListeners.forEach((cb) => cb(e));
  }

  private wsConnections(): number {
    return Math.max(8, Math.round(this.wsBase + Math.sin(this.tickCount / 20) * 18 + this.rng() * 6));
  }

  private spawnJob(silent: boolean, nowIso: string): void {
    const types = Object.keys(JOB_TYPE_META) as JobType[];
    const type = types[Math.floor(this.rng() * types.length)];
    let payload: Record<string, unknown>;
    if (type === "send_email") {
      payload = {
        to: EMAILS[Math.floor(this.rng() * EMAILS.length)],
        subject: ["Welcome to Acme", "Your receipt", "Password reset", "Weekly digest"][Math.floor(this.rng() * 4)],
        template: ["welcome_v1", "receipt_v2", "reset_v3", "digest_v1"][Math.floor(this.rng() * 4)],
      };
    } else if (type === "resize_image") {
      payload = {
        image_url: IMAGE_URLS[Math.floor(this.rng() * IMAGE_URLS.length)],
        width: [640, 1280, 1920][Math.floor(this.rng() * 3)],
        height: [480, 720, 1080][Math.floor(this.rng() * 3)],
        format: this.rng() > 0.4 ? "webp" : "jpeg",
      };
    } else {
      payload = {
        url: WEBHOOK_HOSTS[Math.floor(this.rng() * WEBHOOK_HOSTS.length)],
        method: "POST",
        event: ["user.created", "job.finished", "invoice.paid"][Math.floor(this.rng() * 3)],
      };
    }
    const job: Job = {
      id: `job_01J${randId(this.rng, CROCKFORD, 10)}`,
      type,
      payload,
      status: "QUEUED",
      priority: 1 + Math.floor(this.rng() * 10),
      attempts: 0,
      max_attempts: 3,
      created_at: nowIso,
      started_at: null,
      finished_at: null,
      error: null,
      worker_id: null,
      idempotency_key: null,
    };
    if (this.rng() < 0.025) this.poison.add(job.id); // always fails → lands in DLQ
    this.jobs.unshift(job);
    if (!silent) this.emit(job, "QUEUED");
  }

  private tick(silent: boolean, at?: number): void {
    this.tickCount++;
    const nowMs = at ?? Date.now();
    const nowIso = new Date(nowMs).toISOString();

    // 1. Incoming jobs (~1.4/s on average, occasional bursts)
    const roll = this.rng();
    if (roll < 0.72) this.spawnJob(silent, nowIso);
    if (roll < 0.08) {
      this.spawnJob(silent, nowIso);
      this.spawnJob(silent, nowIso);
      this.spawnJob(silent, nowIso);
    }

    // 2. Retrying jobs go back into the queue
    for (const job of this.jobs) {
      if (job.status === "RETRYING" && (this.retryAt.get(job.id) ?? 0) <= this.tickCount) {
        this.retryAt.delete(job.id);
        job.status = "QUEUED";
        job.worker_id = null;
        if (!silent) this.emit(job, "QUEUED");
      }
    }

    // 3. Assign queued jobs to free worker slots, highest priority first.
    // Rotate the starting worker each tick so load spreads across the pool.
    const queued = this.jobs
      .filter((j) => j.status === "QUEUED")
      .sort((a, b) => b.priority - a.priority || a.created_at.localeCompare(b.created_at));
    const startIdx = this.tickCount % this.workers.length;
    for (let k = 0; k < this.workers.length; k++) {
      const worker = this.workers[(startIdx + k) % this.workers.length];
      const slots = this.busy.get(worker.id)!;
      while (slots.size < worker.concurrency && queued.length > 0) {
        const job = queued.shift()!;
        job.status = "PROCESSING";
        job.attempts += 1;
        job.started_at = nowIso;
        job.worker_id = worker.id;
        worker.active_jobs = [...slots.keys(), job.id];
        slots.set(job.id, 2 + Math.floor(this.rng() * 7));
        if (!silent) this.emit(job, "PROCESSING");
      }
    }

    // 4. Progress running jobs
    for (const worker of this.workers) {
      const slots = this.busy.get(worker.id)!;
      for (const [jobId, left] of [...slots.entries()]) {
        if (left > 1) {
          slots.set(jobId, left - 1);
          continue;
        }
        slots.delete(jobId);
        worker.active_jobs = [...slots.keys()];
        const job = this.jobs.find((j) => j.id === jobId);
        if (!job) continue;
        worker.jobs_processed += 1;
        if (!this.poison.has(jobId) && this.rng() < 0.88) {
          job.status = "SUCCESS";
          job.finished_at = nowIso;
          if (!silent) this.emit(job, "SUCCESS");
        } else {
          worker.jobs_failed += 1;
          job.error = ERRORS[Math.floor(this.rng() * ERRORS.length)];
          if (job.attempts < job.max_attempts) {
            job.status = "RETRYING";
            this.retryAt.set(job.id, this.tickCount + 1 + Math.floor(this.rng() * 2));
            if (!silent) this.emit(job, "RETRYING");
          } else {
            job.status = "DEAD";
            job.finished_at = nowIso;
            if (!silent) this.emit(job, "DEAD");
          }
        }
      }
    }

    // 5. Topics: high-water marks grow, consumers trail behind
    for (const topic of this.topics) {
      const rate = Math.max(0, topic.baseRate + Math.sin(this.tickCount / 30 + topic.baseRate) * topic.baseRate * 0.4 + this.rng() * 1.2);
      topic.rate.push(rate);
      if (topic.rate.length > 90) topic.rate.shift();
      for (const p of topic.partitions) {
        p.high_watermark += Math.round(rate / topic.partitions.length + this.rng());
      }
      for (const g of topic.consumer_groups) {
        let lag = 0;
        g.committed = g.committed.map((c, i) => {
          const hwm = topic.partitions[i].high_watermark;
          // Consumers catch up but occasionally fall behind (visible lag spikes)
          const catchup = rate / topic.partitions.length + this.rng() * 2 - (this.rng() < 0.06 ? 30 : 0);
          const next = clamp(Math.round(c + catchup), 0, hwm);
          lag += hwm - next;
          return next;
        });
        g.lag = lag;
      }
    }

    // 6. Request series (5 min window at 1s resolution)
    const burst = this.tickCount % 97 < 6; // periodic load burst
    const rps = Math.max(4, 120 + Math.sin(this.tickCount / 45) * 42 + this.rng() * 14 + (burst ? 70 : 0));
    const err = clamp(0.004 + this.rng() * 0.004 + (burst ? 0.012 : 0), 0, 0.2);
    const p99 = 90 + rps * 0.42 + this.rng() * 24 + (burst ? 60 : 0);
    this.series.push({ t: nowMs, rps, err, p99 });
    if (this.series.length > 300) this.series.shift();
    this.requestsTotal += rps;
    this.requests5xx += rps * err;

    // 7. Service health jitter, rare incidents
    for (const svc of this.services) {
      if (svc.incidentTicks > 0) {
        svc.incidentTicks--;
        svc.status = svc.incidentTicks > 2 ? "down" : "degraded";
        svc.latency_ms = svc.status === "down" ? null : svc.baseLatency * (5 + this.rng() * 4);
        if (svc.incidentTicks === 0) {
          svc.status = "ok";
          svc.latency_ms = svc.baseLatency;
        }
      } else {
        svc.status = "ok";
        svc.latency_ms = Math.round((svc.baseLatency + this.rng() * svc.baseLatency * 0.6) * 10) / 10;
        // gateway and broker never go down in the demo; others very rarely
        if (svc.id !== "gateway" && svc.id !== "broker" && this.rng() < 0.0018) {
          svc.incidentTicks = 4 + Math.floor(this.rng() * 4);
        }
      }
    }

    // 8. Heartbeats — worker[3] is flaky on purpose
    for (let i = 0; i < this.workers.length; i++) {
      const w = this.workers[i];
      const flaky = i === 3 && this.tickCount % 41 > 28;
      if (!flaky) w.last_heartbeat = nowIso;
    }

    // 9. Trim old terminal jobs
    if (this.jobs.length > 260) {
      let toRemove = this.jobs.length - 260;
      for (let i = this.jobs.length - 1; i >= 0 && toRemove > 0; i--) {
        const s = this.jobs[i].status;
        if (s === "SUCCESS" || s === "CANCELLED") {
          this.jobs.splice(i, 1);
          toRemove--;
        }
      }
    }

    // 10. Publish snapshot
    if (!silent) {
      const snapshot = this.snapshot();
      this.snapshotListeners.forEach((cb) => cb(snapshot));
    }
  }

  private snapshot(): Snapshot {
    const s = this.series[this.series.length - 1];
    return {
      at: Date.now(),
      totals: {
        rps: s.rps,
        err_rate: s.err,
        p99_ms: s.p99,
        active_jobs: this.jobs.filter((j) => j.status === "PROCESSING").length,
        queue_depth: this.jobs.filter((j) => j.status === "QUEUED" || j.status === "RETRYING").length,
        ws_connections: this.wsConnections(),
        ws_rooms: 6,
        ws_users: Math.round(this.wsConnections() * 0.7),
      },
      services: this.services.map(({ baseLatency: _b, incidentTicks: _i, ...rest }) => ({ ...rest })),
      workers: this.workers.map((w) => ({ ...w, active_jobs: [...w.active_jobs] })),
      topics: this.topics.map((t) => ({
        ...t,
        partitions: t.partitions.map((p) => ({ ...p })),
        consumer_groups: t.consumer_groups.map((g) => ({ ...g, committed: [...g.committed] })),
        rate: [...t.rate],
      })),
      series: [...this.series],
    };
  }
}
