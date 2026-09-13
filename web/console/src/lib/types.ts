// Domain types shared by the live API, the websocket feed and the demo simulator.

export type JobStatus =
  | "QUEUED"
  | "PROCESSING"
  | "SUCCESS"
  | "FAILED"
  | "RETRYING"
  | "DEAD"
  | "CANCELLED";

export const JOB_STATUSES: JobStatus[] = [
  "QUEUED",
  "PROCESSING",
  "SUCCESS",
  "FAILED",
  "RETRYING",
  "DEAD",
  "CANCELLED",
];

export type JobType = "send_email" | "resize_image" | "webhook";

export const JOB_TYPE_META: Record<
  JobType,
  { label: string; description: string; template: Record<string, unknown> }
> = {
  send_email: {
    label: "Send email",
    description: "Transactional email through the notifications pipeline",
    template: {
      to: "marina.costa@acme.io",
      subject: "Your invoice for March",
      template: "invoice_v2",
    },
  },
  resize_image: {
    label: "Resize image",
    description: "Generate resized variants of an uploaded asset",
    template: {
      image_url: "https://cdn.acme.io/uploads/hero_4k.png",
      width: 1280,
      height: 720,
      format: "webp",
    },
  },
  webhook: {
    label: "Webhook",
    description: "POST an event payload to an external endpoint",
    template: {
      url: "https://hooks.acme.io/raven/events",
      method: "POST",
      event: "user.created",
    },
  },
};

export interface Job {
  id: string;
  type: JobType;
  payload: Record<string, unknown>;
  status: JobStatus;
  priority: number;
  attempts: number;
  max_attempts: number;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  error: string | null;
  worker_id: string | null;
  idempotency_key: string | null;
}

// Matches the websocket frame data: {"type":"job_status","job_id","status","worker_id"}
export interface JobEvent {
  type: "job_status";
  job_id: string;
  job_type: JobType | null;
  status: JobStatus;
  worker_id: string | null;
  error: string | null;
  at: string; // ISO timestamp
}

export interface CreateJobInput {
  type: JobType;
  payload: Record<string, unknown>;
  priority: number;
  idempotency_key: string;
}

export type Health = "ok" | "degraded" | "down" | "unknown";

export interface ServiceHealth {
  id: string;
  name: string;
  status: Health;
  latency_ms: number | null;
  detail: string;
}

export type WorkerStatus = "active" | "stale" | "down";

export interface WorkerInfo {
  id: string;
  last_heartbeat: string;
  started_at: string;
  jobs_processed: number;
  jobs_failed: number;
  /** Max concurrent jobs; 0 means the platform does not report it. */
  concurrency: number;
  active_jobs: string[];
}

export interface PartitionInfo {
  id: number;
  high_watermark: number;
}

export interface ConsumerGroupInfo {
  name: string;
  /** Committed offset per partition, aligned with partitions[].id */
  committed: number[];
  lag: number;
}

export interface TopicInfo {
  name: string;
  partitions: PartitionInfo[];
  consumer_groups: ConsumerGroupInfo[];
  /** Recent messages/sec samples, oldest first. Built client-side in live mode. */
  rate: number[];
}

export interface SeriesPoint {
  t: number; // epoch ms
  rps: number;
  err: number; // error ratio 0..1
  p99: number; // ms
}

export interface Totals {
  /** null when no metrics source has answered yet (never garbage numbers). */
  rps: number | null;
  err_rate: number | null; // 0..1
  p99_ms: number | null;
  active_jobs: number;
  queue_depth: number;
  ws_connections: number | null;
  ws_rooms: number | null;
  ws_users: number | null;
  /** Live mode: true while showing last-good values after Prometheus stopped answering. */
  metrics_stale?: boolean;
}

export interface Snapshot {
  at: number;
  totals: Totals;
  services: ServiceHealth[];
  workers: WorkerInfo[];
  topics: TopicInfo[];
  series: SeriesPoint[];
  /** Live mode only: throughput chart series straight from Prometheus query_range. */
  chart?: Array<{ t: number; rps: number }>;
}

export interface JobsQuery {
  statuses: JobStatus[];
  type: JobType | "all";
  search: string;
  page: number;
  pageSize: number;
  /** Dead-letter queue view (status DEAD only). */
  dlq: boolean;
}

export interface JobsPage {
  items: Job[];
  total: number;
  page: number;
  pageSize: number;
}

export type JobsPageState =
  | { status: "loading"; items: Job[]; total: number }
  | { status: "error"; items: Job[]; total: number; message: string }
  | { status: "ready"; items: Job[]; total: number };

export interface MetricSample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export type Mode = "connecting" | "live" | "demo";

export type ConnState =
  | "connecting"
  | "connected"
  | "reconnecting"
  | "simulated"
  | "offline";
