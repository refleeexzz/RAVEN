import type { ConnState, JobEvent, JobStatus, JobType } from "./types";
import { JOB_STATUSES } from "./types";

// WebSocket client for the realtime feed (ws://localhost:8084/ws).
// Joins room "jobs" and dispatches job_status events. Reconnects with
// exponential backoff: 1s → 2s → 5s → 10s (capped).

const BACKOFF_MS = [1000, 2000, 5000, 10000];

interface WsFrame {
  op?: string;
  room?: string;
  data?: unknown;
}

function asString(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}

export function normalizeJobEvent(data: unknown): JobEvent | null {
  if (typeof data !== "object" || data === null) return null;
  const d = data as Record<string, unknown>;
  if (d.type !== "job_status") return null;
  const jobId = asString(d.job_id);
  const statusRaw = asString(d.status)?.toUpperCase() ?? "";
  const status = (JOB_STATUSES as string[]).includes(statusRaw) ? (statusRaw as JobStatus) : null;
  if (!jobId || !status) return null;
  const jobTypeRaw = asString(d.job_type);
  return {
    type: "job_status",
    job_id: jobId,
    job_type: (jobTypeRaw === "send_email" || jobTypeRaw === "resize_image" || jobTypeRaw === "webhook"
      ? jobTypeRaw
      : null) as JobType | null,
    status,
    worker_id: asString(d.worker_id),
    error: asString(d.error),
    at: asString(d.at) ?? new Date().toISOString(),
  };
}

export class LiveSocket {
  private ws: WebSocket | null = null;
  private attempt = 0;
  private stopped = false;
  private timer: ReturnType<typeof setTimeout> | null = null;

  constructor(
    private url: string,
    private token: string | null,
    private onEvent: (e: JobEvent) => void,
    private onConn: (c: ConnState) => void,
  ) {}

  start(): void {
    this.connect();
  }

  private connect(): void {
    if (this.stopped) return;
    this.onConn(this.attempt === 0 ? "connecting" : "reconnecting");
    const url = this.token ? `${this.url}?token=${encodeURIComponent(this.token)}` : this.url;
    let ws: WebSocket;
    try {
      ws = new WebSocket(url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.onopen = () => {
      this.attempt = 0;
      this.onConn("connected");
      ws.send(JSON.stringify({ op: "join", room: "jobs" }));
    };
    ws.onmessage = (ev: MessageEvent<string>) => {
      try {
        const frame = JSON.parse(ev.data) as WsFrame;
        if (frame.op === "event") {
          const e = normalizeJobEvent(frame.data);
          if (e) this.onEvent(e);
        }
      } catch {
        // malformed frame — ignore
      }
    };
    ws.onclose = () => {
      this.scheduleReconnect();
    };
    ws.onerror = () => {
      ws.close();
    };
  }

  private scheduleReconnect(): void {
    if (this.stopped) return;
    const delay = BACKOFF_MS[Math.min(this.attempt, BACKOFF_MS.length - 1)];
    this.attempt++;
    this.onConn("reconnecting");
    this.timer = setTimeout(() => this.connect(), delay);
  }

  stop(): void {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    this.ws?.close();
    this.ws = null;
  }
}
