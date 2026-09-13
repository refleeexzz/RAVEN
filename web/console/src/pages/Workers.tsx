import { useMemo } from "react";
import { Cpu, HeartPulse, TrendingUp, Users } from "lucide-react";
import { useConsole } from "@/lib/store";
import { useNow } from "@/lib/useNow";
import { fmtPct, fmtNum, relTime } from "@/lib/utils";
import { Card } from "@/components/ui/card";
import { Badge, Dot } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/empty";
import { StatCard } from "@/components/StatCard";
import type { WorkerInfo, WorkerStatus } from "@/lib/types";

const STALE_AFTER_MS = 5_000;
const DOWN_AFTER_MS = 15_000;

function workerStatus(w: WorkerInfo, now: number): WorkerStatus {
  const age = now - new Date(w.last_heartbeat).getTime();
  if (age < STALE_AFTER_MS) return "active";
  if (age < DOWN_AFTER_MS) return "stale";
  return "down";
}

const STATUS_BADGE: Record<WorkerStatus, { label: string; variant: "success" | "warning" | "error"; dot: "success" | "warning" | "error" }> = {
  active: { label: "Active", variant: "success", dot: "success" },
  stale: { label: "Stale", variant: "warning", dot: "warning" },
  down: { label: "Down", variant: "error", dot: "error" },
};

function WorkerCard({ worker, now }: { worker: WorkerInfo; now: number }) {
  const status = workerStatus(worker, now);
  const meta = STATUS_BADGE[status];
  const successRate =
    worker.jobs_processed > 0
      ? (worker.jobs_processed - worker.jobs_failed) / worker.jobs_processed
      : null;
  const used = worker.active_jobs.length;

  return (
    <Card className="p-4">
      <div className="flex items-center justify-between gap-2">
        <span className="truncate font-mono text-sm font-medium text-fg">{worker.id}</span>
        <Badge variant={meta.variant}>
          <Dot variant={meta.dot} pulse={status === "active"} />
          {meta.label}
        </Badge>
      </div>

      <dl className="mt-3 grid grid-cols-2 gap-x-4 gap-y-2">
        <div>
          <dt className="text-xs text-muted">Jobs processed</dt>
          <dd className="text-sm font-medium tabular-nums text-fg">{fmtNum(worker.jobs_processed)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted">Success rate</dt>
          <dd className="text-sm font-medium tabular-nums text-fg">{successRate === null ? "—" : fmtPct(successRate, 1)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted">Heartbeat</dt>
          <dd className="text-sm tabular-nums text-fg">{relTime(worker.last_heartbeat, now)}</dd>
        </div>
        <div>
          <dt className="text-xs text-muted">Uptime</dt>
          <dd className="text-sm tabular-nums text-fg">up {relTime(worker.started_at, now).replace(" ago", "")}</dd>
        </div>
      </dl>

      <div className="mt-3">
        <div className="mb-1 flex items-center justify-between text-xs text-muted">
          <span>Concurrency</span>
          {worker.concurrency > 0 ? (
            <span className="tabular-nums">
              {used} / {worker.concurrency} slots
            </span>
          ) : (
            <span>slots not reported</span>
          )}
        </div>
        {worker.concurrency > 0 && (
          <Progress
            value={used}
            max={worker.concurrency}
            tone={used >= worker.concurrency ? "warning" : "accent"}
            label={`${worker.id} concurrency`}
          />
        )}
      </div>

      {worker.active_jobs.length > 0 && (
        <div className="mt-3 flex flex-wrap gap-1">
          {worker.active_jobs.slice(0, 3).map((id) => (
            <span key={id} className="rounded-sm bg-surface-2 px-1.5 font-mono text-xs text-muted">
              {id.slice(0, 12)}…
            </span>
          ))}
          {worker.active_jobs.length > 3 && (
            <span className="rounded-sm bg-surface-2 px-1.5 text-xs text-subtle">
              +{worker.active_jobs.length - 3} more
            </span>
          )}
        </div>
      )}
    </Card>
  );
}

export function WorkersPage() {
  const snapshot = useConsole((s) => s.snapshot);
  const events = useConsole((s) => s.events);
  const now = useNow(1000);

  // Live fallback: when the gateway does not expose a worker registry, derive
  // a minimal view from recent job_status events.
  const derived = useMemo<WorkerInfo[]>(() => {
    const byWorker = new Map<string, WorkerInfo>();
    for (const e of events) {
      if (!e.worker_id) continue;
      const w = byWorker.get(e.worker_id) ?? {
        id: e.worker_id,
        last_heartbeat: e.at,
        started_at: e.at,
        jobs_processed: 0,
        jobs_failed: 0,
        concurrency: 0,
        active_jobs: [],
      };
      if (e.at > w.last_heartbeat) w.last_heartbeat = e.at;
      if (e.at < w.started_at) w.started_at = e.at;
      if (e.status === "SUCCESS") w.jobs_processed += 1;
      if (e.status === "FAILED" || e.status === "DEAD") w.jobs_failed += 1;
      if (e.status === "PROCESSING" && !w.active_jobs.includes(e.job_id)) w.active_jobs.push(e.job_id);
      if ((e.status === "SUCCESS" || e.status === "FAILED" || e.status === "DEAD") && w.active_jobs.includes(e.job_id)) {
        w.active_jobs = w.active_jobs.filter((j) => j !== e.job_id);
      }
      byWorker.set(e.worker_id, w);
    }
    return [...byWorker.values()];
  }, [events]);

  if (!snapshot) {
    return (
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 6 }, (_, i) => (
          <Skeleton key={i} className="h-48" />
        ))}
      </div>
    );
  }

  const workers = snapshot.workers.length > 0 ? snapshot.workers : derived;
  const fromEvents = snapshot.workers.length === 0 && workers.length > 0;
  const activeCount = workers.filter((w) => workerStatus(w, now) === "active").length;
  const staleCount = workers.filter((w) => workerStatus(w, now) !== "active").length;
  const totalProcessed = workers.reduce((a, w) => a + w.jobs_processed, 0);
  const totalFailed = workers.reduce((a, w) => a + w.jobs_failed, 0);
  const avgRate = totalProcessed > 0 ? (totalProcessed - totalFailed) / totalProcessed : null;

  return (
    <div className="flex flex-col gap-6">
      <div className="grid grid-cols-2 gap-4 xl:grid-cols-4">
        <StatCard label="Workers" value={workers.length} icon={Users} sub="registered in pool" />
        <StatCard label="Active" value={activeCount} icon={HeartPulse} sub="heartbeat < 5s" />
        <StatCard label="Stale / down" value={staleCount} icon={Cpu} sub="heartbeat > 5s" />
        <StatCard
          label="Avg success rate"
          value={avgRate === null ? "—" : fmtPct(avgRate, 1)}
          icon={TrendingUp}
          sub={fromEvents ? "from recent events" : "across pool"}
        />
      </div>

      {workers.length === 0 ? (
        <Card>
          <EmptyState
            icon={Cpu}
            title="No workers connected"
            description="Workers register on their first heartbeat. When the gateway is unreachable, start it or switch to demo mode to see a simulated pool."
          />
        </Card>
      ) : (
        <>
          {fromEvents && (
            <p className="text-xs text-muted">
              The gateway does not expose a worker registry — this view is derived from recent job events.
            </p>
          )}
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
            {workers.map((w) => (
              <WorkerCard key={w.id} worker={w} now={now} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}
