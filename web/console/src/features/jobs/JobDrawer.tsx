import { useEffect, useMemo, useState } from "react";
import { Ban, Check, Copy, RotateCcw } from "lucide-react";
import { useConsole } from "@/lib/store";
import { toast } from "@/lib/toast";
import { fmtTime, relTime } from "@/lib/utils";
import { JOB_TYPE_META, type Job, type JobStatus } from "@/lib/types";
import { Drawer } from "@/components/ui/overlay";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/empty";
import { StatusChip } from "@/components/StatusChip";

function CopyButton({ value, label }: { value: string; label: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      type="button"
      aria-label={`Copy ${label}`}
      onClick={() => {
        void navigator.clipboard.writeText(value).then(() => {
          setCopied(true);
          setTimeout(() => setCopied(false), 1500);
        });
      }}
      className="rounded-md p-1 text-muted transition-colors duration-150 hover:bg-surface-2 hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
    >
      {copied ? <Check className="h-3.5 w-3.5 text-success" /> : <Copy className="h-3.5 w-3.5" />}
    </button>
  );
}

function MetaRow({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-4 py-1.5">
      <dt className="shrink-0 text-xs text-muted">{label}</dt>
      <dd className="min-w-0 text-right text-sm text-fg">{children}</dd>
    </div>
  );
}

interface TimelineEntry {
  at: string;
  status: JobStatus;
  worker: string | null;
  error: string | null;
}

export function JobDrawer({ jobId, onClose }: { jobId: string | null; onClose: () => void }) {
  const getJob = useConsole((s) => s.getJob);
  const cancelJob = useConsole((s) => s.cancelJob);
  const requeueJob = useConsole((s) => s.requeueJob);
  const events = useConsole((s) => s.events);
  const mode = useConsole((s) => s.mode);
  const [job, setJob] = useState<Job | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [acting, setActing] = useState(false);

  useEffect(() => {
    if (!jobId || mode === "connecting") return; // wait for the data source
    let cancelled = false;
    setJob(null);
    setError(null);
    const load = () => {
      getJob(jobId)
        .then((j) => {
          if (cancelled) return;
          if (j) {
            setJob(j);
            setError(null); // a later success clears any earlier failure
          } else {
            setError("Job not found — it may have been trimmed from the recent window.");
          }
        })
        .catch((e: unknown) => {
          if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load job");
        });
    };
    load();
    const id = setInterval(load, 2000); // keep the drawer live while open
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [jobId, getJob, mode]);

  const timeline = useMemo<TimelineEntry[]>(() => {
    const fromEvents: TimelineEntry[] = events
      .filter((e) => e.job_id === jobId)
      .map((e) => ({ at: e.at, status: e.status, worker: e.worker_id, error: e.error }));
    if (fromEvents.length > 0) {
      return [...fromEvents].sort((a, b) => b.at.localeCompare(a.at)).slice(0, 20);
    }
    if (!job) return [];
    // Fall back to timestamps on the job itself
    const rows: TimelineEntry[] = [];
    if (job.finished_at) rows.push({ at: job.finished_at, status: job.status, worker: job.worker_id, error: job.error });
    if (job.started_at) rows.push({ at: job.started_at, status: "PROCESSING", worker: job.worker_id, error: null });
    rows.push({ at: job.created_at, status: "QUEUED", worker: null, error: null });
    return rows;
  }, [events, jobId, job]);

  const act = async (fn: () => Promise<void>, okTitle: string) => {
    if (!job) return;
    setActing(true);
    try {
      await fn();
      toast({ variant: "success", title: okTitle, description: job.id });
      onClose();
    } catch (err) {
      toast({
        variant: "error",
        title: "Action failed",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    } finally {
      setActing(false);
    }
  };

  return (
    <Drawer
      open={jobId !== null}
      onClose={onClose}
      title={
        <div className="flex items-center gap-2">
          <span className="truncate font-mono text-sm font-semibold text-fg">{jobId}</span>
          {job && <StatusChip status={job.status} />}
        </div>
      }
    >
      {job ? (
        <div className="flex flex-col gap-6">
          {job.error && (
            <div className="rounded-md border border-error/40 bg-error/10 p-3">
              <p className="text-xs font-medium uppercase tracking-wide text-error">Last error</p>
              <p className="mt-1 break-words font-mono text-xs text-error">{job.error}</p>
            </div>
          )}

          <section>
            <h3 className="mb-1 text-xs font-medium uppercase tracking-wide text-muted">Details</h3>
            <dl className="divide-y divide-border/60">
              <MetaRow label="Type">{JOB_TYPE_META[job.type].label}</MetaRow>
              <MetaRow label="Priority">{job.priority}</MetaRow>
              <MetaRow label="Attempts">
                <span className="tabular-nums">
                  {job.attempts} / {job.max_attempts}
                </span>
              </MetaRow>
              <MetaRow label="Worker">
                <span className="font-mono text-xs">{job.worker_id ?? "—"}</span>
              </MetaRow>
              <MetaRow label="Created">
                <span title={job.created_at}>
                  {fmtTime(job.created_at)} · {relTime(job.created_at)}
                </span>
              </MetaRow>
              {job.started_at && <MetaRow label="Started">{fmtTime(job.started_at)}</MetaRow>}
              {job.finished_at && <MetaRow label="Finished">{fmtTime(job.finished_at)}</MetaRow>}
              {job.idempotency_key && (
                <MetaRow label="Idempotency key">
                  <span className="inline-flex items-center gap-1 font-mono text-xs">
                    <span className="max-w-44 truncate" title={job.idempotency_key}>
                      {job.idempotency_key}
                    </span>
                    <CopyButton value={job.idempotency_key} label="idempotency key" />
                  </span>
                </MetaRow>
              )}
              <MetaRow label="Job ID">
                <span className="inline-flex items-center gap-1 font-mono text-xs">
                  <span className="max-w-44 truncate" title={job.id}>
                    {job.id}
                  </span>
                  <CopyButton value={job.id} label="job id" />
                </span>
              </MetaRow>
            </dl>
          </section>

          <section>
            <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted">Payload</h3>
            <pre className="max-h-64 overflow-auto rounded-md border border-border bg-bg p-3 font-mono text-xs text-fg">
              {JSON.stringify(job.payload, null, 2)}
            </pre>
          </section>

          <section>
            <h3 className="mb-2 text-xs font-medium uppercase tracking-wide text-muted">Timeline</h3>
            {timeline.length === 0 ? (
              <p className="text-sm text-muted">No status transitions recorded yet.</p>
            ) : (
              <ol className="relative ml-1 flex flex-col gap-3 border-l border-border pl-4">
                {timeline.map((row, i) => (
                  <li key={`${row.at}-${row.status}-${i}`} className="relative">
                    <span className="absolute -left-[21px] top-1 h-2 w-2 rounded-full bg-accent" aria-hidden />
                    <div className="flex items-center gap-2">
                      <StatusChip status={row.status} />
                      <span className="font-mono text-xs tabular-nums text-subtle">{fmtTime(row.at)}</span>
                    </div>
                    {(row.worker || row.error) && (
                      <p className="mt-0.5 break-words font-mono text-xs text-muted">
                        {row.worker}
                        {row.error ? ` · ${row.error}` : ""}
                      </p>
                    )}
                  </li>
                ))}
              </ol>
            )}
          </section>

          {(job.status === "QUEUED" || job.status === "RETRYING" || job.status === "DEAD") && (
            <div className="flex justify-end gap-2 border-t border-border pt-4">
              {(job.status === "QUEUED" || job.status === "RETRYING") && (
                <Button
                  variant="danger"
                  size="sm"
                  loading={acting}
                  onClick={() => void act(() => cancelJob(job.id), "Job cancelled")}
                >
                  <Ban className="h-4 w-4" aria-hidden />
                  Cancel job
                </Button>
              )}
              {job.status === "DEAD" && (
                <Button
                  variant="secondary"
                  size="sm"
                  loading={acting}
                  onClick={() => void act(() => requeueJob(job.id), "Job requeued")}
                >
                  <RotateCcw className="h-4 w-4" aria-hidden />
                  Requeue
                </Button>
              )}
            </div>
          )}
        </div>
      ) : error ? (
        <ErrorState message={error} onRetry={onClose} />
      ) : (
        <div className="flex flex-col gap-4">
          <Skeleton className="h-24" />
          <Skeleton className="h-40" />
          <Skeleton className="h-32" />
        </div>
      )}
    </Drawer>
  );
}
