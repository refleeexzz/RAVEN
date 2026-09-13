import { useCallback, useEffect, useRef, useState } from "react";
import { Bug, LogIn, Play, RotateCcw } from "lucide-react";
import { useConsole } from "@/lib/store";
import type { Job, JobStatus } from "@/lib/types";
import { fmtTime, shortId, uuid } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { StatusChip } from "@/components/StatusChip";
import { errMsg, sleep, type LogFn } from "./lab";

// Scenario 3 — Fail on purpose: a webhook to a dead port. Every attempt
// fails, the worker retries with backoff, and the job lands in the DLQ.
// Requeueing runs the same cycle again — the point is watching DLQ mechanics.

const DOOMED_URL = "http://localhost:9/never-works";
const WATCH_TIMEOUT_MS = 180_000;

interface TimelineEntry {
  at: number;
  label: string;
  status?: JobStatus;
}

type Phase = "idle" | "watching" | "dead" | "requeued" | "deadagain";

export function FailOnPurpose({ log, autoRun = false }: { log: LogFn; autoRun?: boolean }) {
  const setSignInOpen = useConsole((s) => s.setSignInOpen);
  const live = useConsole((s) => s.mode === "live");
  const noToken = useConsole((s) => !s.token);
  const [phase, setPhase] = useState<Phase>("idle");
  const [jobId, setJobId] = useState<string | null>(null);
  const [timeline, setTimeline] = useState<TimelineEntry[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [requeueing, setRequeueing] = useState(false);
  const runRef = useRef(0);

  // Bumping the run id makes every in-flight async loop go stale.
  const cancelRuns = useCallback(() => {
    runRef.current += 1;
  }, []);

  useEffect(() => cancelRuns, [cancelRuns]);

  const push = useCallback((label: string, status?: JobStatus) => {
    setTimeline((prev) => [...prev, { at: Date.now(), label, status }]);
  }, []);

  /** Watches the job, appending timeline entries, until it dies `wantDeaths` times. */
  const watch = useCallback(
    async (
      id: string,
      runId: number,
      wantDeaths: number,
      initial?: { deaths: number; status: JobStatus | null; attempts: number },
    ) => {
      const stale = () => runRef.current !== runId;
      const t0 = Date.now();
      let deaths = initial?.deaths ?? 0;
      let prevStatus: JobStatus | null = initial?.status ?? null;
      let prevAttempts = initial?.attempts ?? 0;
      while (!stale() && Date.now() - t0 < WATCH_TIMEOUT_MS) {
        let job: Job | null = null;
        try {
          job = await useConsole.getState().getJob(id);
        } catch {
          job = null;
        }
        if (job) {
          if (job.status !== prevStatus) {
            if (job.status === "PROCESSING" && job.attempts !== prevAttempts) {
              push(`attempt ${job.attempts} started`, "PROCESSING");
            } else if (job.status === "RETRYING") {
              push(`attempt ${job.attempts} failed${job.error ? ` — ${job.error}` : ""} · backoff, then retry`, "RETRYING");
            } else if (job.status === "DEAD") {
              deaths++;
              push(deaths === 1 ? "job is DEAD — moved to the DLQ" : "died again, back in the DLQ", "DEAD");
            } else if (job.status === "QUEUED" && prevStatus !== null) {
              push("back in the queue", "QUEUED");
            }
            prevStatus = job.status;
            prevAttempts = job.attempts;
          } else if (job.status === "PROCESSING" && job.attempts !== prevAttempts) {
            push(`attempt ${job.attempts} started`, "PROCESSING");
            prevAttempts = job.attempts;
          }
          if (deaths >= wantDeaths) return deaths;
        }
        await sleep(1000);
      }
      return deaths;
    },
    [push],
  );

  const run = useCallback(async () => {
    const runId = ++runRef.current;
    const stale = () => runRef.current !== runId;

    setPhase("watching");
    setError(null);
    setTimeline([]);
    setRequeueing(false);

    await useConsole.getState().boot();
    let job: Job;
    try {
      if (stale()) return;
      job = await useConsole.getState().createJob({
        type: "webhook",
        payload: { url: DOOMED_URL, method: "POST", event: "testlab.doomed" },
        priority: 5,
        idempotency_key: uuid(),
      });
    } catch (err) {
      if (stale()) return;
      const msg = errMsg(err);
      setError(msg);
      setPhase("idle");
      log({ scenario: "Fail on purpose", outcome: "fail", detail: `create failed: ${msg}` });
      return;
    }
    if (stale()) return;
    setJobId(job.id);
    push(`job ${shortId(job.id)} created — POST ${DOOMED_URL}`, "QUEUED");

    const deaths = await watch(job.id, runId, 1);
    if (stale()) return;
    if (deaths >= 1) {
      setPhase("dead");
      log({ scenario: "Fail on purpose", outcome: "ok", detail: `job ${shortId(job.id)} exhausted retries → DEAD` });
    } else {
      setPhase("idle");
      push("watch timed out after 3 minutes");
      log({ scenario: "Fail on purpose", outcome: "warn", detail: "timed out before the job went DEAD" });
    }
  }, [log, push, watch]);

  const requeue = useCallback(async () => {
    if (!jobId) return;
    const runId = runRef.current; // same run — the watch continues
    const stale = () => runRef.current !== runId;
    setRequeueing(true);
    setError(null);
    try {
      await useConsole.getState().requeueJob(jobId);
      if (stale()) return;
      push("requeued from the DLQ", "QUEUED");
      setPhase("requeued");
      log({ scenario: "Fail on purpose", outcome: "ok", detail: `requeued ${shortId(jobId)} from DLQ` });
      // Resume watching; the job is DEAD right now, so carry that in to avoid
      // a duplicate "moved to the DLQ" entry on the first poll.
      const deaths = await watch(jobId, runId, 2, { deaths: 1, status: "DEAD", attempts: 0 });
      if (stale()) return;
      if (deaths >= 2) {
        setPhase("deadagain");
        log({ scenario: "Fail on purpose", outcome: "ok", detail: "requeued job died again (expected)" });
      } else {
        setPhase("dead");
      }
    } catch (err) {
      if (stale()) return;
      setError(errMsg(err));
    } finally {
      if (!stale()) setRequeueing(false);
    }
  }, [jobId, log, push, watch]);

  useEffect(() => {
    if (!autoRun) return;
    void run();
    return cancelRuns;
  }, [autoRun, run, cancelRuns]);

  return (
    <Card>
      <CardHeader
        title="Fail on purpose"
        description="A webhook to a dead port: it retries with backoff (100ms → 250ms → 500ms), then lands in the DLQ."
        action={<Bug className="h-4 w-4 text-subtle" aria-hidden />}
      />
      <CardContent className="flex flex-col gap-4">
        <p className="text-xs text-subtle">
          Payload <span className="font-mono">{`{"url":"${DOOMED_URL}"}`}</span> — nothing listens on
          port 9, so every attempt fails. After the retry budget, the job goes DEAD.
        </p>

        {error && live && noToken ? (
          <p className="flex flex-wrap items-center gap-2 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            {error} — the live gateway requires a session for this.
            <Button variant="outline" size="sm" onClick={() => setSignInOpen(true)}>
              <LogIn className="h-3.5 w-3.5" aria-hidden /> Sign in
            </Button>
          </p>
        ) : error ? (
          <p role="alert" className="rounded-md border border-error/40 bg-error/10 px-3 py-2 text-xs text-error">
            {error}
          </p>
        ) : null}

        {timeline.length > 0 && (
          <ol className="flex flex-col" aria-label="Attempt timeline">
            {timeline.map((t, i) => (
              <li
                key={i}
                className="flex items-center gap-3 border-b border-border/60 py-1.5 last:border-b-0"
              >
                <span className="shrink-0 font-mono text-xs tabular-nums text-subtle">{fmtTime(t.at)}</span>
                {t.status && <StatusChip status={t.status} />}
                <span className="min-w-0 flex-1 truncate font-mono text-xs text-muted">{t.label}</span>
              </li>
            ))}
          </ol>
        )}

        {phase === "dead" && (
          <div className="flex flex-wrap items-center gap-3 rounded-md border border-error/40 bg-error/10 px-3 py-2">
            <p className="flex-1 text-xs text-error">
              In the DLQ. Operators requeue after fixing the cause — here the cause is the URL, so it
              will die again.
            </p>
            <Button variant="danger" size="sm" onClick={() => void requeue()} loading={requeueing}>
              <RotateCcw className="h-3.5 w-3.5" aria-hidden /> Requeue from DLQ
            </Button>
          </div>
        )}

        {phase === "deadagain" && (
          <p className="rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            Died again — that is the point: a requeue retries the same payload. Fix the root cause
            first, then requeue.
          </p>
        )}

        <div>
          <Button
            variant={phase === "idle" ? "primary" : "secondary"}
            onClick={() => void run()}
            loading={phase === "watching" || phase === "requeued"}
          >
            <Play className="h-4 w-4" aria-hidden />
            {phase === "idle" ? "Create doomed webhook" : "Run again"}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
