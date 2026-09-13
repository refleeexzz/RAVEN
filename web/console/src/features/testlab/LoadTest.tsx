import { useCallback, useEffect, useRef, useState } from "react";
import { Gauge, LogIn, Play, Square } from "lucide-react";
import { useConsole } from "@/lib/store";
import { JOB_STATUSES, JOB_TYPE_META, type JobStatus, type JobType } from "@/lib/types";
import { fmtNum, uuid } from "@/lib/utils";
import { useNow } from "@/lib/useNow";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Select } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";
import { StatusChip } from "@/components/StatusChip";
import { errMsg, isTerminal, pool, sleep, type LogFn } from "./lab";

// Scenario 2 — Mini load test: create N jobs through a small promise pool,
// then sample-poll their statuses until everything is terminal.

const COUNTS = [10, 50, 100, 500] as const;
const WATCH_TIMEOUT_MS = 240_000;
/** Live mode polls at most this many job ids per tick (rotating window). */
const LIVE_POLL_WINDOW = 60;

interface Stats {
  created: number;
  inFlight: number;
  resolved: number;
  success: number;
  failed: number;
}

const ZERO: Stats = { created: 0, inFlight: 0, resolved: 0, success: 0, failed: 0 };

export function LoadTest({ log, autoRun = false }: { log: LogFn; autoRun?: boolean }) {
  const setSignInOpen = useConsole((s) => s.setSignInOpen);
  const live = useConsole((s) => s.mode === "live");
  const noToken = useConsole((s) => !s.token);
  const [count, setCount] = useState<number>(50);
  const [type, setType] = useState<Extract<JobType, "send_email" | "resize_image">>("send_email");
  const [concurrency, setConcurrency] = useState(20);
  const [running, setRunning] = useState(false);
  const [stats, setStats] = useState<Stats>(ZERO);
  const [breakdown, setBreakdown] = useState<Partial<Record<JobStatus, number>>>({});
  const [startedAt, setStartedAt] = useState<number | null>(null);
  const [finishedAt, setFinishedAt] = useState<number | null>(null);
  const [stopped, setStopped] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const now = useNow(500);

  const runRef = useRef(0);
  const cancelRef = useRef(false);
  const statusesRef = useRef(new Map<string, JobStatus>());
  const createFailedRef = useRef(0);
  const cursorRef = useRef(0);

  // Bumping the run id makes every in-flight async loop go stale.
  const cancelRuns = useCallback(() => {
    cancelRef.current = true;
    runRef.current += 1;
  }, []);

  useEffect(() => cancelRuns, [cancelRuns]);

  const syncStats = useCallback(() => {
    let resolved = 0;
    let success = 0;
    let failed = createFailedRef.current;
    const bd: Partial<Record<JobStatus, number>> = {};
    for (const s of statusesRef.current.values()) {
      bd[s] = (bd[s] ?? 0) + 1;
      if (isTerminal(s)) {
        resolved++;
        if (s === "SUCCESS") success++;
        else failed++;
      }
    }
    resolved += createFailedRef.current;
    setStats((prev) => ({ ...prev, resolved, success, failed }));
    setBreakdown(bd);
  }, []);

  const run = useCallback(async () => {
    const runId = ++runRef.current;
    const stale = () => runRef.current !== runId;
    const halt = () => stale() || cancelRef.current;

    cancelRef.current = false;
    statusesRef.current.clear();
    createFailedRef.current = 0;
    cursorRef.current = 0;
    setRunning(true);
    setStopped(false);
    setError(null);
    setStats(ZERO);
    setBreakdown({});
    const t0 = Date.now();
    setStartedAt(t0);
    setFinishedAt(null);

    await useConsole.getState().boot();
    const demo = useConsole.getState().mode === "demo";
    const ids: string[] = [];

    // ── Phase A: create N jobs with bounded concurrency ────────────────────
    let firstCreateError: string | null = null;
    await pool(
      Array.from({ length: count }, (_, i) => i),
      concurrency,
      halt,
      async (i) => {
        setStats((s) => ({ ...s, inFlight: s.inFlight + 1 }));
        try {
          const job = await useConsole.getState().createJob({
            type,
            payload: { ...JOB_TYPE_META[type].template },
            priority: 5,
            idempotency_key: `uitest-${t0}-${i}-${uuid().slice(0, 8)}`,
          });
          ids.push(job.id);
          statusesRef.current.set(job.id, job.status);
        } catch (err) {
          createFailedRef.current++;
          if (!firstCreateError) firstCreateError = errMsg(err);
        } finally {
          setStats((s) => ({ ...s, created: ids.length, inFlight: s.inFlight - 1 }));
        }
      },
    );
    if (stale()) return;
    syncStats();

    const finish = (outcome: "ok" | "warn" | "fail", detail: string) => {
      log({ scenario: "Mini load test", outcome, detail });
      if (stale()) return;
      setStopped(cancelRef.current);
      setFinishedAt(Date.now());
      setRunning(false);
    };

    if (ids.length === 0) {
      const msg = firstCreateError ?? "no jobs were created";
      setError(msg);
      return finish("fail", `${count}× ${type} · create phase failed: ${msg}`);
    }

    // ── Phase B: watch until every created job is terminal ─────────────────
    let timedOut = false;
    while (!halt()) {
      const pending = ids.filter((id) => {
        const s = statusesRef.current.get(id);
        return !s || !isTerminal(s);
      });
      if (pending.length === 0) break;
      if (Date.now() - t0 > WATCH_TIMEOUT_MS) {
        timedOut = true;
        break;
      }
      if (demo) {
        // In-memory world: a full sweep each tick is free.
        for (const id of pending) {
          try {
            const job = await useConsole.getState().getJob(id);
            // The demo world trims old terminal jobs; keep the last known
            // status when a job disappears.
            if (job) statusesRef.current.set(id, job.status);
          } catch {
            // keep last known status
          }
        }
      } else {
        // Live: rotating window of per-id probes, bounded per tick.
        const start = cursorRef.current % pending.length;
        const windowIds =
          pending.length <= LIVE_POLL_WINDOW
            ? pending
            : Array.from({ length: LIVE_POLL_WINDOW }, (_, k) => pending[(start + k) % pending.length]);
        cursorRef.current = start + LIVE_POLL_WINDOW;
        await pool(windowIds, 8, halt, async (id) => {
          try {
            const job = await useConsole.getState().getJob(id);
            if (job) statusesRef.current.set(id, job.status);
          } catch {
            // transient — next rotation picks it up
          }
        });
      }
      if (stale()) return;
      syncStats();
      await sleep(1000);
    }
    if (stale()) return;
    syncStats();

    const finalStats = (() => {
      let resolved = createFailedRef.current;
      let success = 0;
      for (const s of statusesRef.current.values()) {
        if (isTerminal(s)) {
          resolved++;
          if (s === "SUCCESS") success++;
        }
      }
      return { resolved, success };
    })();
    const wall = (Date.now() - t0) / 1000;
    const rate = wall > 0 ? finalStats.resolved / wall : 0;
    const pct = finalStats.resolved > 0 ? Math.round((finalStats.success / finalStats.resolved) * 100) : 0;
    const flags = cancelRef.current ? " · stopped early" : timedOut ? " · timed out" : "";
    finish(
      finalStats.success === finalStats.resolved && !timedOut && !cancelRef.current ? "ok" : "warn",
      `${count}× ${type} · ${wall.toFixed(1)}s · ${rate.toFixed(1)}/s · ${pct}% success${flags}`,
    );
  }, [count, type, concurrency, log, syncStats]);

  useEffect(() => {
    if (!autoRun) return;
    void run();
    return cancelRuns;
  }, [autoRun, run, cancelRuns]);

  const elapsed = startedAt === null ? 0 : ((running ? now : (finishedAt ?? now)) - startedAt) / 1000;
  const rate = elapsed > 0 ? stats.resolved / elapsed : 0;
  const done = !running && startedAt !== null;

  return (
    <Card>
      <CardHeader
        title="Mini load test"
        description="Create N jobs with bounded concurrency, then watch the pool drain."
        action={<Gauge className="h-4 w-4 text-subtle" aria-hidden />}
      />
      <CardContent className="flex flex-col gap-4">
        <div className="flex flex-wrap items-end gap-4">
          <div className="flex flex-col gap-1.5">
            <label htmlFor="lt-count" className="text-xs font-medium text-muted">
              Jobs
            </label>
            <Select id="lt-count" value={String(count)} onChange={(e) => setCount(Number(e.target.value))} disabled={running}>
              {COUNTS.map((c) => (
                <option key={c} value={c}>
                  {c}
                </option>
              ))}
            </Select>
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="lt-type" className="text-xs font-medium text-muted">
              Type
            </label>
            <Select
              id="lt-type"
              value={type}
              onChange={(e) => setType(e.target.value as typeof type)}
              disabled={running}
            >
              <option value="send_email">send_email · io-bound</option>
              <option value="resize_image">resize_image · cpu-bound</option>
            </Select>
          </div>
          <div className="flex min-w-40 flex-1 flex-col gap-1.5">
            <div className="flex items-center justify-between">
              <label htmlFor="lt-conc" className="text-xs font-medium text-muted">
                Concurrency
              </label>
              <span className="text-xs tabular-nums text-muted">{concurrency}</span>
            </div>
            <Slider
              id="lt-conc"
              min={1}
              max={40}
              step={1}
              value={concurrency}
              onChange={(e) => setConcurrency(Number(e.target.value))}
              disabled={running}
            />
          </div>
          {running ? (
            <Button variant="danger" onClick={() => (cancelRef.current = true)}>
              <Square className="h-4 w-4" aria-hidden /> Stop
            </Button>
          ) : (
            <Button onClick={() => void run()}>
              <Play className="h-4 w-4" aria-hidden /> Start
            </Button>
          )}
        </div>

        {(running || done) && (
          <>
            <Progress
              value={stats.resolved}
              max={count}
              tone={stats.failed > 0 ? "warning" : "accent"}
              label="Jobs resolved"
            />
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-5">
              {(
                [
                  ["created", fmtNum(stats.created)],
                  ["in-flight", fmtNum(stats.inFlight)],
                  ["success", fmtNum(stats.success)],
                  ["failed", fmtNum(stats.failed)],
                  ["rate", `${rate.toFixed(1)}/s`],
                ] as const
              ).map(([label, value]) => (
                <div key={label}>
                  <p className="text-xs uppercase tracking-wide text-muted">{label}</p>
                  <p className="mt-0.5 text-lg font-semibold tabular-nums text-fg">{value}</p>
                </div>
              ))}
            </div>
          </>
        )}

        {error && live && noToken ? (
          <p className="flex flex-wrap items-center gap-2 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            {error} — the live gateway requires a session for this.
            <Button variant="outline" size="sm" onClick={() => setSignInOpen(true)}>
              <LogIn className="h-3.5 w-3.5" aria-hidden /> Sign in
            </Button>
          </p>
        ) : error ? (
          <p role="alert" className="rounded-md border border-error/40 bg-error/10 px-3 py-2 text-xs text-error">
            Create phase failed: {error}
          </p>
        ) : null}

        {done && (
          <div className="flex flex-col gap-2 rounded-md border border-border bg-surface-2/50 px-3 py-2">
            <p className="text-sm text-fg">
              {stopped ? "Stopped early" : "Done"} · wall time {elapsed.toFixed(1)}s ·{" "}
              {rate.toFixed(1)} jobs/s ·{" "}
              {stats.resolved > 0 ? Math.round((stats.success / stats.resolved) * 100) : 0}% success
            </p>
            {Object.keys(breakdown).length > 0 && (
              <div className="flex flex-wrap items-center gap-2">
                {JOB_STATUSES.filter((s) => breakdown[s]).map((s) => (
                  <span key={s} className="flex items-center gap-1">
                    <StatusChip status={s} />
                    <span className="font-mono text-xs tabular-nums text-muted">{breakdown[s]}</span>
                  </span>
                ))}
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
