import { useCallback, useEffect, useRef, useState } from "react";
import { ArrowRight, HeartPulse, LogIn, Play } from "lucide-react";
import { config } from "@/lib/config";
import { useConsole } from "@/lib/store";
import type { Job, JobStatus } from "@/lib/types";
import { fmtMs, shortId, uuid } from "@/lib/utils";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { errMsg, sleep, watchJob, type LogFn, type StepState } from "./lab";
import { StepTimeline } from "./StepTimeline";

// Scenario 1 — Quick E2E check: one button drives the whole pipeline
// (gateway → auth → jobs service → broker → worker → back) and renders each
// step as it happens.

const INITIAL_STEPS: StepState[] = [
  { label: "Reach gateway (GET /health)", status: "pending" },
  { label: "Session token", status: "pending" },
  { label: "Create send_email job (POST /api/jobs)", status: "pending" },
  { label: "Watch job to a terminal status (GET /api/jobs/{id})", status: "pending" },
];

type Verdict = { tone: "ok" | "warn" | "fail"; text: string };

const verdictStyles: Record<Verdict["tone"], string> = {
  ok: "border-success/40 bg-success/10 text-success",
  warn: "border-warning/40 bg-warning/10 text-warning",
  fail: "border-error/40 bg-error/10 text-error",
};

export function E2eCheck({ log, autoRun = false }: { log: LogFn; autoRun?: boolean }) {
  const setSignInOpen = useConsole((s) => s.setSignInOpen);
  const [steps, setSteps] = useState<StepState[]>(INITIAL_STEPS);
  const [transitions, setTransitions] = useState<Array<{ status: JobStatus; at: number }>>([]);
  const [verdict, setVerdict] = useState<Verdict | null>(null);
  const [running, setRunning] = useState(false);
  const [needsSignIn, setNeedsSignIn] = useState(false);
  const runRef = useRef(0);

  // Bumping the run id makes every in-flight async loop go stale.
  const cancelRuns = useCallback(() => {
    runRef.current += 1;
  }, []);

  // Cancel in-flight work on unmount (and on StrictMode remount).
  useEffect(() => cancelRuns, [cancelRuns]);

  const run = useCallback(async () => {
    const runId = ++runRef.current;
    const stale = () => runRef.current !== runId;
    const setStep = (i: number, patch: Partial<StepState>) =>
      setSteps((prev) => prev.map((s, j) => (j === i ? { ...s, ...patch } : s)));

    setRunning(true);
    setVerdict(null);
    setNeedsSignIn(false);
    setTransitions([]);
    setSteps(INITIAL_STEPS.map((s) => ({ ...s })));
    const t0 = Date.now();

    const finish = (v: Verdict, outcome: "ok" | "warn" | "fail", detail: string) => {
      log({ scenario: "Quick E2E check", outcome, detail });
      if (stale()) return;
      setVerdict(v);
      setRunning(false);
    };

    await useConsole.getState().boot();
    const demo = useConsole.getState().mode === "demo";

    // ── Step 1: reach the gateway ──────────────────────────────────────────
    setStep(0, { status: "running" });
    let s = Date.now();
    if (demo) {
      await sleep(200);
      if (stale()) return;
      setStep(0, { status: "ok", detail: "demo simulator stands in for the gateway", durationMs: Date.now() - s });
    } else {
      try {
        const res = await fetch(`${config.gatewayUrl}/health`, { signal: AbortSignal.timeout(4000) });
        if (!res.ok) throw new Error(`GET /health → HTTP ${res.status}`);
        if (stale()) return;
        setStep(0, { status: "ok", detail: `GET /health → ${res.status}`, durationMs: Date.now() - s });
      } catch (err) {
        if (stale()) return;
        const msg = errMsg(err);
        setStep(0, { status: "fail", detail: msg, durationMs: Date.now() - s });
        setStep(1, { status: "skipped" });
        setStep(2, { status: "skipped" });
        setStep(3, { status: "skipped" });
        return finish(
          { tone: "fail", text: "Step 1 failed — gateway unreachable. Is the stack up?" },
          "fail",
          `gateway unreachable: ${msg}`,
        );
      }
    }

    // ── Step 2: session token ──────────────────────────────────────────────
    setStep(1, { status: "running" });
    s = Date.now();
    const { token, email } = useConsole.getState();
    if (stale()) return;
    if (token) {
      setStep(1, { status: "ok", detail: `signed in as ${email ?? "unknown"}`, durationMs: Date.now() - s });
    } else {
      // Not a wall: the run continues, but live mutating calls will 401.
      setNeedsSignIn(true);
      setStep(1, { status: "skipped", detail: "no session token", durationMs: Date.now() - s });
    }

    // ── Step 3: create a job ───────────────────────────────────────────────
    setStep(2, { status: "running" });
    s = Date.now();
    let job: Job;
    try {
      if (stale()) return;
      job = await useConsole.getState().createJob({
        type: "send_email",
        payload: { to: "testlab@raven.dev" },
        priority: 5,
        idempotency_key: uuid(),
      });
      if (stale()) return;
      setStep(2, { status: "ok", detail: `created ${job.id}`, durationMs: Date.now() - s });
    } catch (err) {
      if (stale()) return;
      const msg = errMsg(err);
      setStep(2, { status: "fail", detail: msg, durationMs: Date.now() - s });
      setStep(3, { status: "skipped" });
      return finish(
        { tone: "fail", text: `Step 3 failed — could not create the job: ${msg}` },
        "fail",
        `create failed: ${msg}`,
      );
    }

    // ── Step 4: watch to terminal ──────────────────────────────────────────
    setStep(3, { status: "running", detail: `watching ${shortId(job.id)}…` });
    s = Date.now();
    const seen: Array<{ status: JobStatus; at: number }> = [{ status: job.status, at: Date.now() }];
    const { job: finalJob, timedOut } = await watchJob(
      job.id,
      (j) => {
        if (j.status !== seen[seen.length - 1].status) {
          seen.push({ status: j.status, at: Date.now() });
          if (!stale()) setTransitions([...seen]);
        }
        if (!stale()) setStep(3, { detail: `${j.status} · attempt ${j.attempts}` });
      },
      { intervalMs: 1000, timeoutMs: 20_000, cancelled: stale },
    );
    if (stale()) return;
    const durationMs = Date.now() - s;
    if (timedOut || !finalJob) {
      const last = finalJob?.status ?? job.status;
      setStep(3, { status: "fail", detail: `still ${last} after 20s`, durationMs });
      return finish(
        { tone: "fail", text: "Step 4 timed out — the job never went terminal. Workers down?" },
        "fail",
        `job ${shortId(job.id)} stuck at ${last} (20s timeout)`,
      );
    }
    setStep(3, { status: "ok", detail: `terminal: ${finalJob.status}`, durationMs });
    const total = fmtMs(Date.now() - t0);
    if (finalJob.status === "SUCCESS") {
      return finish({ tone: "ok", text: `Platform is alive (${total})` }, "ok", `job ${shortId(job.id)} → SUCCESS in ${total}`);
    }
    return finish(
      { tone: "warn", text: `Pipeline works, but the job ended ${finalJob.status} — check worker logs.` },
      "warn",
      `job ${shortId(job.id)} ended ${finalJob.status}`,
    );
  }, [log]);

  // Headless-verification hook: `#/testlab?autorun=e2e` starts the scenario.
  useEffect(() => {
    if (!autoRun) return;
    void run();
    return cancelRuns;
  }, [autoRun, run, cancelRuns]);

  return (
    <Card>
      <CardHeader
        title="Quick E2E check"
        description="One click drives the whole pipeline: gateway → auth → jobs → broker → worker → back."
        action={<HeartPulse className="h-4 w-4 text-subtle" aria-hidden />}
      />
      <CardContent className="flex flex-col gap-4">
        <StepTimeline steps={steps} />

        {transitions.length > 0 && (
          <div className="flex flex-wrap items-center gap-1.5" aria-label="Observed status transitions">
            <span className="text-xs text-subtle">observed:</span>
            {transitions.map((t, i) => (
              <span key={i} className="flex items-center gap-1.5">
                {i > 0 && <ArrowRight className="h-3 w-3 text-subtle" aria-hidden />}
                <span className="rounded-sm bg-surface-2 px-1.5 py-0.5 font-mono text-xs text-fg">
                  {t.status}
                </span>
              </span>
            ))}
          </div>
        )}

        {needsSignIn && !running && (
          <p className="flex flex-wrap items-center gap-2 rounded-md border border-warning/40 bg-warning/10 px-3 py-2 text-xs text-warning">
            No session token — live mutating calls return 401. Sign in and run again.
            <Button variant="outline" size="sm" onClick={() => setSignInOpen(true)}>
              <LogIn className="h-3.5 w-3.5" aria-hidden /> Sign in
            </Button>
          </p>
        )}

        {verdict && (
          <p className={cn("rounded-md border px-3 py-2 text-sm font-medium", verdictStyles[verdict.tone])} role="status">
            {verdict.text}
          </p>
        )}

        <div>
          <Button onClick={() => void run()} loading={running}>
            <Play className="h-4 w-4" aria-hidden />
            {running ? "Running…" : "Run self-test"}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
