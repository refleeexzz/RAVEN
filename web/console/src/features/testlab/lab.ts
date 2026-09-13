import { useConsole } from "@/lib/store";
import type { Job, JobStatus } from "@/lib/types";

// Shared logic for the Test Lab scenarios. Components stay thin; the async
// runners live here and in the scenario components themselves.

export type StepStatus = "pending" | "running" | "ok" | "fail" | "skipped";

export interface StepState {
  label: string;
  status: StepStatus;
  detail?: string;
  durationMs?: number;
}

export interface LogEntry {
  id: string;
  at: number;
  scenario: string;
  outcome: "ok" | "warn" | "fail";
  detail: string;
}

export type LogFn = (e: Omit<LogEntry, "id" | "at">) => void;

export const TERMINAL: readonly JobStatus[] = ["SUCCESS", "FAILED", "DEAD", "CANCELLED"];
export const isTerminal = (s: JobStatus): boolean => TERMINAL.includes(s);

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

export function errMsg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

/**
 * Runs `worker` over `items` with bounded concurrency. Stops launching new
 * items as soon as `halt()` returns true (in-flight items finish).
 */
export async function pool<T>(
  items: readonly T[],
  concurrency: number,
  halt: () => boolean,
  worker: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let next = 0;
  const lanes = Array.from(
    { length: Math.max(1, Math.min(concurrency, items.length)) },
    async () => {
      while (next < items.length) {
        if (halt()) return;
        const i = next++;
        await worker(items[i], i);
      }
    },
  );
  await Promise.all(lanes);
}

/**
 * Polls a job until it reaches a terminal status, the timeout hits, or
 * `cancelled()` becomes true. `onUpdate` fires on every successful poll.
 */
export async function watchJob(
  id: string,
  onUpdate: (job: Job) => void,
  opts: { intervalMs?: number; timeoutMs?: number; cancelled?: () => boolean } = {},
): Promise<{ job: Job | null; timedOut: boolean }> {
  const intervalMs = opts.intervalMs ?? 1000;
  const timeoutMs = opts.timeoutMs ?? 20_000;
  const t0 = Date.now();
  let last: Job | null = null;
  while (Date.now() - t0 < timeoutMs) {
    if (opts.cancelled?.()) return { job: last, timedOut: false };
    try {
      const job = await useConsole.getState().getJob(id);
      if (job) {
        last = job;
        onUpdate(job);
        if (isTerminal(job.status)) return { job, timedOut: false };
      }
    } catch {
      // transient poll error — keep watching until the timeout
    }
    await sleep(intervalMs);
  }
  return { job: last, timedOut: true };
}

/** Copies text to the clipboard, with a textarea fallback for older contexts. */
export async function copyText(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    try {
      const ta = document.createElement("textarea");
      ta.value = text;
      ta.style.position = "fixed";
      ta.style.opacity = "0";
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      return ok;
    } catch {
      return false;
    }
  }
}
