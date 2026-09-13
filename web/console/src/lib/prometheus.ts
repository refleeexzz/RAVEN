import { config } from "./config";

// Minimal Prometheus HTTP API client (no auth, CORS open on :9090).
// Instant queries feed the stat cards; query_range feeds the throughput
// chart. Non-finite samples (NaN/Inf) and empty results map to null, never
// to garbage numbers.

interface PromInstant {
  status: string;
  data: { resultType: string; result: Array<{ value?: [number, string] }> };
}

interface PromRange {
  status: string;
  data: {
    resultType: string;
    result: Array<{ values?: Array<[number, string]> }>;
  };
}

function finiteOrNull(raw: string): number | null {
  const v = Number(raw);
  return Number.isFinite(v) ? v : null;
}

/** Sums an instant vector. Returns null when there are no usable samples. */
export async function promInstant(query: string): Promise<number | null> {
  const url = `${config.prometheusUrl}/api/v1/query?query=${encodeURIComponent(query)}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(3000) });
  if (!res.ok) throw new Error(`Prometheus query → HTTP ${res.status}`);
  const data = (await res.json()) as PromInstant;
  if (data.status !== "success") throw new Error("Prometheus returned an error status");
  let sum = 0;
  let found = false;
  for (const r of data.data.result) {
    if (!r.value) continue;
    const v = finiteOrNull(r.value[1]);
    if (v === null) continue;
    sum += v;
    found = true;
  }
  return found ? sum : null;
}

export interface RangePoint {
  t: number; // epoch ms
  v: number;
}

/** Flattens a matrix result into one time-ordered series (sums per step). */
export async function promRange(query: string, startMs: number, endMs: number, stepS: number): Promise<RangePoint[]> {
  const params = new URLSearchParams({
    query,
    start: (startMs / 1000).toFixed(0),
    end: (endMs / 1000).toFixed(0),
    step: `${stepS}s`,
  });
  const url = `${config.prometheusUrl}/api/v1/query_range?${params.toString()}`;
  const res = await fetch(url, { signal: AbortSignal.timeout(4000) });
  if (!res.ok) throw new Error(`Prometheus range query → HTTP ${res.status}`);
  const data = (await res.json()) as PromRange;
  if (data.status !== "success") throw new Error("Prometheus returned an error status");
  const byT = new Map<number, number>();
  for (const r of data.data.result) {
    for (const [ts, raw] of r.values ?? []) {
      const v = finiteOrNull(raw);
      if (v === null) continue;
      const t = Math.round(ts * 1000);
      byT.set(t, (byT.get(t) ?? 0) + v);
    }
  }
  return [...byT.entries()].sort((a, b) => a[0] - b[0]).map(([t, v]) => ({ t, v }));
}
