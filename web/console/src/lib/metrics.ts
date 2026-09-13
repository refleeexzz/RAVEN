import type { MetricSample } from "./types";

// Minimal Prometheus text exposition parser — enough to read the gateway's
// /metrics endpoint and pick out a handful of interesting series.

const SAMPLE_RE =
  /^([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{([^}]*)\})?\s+([+-]?(?:\d+(?:\.\d+)?(?:[eE][+-]?\d+)?|\.\d+|NaN|\+?Inf|-Inf))(?:\s+\d+)?\s*$/;
const LABEL_RE = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;

export function parsePrometheus(text: string): MetricSample[] {
  const out: MetricSample[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const m = SAMPLE_RE.exec(line);
    if (!m) continue;
    const value = Number(m[3]);
    if (Number.isNaN(value)) continue;
    const labels: Record<string, string> = {};
    if (m[2]) {
      for (const lm of m[2].matchAll(LABEL_RE)) {
        labels[lm[1]] = lm[2].replace(/\\"/g, '"').replace(/\\\\/g, "\\");
      }
    }
    out.push({ name: m[1], labels, value });
  }
  return out;
}

/** Pick the series worth showing on the Observability page. */
export function pickInteresting(samples: MetricSample[], limit = 10): MetricSample[] {
  const wanted = /^(raven_|http_requests_total|http_request_duration|go_goroutines|go_memstats_alloc_bytes$|process_cpu_seconds_total)/;
  const interesting = samples.filter((s) => wanted.test(s.name));
  const picked = (interesting.length > 0 ? interesting : samples).slice(0, limit);
  return picked;
}

export function formatMetricValue(v: number): string {
  if (!Number.isFinite(v)) return String(v);
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(2)}M`;
  if (Math.abs(v) >= 10_000) return `${(v / 1_000).toFixed(1)}k`;
  if (Number.isInteger(v)) return String(v);
  return v.toFixed(4).replace(/0+$/, "").replace(/\.$/, "");
}
