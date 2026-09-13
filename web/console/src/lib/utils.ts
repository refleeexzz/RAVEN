// Small utilities. Formatting is Intl-based (zero dependencies).

export function cn(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(" ");
}

const numFmt = new Intl.NumberFormat("en-US");
const compactFmt = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});
const timeFmt = new Intl.DateTimeFormat("en-US", {
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

export const fmtNum = (n: number) => numFmt.format(Math.round(n));
export const fmtCompact = (n: number) => compactFmt.format(n);

export function fmtMs(ms: number | null): string {
  if (ms === null) return "—";
  if (ms < 1) return `${ms.toFixed(2)}ms`;
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export function fmtPct(ratio: number | null, digits = 2): string {
  if (ratio === null) return "—";
  return `${(ratio * 100).toFixed(digits)}%`;
}

export function fmtTime(t: number | string | Date): string {
  return timeFmt.format(new Date(t));
}

/** "4s ago", "2m ago", "1h ago" — short and dense for ops tables. */
export function relTime(iso: string | number, now: number = Date.now()): string {
  const t = typeof iso === "number" ? iso : new Date(iso).getTime();
  const diff = Math.max(0, Math.round((now - t) / 1000));
  if (diff < 2) return "just now";
  if (diff < 60) return `${diff}s ago`;
  const m = Math.floor(diff / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

/** job_01JFXKM2V9QP → "job_01JFXKM2…" for dense tables. */
export function shortId(id: string, head = 12): string {
  return id.length <= head + 1 ? id : `${id.slice(0, head)}…`;
}

export function uuid(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export const clamp = (n: number, min: number, max: number) =>
  Math.min(max, Math.max(min, n));
