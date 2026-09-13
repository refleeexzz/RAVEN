import { cn } from "@/lib/utils";

export function Progress({
  value,
  max = 100,
  tone = "accent",
  className,
  label,
}: {
  value: number;
  max?: number;
  tone?: "accent" | "success" | "warning" | "error";
  className?: string;
  label?: string;
}) {
  const pct = max > 0 ? Math.min(100, Math.max(0, (value / max) * 100)) : 0;
  const tones = {
    accent: "bg-accent",
    success: "bg-success",
    warning: "bg-warning",
    error: "bg-error",
  } as const;
  return (
    <div
      role="progressbar"
      aria-valuenow={Math.round(pct)}
      aria-valuemin={0}
      aria-valuemax={100}
      aria-label={label}
      className={cn("h-2 w-full overflow-hidden rounded-sm bg-surface-2", className)}
    >
      <div
        className={cn("h-full rounded-sm transition-[width] duration-200", tones[tone])}
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}
