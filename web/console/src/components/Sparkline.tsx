import { useId } from "react";
import { cn } from "@/lib/utils";

// Tiny hand-rolled SVG sparkline for stat cards and topic rows.
// (Recharts is used for the main overview chart; these are too small for it.)

export function Sparkline({
  data,
  className,
  tone = "accent",
}: {
  data: number[];
  className?: string;
  tone?: "accent" | "success" | "warning" | "error";
}) {
  const gradientId = useId();
  if (data.length < 2) {
    return <div className={cn("h-6 w-full rounded-sm bg-surface-2/40", className)} aria-hidden />;
  }
  const w = 120;
  const h = 24;
  const min = Math.min(...data);
  const max = Math.max(...data);
  const span = max - min || 1;
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - 2 - ((v - min) / span) * (h - 4);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const line = pts.join(" ");
  const area = `0,${h} ${line} ${w},${h}`;
  const colors = {
    accent: "#8b5cf6",
    success: "#34d399",
    warning: "#fbbf24",
    error: "#f87171",
  } as const;
  return (
    <svg
      viewBox={`0 0 ${w} ${h}`}
      preserveAspectRatio="none"
      className={cn("h-6 w-full", className)}
      aria-hidden
    >
      <polygon points={area} fill={colors[tone]} opacity={0.12} id={gradientId} />
      <polyline
        points={line}
        fill="none"
        stroke={colors[tone]}
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
        vectorEffect="non-scaling-stroke"
      />
    </svg>
  );
}
