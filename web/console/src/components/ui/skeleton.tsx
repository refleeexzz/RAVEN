import { cn } from "@/lib/utils";

export function Skeleton({ className }: { className?: string }) {
  return <div className={cn("animate-pulse rounded-md bg-surface-2 motion-reduce:animate-none", className)} />;
}

/** Skeleton rows that match the jobs table layout (same heights, no shift). */
export function TableSkeleton({ rows = 8, cols = 6 }: { rows?: number; cols?: number }) {
  return (
    <div className="flex flex-col">
      {Array.from({ length: rows }, (_, r) => (
        <div key={r} className="flex items-center gap-4 border-b border-border/60 px-3 py-2">
          {Array.from({ length: cols }, (_, c) => (
            <Skeleton
              key={c}
              className={cn("h-4", c === 0 ? "w-32" : c === cols - 1 ? "ml-auto w-16" : "w-20")}
            />
          ))}
        </div>
      ))}
    </div>
  );
}
