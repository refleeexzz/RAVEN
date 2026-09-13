import type { HTMLAttributes } from "react";
import { cn } from "@/lib/utils";

export type BadgeVariant =
  | "default"
  | "accent"
  | "success"
  | "warning"
  | "error"
  | "info"
  | "outline";

const variants: Record<BadgeVariant, string> = {
  default: "bg-surface-2 text-muted",
  accent: "bg-accent/15 text-accent-fg",
  success: "bg-success/10 text-success",
  warning: "bg-warning/10 text-warning",
  error: "bg-error/10 text-error",
  info: "bg-info/10 text-info",
  outline: "border border-border text-muted",
};

export interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  variant?: BadgeVariant;
}

export function Badge({ className, variant = "default", ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex h-5 shrink-0 items-center gap-1.5 rounded-sm px-2 text-xs font-medium",
        variants[variant],
        className,
      )}
      {...props}
    />
  );
}

/** Small status dot, pairs with a label (GitHub-style dot + label pattern). */
export function Dot({ variant, pulse = false }: { variant: "success" | "warning" | "error" | "info" | "accent" | "muted"; pulse?: boolean }) {
  const colors = {
    success: "bg-success",
    warning: "bg-warning",
    error: "bg-error",
    info: "bg-info",
    accent: "bg-accent",
    muted: "bg-subtle",
  } as const;
  return (
    <span className="relative flex h-2 w-2 shrink-0">
      {pulse && (
        <span className={cn("absolute inline-flex h-full w-full animate-ping rounded-full opacity-60 motion-reduce:animate-none", colors[variant])} />
      )}
      <span className={cn("relative inline-flex h-2 w-2 rounded-full", colors[variant])} />
    </span>
  );
}
