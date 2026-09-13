import { cn } from "@/lib/utils";

export type ButtonVariant = "primary" | "secondary" | "ghost" | "danger" | "outline";
export type ButtonSize = "sm" | "md" | "icon" | "icon-sm";

const base =
  "inline-flex select-none items-center justify-center gap-2 rounded-md font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-bg disabled:pointer-events-none disabled:opacity-50";

const variants: Record<ButtonVariant, string> = {
  primary: "bg-accent text-white hover:bg-accent-hover",
  secondary: "border border-border bg-surface-2 text-fg hover:bg-border/60",
  ghost: "text-muted hover:bg-surface-2 hover:text-fg",
  danger: "border border-error/40 bg-error/10 text-error hover:bg-error/20",
  outline: "border border-border bg-transparent text-fg hover:bg-surface-2",
};

const sizes: Record<ButtonSize, string> = {
  sm: "h-6 px-2 text-xs",
  md: "h-8 px-3 text-sm",
  icon: "h-8 w-8",
  "icon-sm": "h-6 w-6",
};

/** Shared so <button> and <a> can render the same visual button. */
export function buttonClasses(variant: ButtonVariant = "primary", size: ButtonSize = "md"): string {
  return cn(base, variants[variant], sizes[size]);
}
