import type { ReactNode } from "react";
import { AlertTriangle, type LucideIcon } from "lucide-react";
import { Button } from "./button";

// Designed async states: empty (explanation + one clear action) and error
// (plain-language message + retry). Used by every async surface.

export function EmptyState({
  icon: Icon,
  title,
  description,
  action,
}: {
  icon: LucideIcon;
  title: string;
  description: string;
  action?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 px-6 py-12 text-center">
      <div className="flex h-8 w-8 items-center justify-center rounded-md border border-border bg-surface-2">
        <Icon className="h-4 w-4 text-muted" aria-hidden />
      </div>
      <p className="mt-1 text-sm font-medium text-fg">{title}</p>
      <p className="max-w-md text-sm text-muted">{description}</p>
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

export function ErrorState({
  message,
  onRetry,
}: {
  message: string;
  onRetry?: () => void;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 px-6 py-12 text-center">
      <div className="flex h-8 w-8 items-center justify-center rounded-md border border-error/40 bg-error/10">
        <AlertTriangle className="h-4 w-4 text-error" aria-hidden />
      </div>
      <p className="mt-1 text-sm font-medium text-fg">Something went wrong</p>
      <p className="max-w-md text-sm text-muted">{message}</p>
      {onRetry && (
        <Button variant="secondary" size="sm" className="mt-2" onClick={onRetry}>
          Try again
        </Button>
      )}
    </div>
  );
}
