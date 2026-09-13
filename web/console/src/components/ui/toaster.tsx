import { CheckCircle2, Info, X, XCircle } from "lucide-react";
import { dismissToast, useToasts, type ToastVariant } from "@/lib/toast";
import { cn } from "@/lib/utils";

const icons: Record<ToastVariant, typeof Info> = {
  default: Info,
  success: CheckCircle2,
  error: XCircle,
};

const iconColors: Record<ToastVariant, string> = {
  default: "text-info",
  success: "text-success",
  error: "text-error",
};

export function Toaster() {
  const toasts = useToasts();
  return (
    <div
      aria-live="polite"
      className="pointer-events-none fixed bottom-4 right-4 z-50 flex w-80 flex-col gap-2"
    >
      {toasts.map((t) => {
        const Icon = icons[t.variant];
        return (
          <div
            key={t.id}
            className="pointer-events-auto flex items-start gap-3 rounded-md border border-border bg-surface p-3 shadow-lg"
          >
            <Icon className={cn("mt-0.5 h-4 w-4 shrink-0", iconColors[t.variant])} aria-hidden />
            <div className="min-w-0 flex-1">
              <p className="text-sm font-medium text-fg">{t.title}</p>
              {t.description && (
                <p className="mt-0.5 break-words font-mono text-xs text-muted">{t.description}</p>
              )}
            </div>
            <button
              type="button"
              onClick={() => dismissToast(t.id)}
              aria-label="Dismiss notification"
              className="rounded-md p-1 text-muted transition-colors duration-150 hover:bg-surface-2 hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        );
      })}
    </div>
  );
}
