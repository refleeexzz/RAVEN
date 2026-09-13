import { CheckCircle2, Circle, Loader2, MinusCircle, XCircle } from "lucide-react";
import { fmtMs } from "@/lib/utils";
import { cn } from "@/lib/utils";
import type { StepState, StepStatus } from "./lab";

// Vertical step timeline used by the E2E self-test: one row per step with a
// status icon, an optional detail line, and the step duration on the right.

function StepIcon({ status }: { status: StepStatus }) {
  switch (status) {
    case "running":
      return <Loader2 className="h-4 w-4 animate-spin text-accent-fg" aria-hidden />;
    case "ok":
      return <CheckCircle2 className="h-4 w-4 text-success" aria-hidden />;
    case "fail":
      return <XCircle className="h-4 w-4 text-error" aria-hidden />;
    case "skipped":
      return <MinusCircle className="h-4 w-4 text-warning" aria-hidden />;
    default:
      return <Circle className="h-4 w-4 text-subtle" aria-hidden />;
  }
}

export function StepTimeline({ steps }: { steps: StepState[] }) {
  return (
    <ol className="flex flex-col">
      {steps.map((step, i) => (
        <li
          key={i}
          className={cn(
            "flex items-start gap-3 py-2",
            i < steps.length - 1 && "border-b border-border/60",
          )}
        >
          <span className="mt-0.5 shrink-0">
            <StepIcon status={step.status} />
          </span>
          <div className="min-w-0 flex-1">
            <p
              className={cn(
                "text-sm",
                step.status === "pending" ? "text-muted" : "text-fg",
              )}
            >
              <span className="mr-2 font-mono text-xs text-subtle">{i + 1}.</span>
              {step.label}
            </p>
            {step.detail && (
              <p
                className={cn(
                  "ml-6 mt-0.5 font-mono text-xs",
                  step.status === "fail" ? "text-error" : "text-subtle",
                )}
              >
                {step.detail}
              </p>
            )}
          </div>
          {step.durationMs !== undefined && (
            <span className="shrink-0 font-mono text-xs tabular-nums text-subtle">
              {fmtMs(step.durationMs)}
            </span>
          )}
        </li>
      ))}
    </ol>
  );
}
