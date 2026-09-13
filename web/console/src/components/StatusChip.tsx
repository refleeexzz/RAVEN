import { Badge, Dot } from "./ui/badge";
import type { JobStatus } from "@/lib/types";

const STATUS_META: Record<
  JobStatus,
  { label: string; variant: "info" | "accent" | "success" | "error" | "warning" | "default"; dot: "info" | "accent" | "success" | "error" | "warning" | "muted"; pulse: boolean }
> = {
  QUEUED: { label: "Queued", variant: "info", dot: "info", pulse: false },
  PROCESSING: { label: "Processing", variant: "accent", dot: "accent", pulse: true },
  SUCCESS: { label: "Success", variant: "success", dot: "success", pulse: false },
  FAILED: { label: "Failed", variant: "error", dot: "error", pulse: false },
  RETRYING: { label: "Retrying", variant: "warning", dot: "warning", pulse: true },
  DEAD: { label: "Dead", variant: "error", dot: "error", pulse: false },
  CANCELLED: { label: "Cancelled", variant: "default", dot: "muted", pulse: false },
};

export function StatusChip({ status }: { status: JobStatus }) {
  const meta = STATUS_META[status];
  return (
    <Badge variant={meta.variant}>
      <Dot variant={meta.dot} pulse={meta.pulse} />
      {meta.label}
    </Badge>
  );
}

export { STATUS_META };
