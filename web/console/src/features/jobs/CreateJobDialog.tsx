import { useEffect, useState, type FormEvent } from "react";
import { RotateCcw } from "lucide-react";
import { useConsole } from "@/lib/store";
import { toast } from "@/lib/toast";
import { uuid } from "@/lib/utils";
import { JOB_TYPE_META, type JobType } from "@/lib/types";
import { Dialog } from "@/components/ui/overlay";
import { Button } from "@/components/ui/button";
import { Input, Textarea } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Slider } from "@/components/ui/slider";

function priorityLabel(p: number): string {
  if (p <= 3) return "Low";
  if (p <= 7) return "Normal";
  return "Critical";
}

const pretty = (v: Record<string, unknown>) => JSON.stringify(v, null, 2);

export function CreateJobDialog({ onCreated }: { onCreated?: () => void }) {
  const open = useConsole((s) => s.createJobOpen);
  const setOpen = useConsole((s) => s.setCreateJobOpen);
  const createJob = useConsole((s) => s.createJob);
  const mode = useConsole((s) => s.mode);
  const token = useConsole((s) => s.token);

  const [type, setType] = useState<JobType>("send_email");
  const [payloadText, setPayloadText] = useState(() => pretty(JOB_TYPE_META.send_email.template));
  const [payloadDirty, setPayloadDirty] = useState(false);
  const [priority, setPriority] = useState(5);
  const [idemKey, setIdemKey] = useState(() => uuid());
  const [error, setError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (open) {
      setType("send_email");
      setPayloadText(pretty(JOB_TYPE_META.send_email.template));
      setPayloadDirty(false);
      setPriority(5);
      setIdemKey(uuid());
      setError(null);
      setSubmitting(false);
    }
  }, [open]);

  const onSubmit = async (e: FormEvent) => {
    e.preventDefault();
    let payload: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(payloadText);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        throw new Error("payload must be an object");
      }
      payload = parsed as Record<string, unknown>;
    } catch {
      setError("Payload must be a valid JSON object.");
      return;
    }
    setSubmitting(true);
    setError(null);
    try {
      const job = await createJob({ type, payload, priority, idempotency_key: idemKey });
      toast({ variant: "success", title: "Job queued", description: job.id });
      setOpen(false);
      onCreated?.();
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to create job";
      setError(
        mode === "live" && !token
          ? `${msg} — mutating actions usually require a signed-in session.`
          : msg,
      );
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <Dialog
      open={open}
      onClose={() => setOpen(false)}
      title="Create job"
      description="Enqueue work for the worker pool. Sent with an Idempotency-Key header, so retries are safe."
    >
      <form onSubmit={(e) => void onSubmit(e)} className="flex flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <label htmlFor="job-type" className="text-sm font-medium text-fg">
            Type
          </label>
          <Select
            id="job-type"
            value={type}
            onChange={(e) => {
              const next = e.target.value as JobType;
              setType(next);
              if (!payloadDirty) setPayloadText(pretty(JOB_TYPE_META[next].template));
            }}
            className="w-full"
          >
            {(Object.keys(JOB_TYPE_META) as JobType[]).map((t) => (
              <option key={t} value={t}>
                {JOB_TYPE_META[t].label} — {JOB_TYPE_META[t].description}
              </option>
            ))}
          </Select>
        </div>

        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <label htmlFor="job-payload" className="text-sm font-medium text-fg">
              Payload (JSON)
            </label>
            <button
              type="button"
              onClick={() => {
                setPayloadText(pretty(JOB_TYPE_META[type].template));
                setPayloadDirty(false);
                setError(null);
              }}
              className="text-xs text-muted transition-colors duration-150 hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent rounded-sm"
            >
              Reset to template
            </button>
          </div>
          <Textarea
            id="job-payload"
            rows={7}
            value={payloadText}
            onChange={(e) => {
              setPayloadText(e.target.value);
              setPayloadDirty(true);
            }}
            spellCheck={false}
            className="font-mono text-xs"
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? "job-payload-error" : undefined}
          />
        </div>

        <div className="flex flex-col gap-1.5">
          <div className="flex items-center justify-between">
            <label htmlFor="job-priority" className="text-sm font-medium text-fg">
              Priority
            </label>
            <span className="text-sm tabular-nums text-muted">
              {priority} · {priorityLabel(priority)}
            </span>
          </div>
          <Slider
            id="job-priority"
            min={1}
            max={10}
            step={1}
            value={priority}
            onChange={(e) => setPriority(Number(e.target.value))}
            aria-valuetext={`${priority} (${priorityLabel(priority)})`}
          />
        </div>

        <div className="flex flex-col gap-1.5">
          <label htmlFor="job-idem" className="text-sm font-medium text-fg">
            Idempotency key
          </label>
          <div className="flex gap-2">
            <Input
              id="job-idem"
              value={idemKey}
              onChange={(e) => setIdemKey(e.target.value)}
              className="font-mono text-xs"
            />
            <Button
              type="button"
              variant="secondary"
              size="icon"
              aria-label="Regenerate idempotency key"
              onClick={() => setIdemKey(uuid())}
            >
              <RotateCcw className="h-4 w-4" />
            </Button>
          </div>
          <p className="text-xs text-subtle">
            Same key + same payload = the gateway returns the existing job instead of duplicating work.
          </p>
        </div>

        {error && (
          <p id="job-payload-error" role="alert" className="text-sm text-error">
            {error}
          </p>
        )}

        <div className="flex justify-end gap-2">
          <Button type="button" variant="ghost" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button type="submit" loading={submitting} disabled={!idemKey.trim()}>
            Create job
          </Button>
        </div>
      </form>
    </Dialog>
  );
}
