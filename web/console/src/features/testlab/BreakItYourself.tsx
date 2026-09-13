import { useState } from "react";
import { Check, Copy, ExternalLink, Hammer, Info } from "lucide-react";
import { config } from "@/lib/config";
import { toast } from "@/lib/toast";
import { buttonClasses } from "@/components/ui/button-styles";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { copyText } from "./lab";

// Scenario 4 — Break it yourself: the honest card. Real chaos needs kubectl,
// which a browser page can't drive, so this is copy-paste guidance plus the
// links where the effects show up.

const COMMANDS: Array<{ label: string; command: string; watch: string }> = [
  {
    label: "Kill every worker mid-load",
    command: "kubectl -n raven delete pod -l app=worker --force --grace-period=0",
    watch: "Run a mini load test first, then fire this. Kubernetes restarts the pods, workers re-join, and in-flight jobs get retried — watch the Workers page and the load-test counters recover.",
  },
  {
    label: "Scale the worker pool",
    command: "kubectl -n raven scale deployment worker --replicas=10",
    watch: "Run the load test again after scaling — throughput (jobs/s) should climb. Watch the worker count on the Workers page.",
  },
];

const LINKS: Array<{ label: string; url: string; hint: string }> = [
  {
    label: "Jaeger",
    url: config.jaegerUrl,
    hint: "Traces: gateway → jobs service → broker → worker, span by span.",
  },
  {
    label: "Grafana",
    url: config.grafanaUrl,
    hint: "Metrics dashboards: request rates, queue depth, worker saturation.",
  },
];

function CommandRow({ label, command, watch }: (typeof COMMANDS)[number]) {
  const [copied, setCopied] = useState(false);
  const copy = async () => {
    const ok = await copyText(command);
    if (ok) {
      setCopied(true);
      toast({ variant: "success", title: "Copied to clipboard" });
      setTimeout(() => setCopied(false), 1500);
    } else {
      toast({ variant: "error", title: "Copy failed", description: "Select the command and copy it manually." });
    }
  };
  return (
    <div className="flex flex-col gap-1.5">
      <p className="text-sm font-medium text-fg">{label}</p>
      <div className="flex items-center gap-2">
        <code className="min-w-0 flex-1 overflow-x-auto whitespace-nowrap rounded-md border border-border bg-bg px-3 py-1.5 font-mono text-xs text-fg">
          {command}
        </code>
        <Button variant="secondary" size="icon" aria-label={`Copy: ${label}`} onClick={() => void copy()}>
          {copied ? <Check className="h-4 w-4 text-success" /> : <Copy className="h-4 w-4" />}
        </Button>
      </div>
      <p className="text-xs text-muted">{watch}</p>
    </div>
  );
}

export function BreakItYourself() {
  return (
    <Card>
      <CardHeader
        title="Break it yourself"
        description="Real chaos needs kubectl access, so it can't be a browser button — copy these and watch the effects live."
        action={<Hammer className="h-4 w-4 text-subtle" aria-hidden />}
      />
      <CardContent className="flex flex-col gap-4">
        {COMMANDS.map((c) => (
          <CommandRow key={c.command} {...c} />
        ))}

        <div className="flex flex-col gap-1.5">
          <p className="text-sm font-medium text-fg">Watch the blast radius</p>
          <div className="flex flex-wrap gap-2">
            {LINKS.map((l) => (
              <a
                key={l.label}
                href={l.url}
                target="_blank"
                rel="noopener noreferrer"
                className={buttonClasses("secondary", "sm")}
                title={l.hint}
              >
                {l.label} <ExternalLink className="h-3.5 w-3.5" aria-hidden />
              </a>
            ))}
          </div>
          <p className="text-xs text-muted">
            {LINKS.map((l) => `${l.label}: ${l.hint}`).join(" ")}
          </p>
        </div>

        <p className="flex items-start gap-2 rounded-md border border-border bg-surface-2/50 px-3 py-2 text-xs text-muted">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0 text-subtle" aria-hidden />
          Chaos actions need kubectl access to the cluster. The browser can only drive the REST API,
          so this card is a guide, not a button.
        </p>
      </CardContent>
    </Card>
  );
}
