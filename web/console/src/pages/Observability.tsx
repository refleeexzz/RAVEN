import { useEffect, useState } from "react";
import {
  Database,
  ExternalLink,
  Gauge,
  RefreshCw,
  Satellite,
  Waypoints,
  Workflow,
  type LucideIcon,
} from "lucide-react";
import { config } from "@/lib/config";
import { formatMetricValue } from "@/lib/metrics";
import { useConsole } from "@/lib/store";
import { useNow } from "@/lib/useNow";
import { fmtNum, relTime } from "@/lib/utils";
import type { MetricSample } from "@/lib/types";
import { buttonClasses } from "@/components/ui/button-styles";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge, Dot } from "@/components/ui/badge";
import { Skeleton, TableSkeleton } from "@/components/ui/skeleton";
import { ErrorState } from "@/components/ui/empty";
import { Table, TBody, TD, TH, THead, TR } from "@/components/ui/table";

type MetricsState =
  | { status: "loading" }
  | { status: "error"; message: string }
  | { status: "ready"; samples: MetricSample[]; at: number };

/** Opaque reachability probe — a resolved no-cors fetch means the host is up. */
function useReachable(url: string): boolean | null {
  const [state, setState] = useState<boolean | null>(null);
  useEffect(() => {
    let cancelled = false;
    const check = async () => {
      try {
        await fetch(url, { mode: "no-cors", signal: AbortSignal.timeout(2500) });
        if (!cancelled) setState(true);
      } catch {
        if (!cancelled) setState(false);
      }
    };
    void check();
    const id = setInterval(() => void check(), 10000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [url]);
  return state;
}

const TOOLS: Array<{
  name: string;
  url: string;
  description: string;
  icon: LucideIcon;
}> = [
  {
    name: "Grafana",
    url: config.grafanaUrl,
    description: "Dashboards for gateway, broker and worker metrics.",
    icon: Gauge,
  },
  {
    name: "Jaeger",
    url: config.jaegerUrl,
    description: "Distributed traces across gateway, services and workers.",
    icon: Workflow,
  },
  {
    name: "Prometheus",
    url: config.prometheusUrl,
    description: "Raw metrics, targets and alerting rules.",
    icon: Database,
  },
];

function ToolCard({ tool }: { tool: (typeof TOOLS)[number] }) {
  const reachable = useReachable(tool.url);
  return (
    <Card className="flex flex-col p-4">
      <div className="flex items-center justify-between gap-2">
        <span className="flex items-center gap-2">
          <tool.icon className="h-4 w-4 text-subtle" aria-hidden />
          <span className="text-sm font-semibold text-fg">{tool.name}</span>
        </span>
        {reachable === null ? (
          <Badge variant="outline">
            <Dot variant="muted" pulse /> probing
          </Badge>
        ) : reachable ? (
          <Badge variant="success">
            <Dot variant="success" /> reachable
          </Badge>
        ) : (
          <Badge variant="error">
            <Dot variant="error" /> unreachable
          </Badge>
        )}
      </div>
      <p className="mt-1 font-mono text-xs text-subtle">{tool.url.replace(/^https?:\/\//, "")}</p>
      <p className="mt-2 flex-1 text-sm text-muted">{tool.description}</p>
      <div className="mt-3">
        <a
          href={tool.url}
          target="_blank"
          rel="noopener noreferrer"
          className={buttonClasses("secondary", "sm")}
        >
          Open <ExternalLink className="h-3.5 w-3.5" aria-hidden />
        </a>
      </div>
    </Card>
  );
}

export function ObservabilityPage() {
  const snapshot = useConsole((s) => s.snapshot);
  const mode = useConsole((s) => s.mode);
  const metricsSummary = useConsole((s) => s.metricsSummary);
  const now = useNow(1000);
  const [metrics, setMetrics] = useState<MetricsState>({ status: "loading" });

  useEffect(() => {
    let cancelled = false;
    const load = async (first: boolean) => {
      if (first) setMetrics({ status: "loading" });
      try {
        const samples = await metricsSummary();
        if (!cancelled) setMetrics({ status: "ready", samples, at: Date.now() });
      } catch (err) {
        if (!cancelled) {
          setMetrics({ status: "error", message: err instanceof Error ? err.message : "Failed to load metrics" });
        }
      }
    };
    void load(true);
    const id = setInterval(() => void load(false), 5000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, [metricsSummary]);

  const totals = snapshot?.totals;
  const topics = snapshot?.topics ?? [];
  const lagTotal = topics.reduce((a, t) => a + t.consumer_groups.reduce((x, g) => x + g.lag, 0), 0);

  return (
    <div className="flex flex-col gap-6">
      {/* External tools */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
        {TOOLS.map((t) => (
          <ToolCard key={t.name} tool={t} />
        ))}
      </div>

      {/* Gateway metrics */}
      <Card>
        <CardHeader
          title="Gateway metrics"
          description={
            mode === "demo"
              ? "Parsed from the simulated /metrics exposition"
              : "Parsed from GET /metrics (Prometheus text format)"
          }
          action={
            <span className="flex items-center gap-2 text-xs text-muted">
              {metrics.status === "ready" && <span>updated {relTime(metrics.at, now)}</span>}
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label="Refresh metrics"
                onClick={() => {
                  setMetrics({ status: "loading" });
                  void metricsSummary()
                    .then((samples) => setMetrics({ status: "ready", samples, at: Date.now() }))
                    .catch((err: unknown) =>
                      setMetrics({
                        status: "error",
                        message: err instanceof Error ? err.message : "Failed to load metrics",
                      }),
                    );
                }}
              >
                <RefreshCw className="h-3.5 w-3.5" />
              </Button>
            </span>
          }
        />
        {metrics.status === "loading" ? (
          <TableSkeleton rows={6} cols={2} />
        ) : metrics.status === "error" ? (
          <ErrorState
            message={`${metrics.message}. Full metrics are available in Prometheus when the stack is up.`}
            onRetry={() => {
              setMetrics({ status: "loading" });
              void metricsSummary()
                .then((samples) => setMetrics({ status: "ready", samples, at: Date.now() }))
                .catch((err: unknown) =>
                  setMetrics({
                    status: "error",
                    message: err instanceof Error ? err.message : "Failed to load metrics",
                  }),
                );
            }}
          />
        ) : (
          <Table>
            <THead>
              <tr>
                <TH>Series</TH>
                <TH className="w-40 text-right">Value</TH>
              </tr>
            </THead>
            <TBody>
              {metrics.samples.map((s, i) => (
                <TR key={`${s.name}-${i}`}>
                  <TD>
                    <span className="font-mono text-xs text-fg">{s.name}</span>
                    {Object.keys(s.labels).length > 0 && (
                      <span className="ml-2 font-mono text-xs text-subtle">
                        {Object.entries(s.labels)
                          .map(([k, v]) => `${k}="${v}"`)
                          .join(" ")}
                      </span>
                    )}
                  </TD>
                  <TD className="text-right font-mono text-sm tabular-nums text-fg">
                    {formatMetricValue(s.value)}
                  </TD>
                </TR>
              ))}
            </TBody>
          </Table>
        )}
      </Card>

      {/* Platform stat panels */}
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <Card>
          <CardHeader
            title="Realtime service"
            description="GET :8084/debug/stats"
            action={<Satellite className="h-4 w-4 text-subtle" aria-hidden />}
          />
          <CardContent className="grid grid-cols-3 gap-4">
            {!totals ? (
              <>
                <Skeleton className="h-12" />
                <Skeleton className="h-12" />
                <Skeleton className="h-12" />
              </>
            ) : (
              <>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted">Connections</p>
                  <p className="mt-1 text-xl font-semibold tabular-nums text-fg">
                    {totals.ws_connections === null ? "—" : fmtNum(totals.ws_connections)}
                  </p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted">Rooms</p>
                  <p className="mt-1 text-xl font-semibold tabular-nums text-fg">
                    {totals.ws_rooms === null ? "—" : fmtNum(totals.ws_rooms)}
                  </p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-muted">Users</p>
                  <p className="mt-1 text-xl font-semibold tabular-nums text-fg">
                    {totals.ws_users === null ? "—" : fmtNum(totals.ws_users)}
                  </p>
                </div>
              </>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader
            title="Broker"
            description="GET :9101/topics"
            action={<Waypoints className="h-4 w-4 text-subtle" aria-hidden />}
          />
          <CardContent className="grid grid-cols-3 gap-4">
            <div>
              <p className="text-xs uppercase tracking-wide text-muted">Topics</p>
              <p className="mt-1 text-xl font-semibold tabular-nums text-fg">{topics.length}</p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-wide text-muted">Partitions</p>
              <p className="mt-1 text-xl font-semibold tabular-nums text-fg">
                {topics.reduce((a, t) => a + t.partitions.length, 0)}
              </p>
            </div>
            <div>
              <p className="text-xs uppercase tracking-wide text-muted">Consumer lag</p>
              <p className="mt-1 text-xl font-semibold tabular-nums text-fg">{fmtNum(lagTotal)}</p>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
