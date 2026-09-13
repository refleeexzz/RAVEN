import {
  Activity,
  AlertTriangle,
  ArrowRight,
  Inbox,
  Layers,
  ListOrdered,
  LoaderCircle,
  Timer,
  Wifi,
  Zap,
} from "lucide-react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  type TooltipProps,
} from "recharts";
import { useConsole } from "@/lib/store";
import { fmtCompact, fmtMs, fmtNum, fmtPct, fmtTime } from "@/lib/utils";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/empty";
import { Dot } from "@/components/ui/badge";
import { StatCard } from "@/components/StatCard";
import { Sparkline } from "@/components/Sparkline";
import { StatusChip } from "@/components/StatusChip";
import type { Health } from "@/lib/types";

function ChartTip({ active, payload, label }: TooltipProps<number, string>) {
  if (!active || !payload || payload.length === 0) return null;
  return (
    <div className="rounded-md border border-border bg-surface px-3 py-2 text-xs shadow-md">
      <p className="text-muted">{fmtTime(typeof label === "number" ? label : String(label))}</p>
      <p className="mt-0.5 font-medium tabular-nums text-fg">
        {payload[0].value?.toFixed(1)} req/s
      </p>
    </div>
  );
}

function HealthTile({
  name,
  status,
  latency,
  detail,
}: {
  name: string;
  status: Health;
  latency: number | null;
  detail: string;
}) {
  const dot = status === "ok" ? "success" : status === "degraded" ? "warning" : "error";
  return (
    <div className="flex flex-col gap-1 rounded-md border border-border bg-bg p-3">
      <div className="flex items-center gap-2">
        <Dot variant={dot} pulse={status !== "ok"} />
        <span className="truncate text-sm font-medium text-fg">{name}</span>
      </div>
      <div className="flex items-baseline justify-between gap-2">
        <span className="text-xs capitalize text-muted">{status}</span>
        <span className="font-mono text-xs tabular-nums text-muted">{fmtMs(latency)}</span>
      </div>
      <span className="truncate text-xs text-subtle" title={detail}>
        {detail}
      </span>
    </div>
  );
}

export function OverviewPage() {
  const snapshot = useConsole((s) => s.snapshot);
  const history = useConsole((s) => s.history);
  const events = useConsole((s) => s.events);
  const mode = useConsole((s) => s.mode);

  if (!snapshot) {
    return (
      <div className="flex flex-col gap-6">
        <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
          {Array.from({ length: 6 }, (_, i) => (
            <Skeleton key={i} className="h-28" />
          ))}
        </div>
        <Skeleton className="h-72" />
        <Skeleton className="h-48" />
      </div>
    );
  }

  const { totals, services, series } = snapshot;
  const tail = series.slice(-90);
  const rpsSpark = series.map((p) => p.rps).slice(-60);
  const errSpark = series.map((p) => p.err * 100).slice(-60);
  const p99Spark = series.map((p) => p.p99).slice(-60);
  const queueSpark = history.map((h) => h.queue);
  const activeSpark = history.map((h) => h.active);
  const wsSpark = history.map((h) => h.ws ?? 0);
  const errHot = totals.err_rate > 0.02;

  return (
    <div className="flex flex-col gap-6">
      {/* Status cards */}
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-6">
        <StatCard label="Requests/sec" value={fmtNum(totals.rps)} icon={Zap} sub="gateway, all routes">
          <Sparkline data={rpsSpark} tone="accent" />
        </StatCard>
        <StatCard
          label="Error rate"
          value={<span className={errHot ? "text-error" : undefined}>{fmtPct(totals.err_rate)}</span>}
          icon={AlertTriangle}
          sub={errHot ? "above 2% threshold" : "5xx share of requests"}
        >
          <Sparkline data={errSpark} tone={errHot ? "error" : "success"} />
        </StatCard>
        <StatCard label="P99 latency" value={fmtMs(totals.p99_ms)} icon={Timer} sub="http request duration">
          <Sparkline data={p99Spark} tone="accent" />
        </StatCard>
        <StatCard label="Active jobs" value={fmtNum(totals.active_jobs)} icon={LoaderCircle} sub="processing right now">
          <Sparkline data={activeSpark} tone="accent" />
        </StatCard>
        <StatCard label="Queue depth" value={fmtNum(totals.queue_depth)} icon={ListOrdered} sub="queued + retrying">
          <Sparkline data={queueSpark} tone="warning" />
        </StatCard>
        <StatCard
          label="WS connections"
          value={totals.ws_connections === null ? "—" : fmtNum(totals.ws_connections)}
          icon={Wifi}
          sub={
            totals.ws_rooms === null
              ? mode === "live"
                ? "websocket service unreachable"
                : "realtime service"
              : `${fmtNum(totals.ws_rooms)} rooms · ${fmtNum(totals.ws_users ?? 0)} users`
          }
        >
          <Sparkline data={wsSpark} tone="success" />
        </StatCard>
      </div>

      {/* Throughput chart */}
      <Card>
        <CardHeader
          title="Gateway throughput"
          description="Requests per second · last 5 minutes"
          action={
            <span className="flex items-center gap-1.5 text-xs text-muted">
              <span className="h-2 w-2 rounded-full bg-accent" /> req/s
            </span>
          }
        />
        <CardContent>
          {tail.length < 2 ? (
            <EmptyState
              icon={Activity}
              title="No request metrics yet"
              description="The gateway does not expose a /metrics endpoint the console can read. Jobs, broker and websocket panels on this page still work."
            />
          ) : (
            <div className="h-56 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={tail} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                  <CartesianGrid stroke="#26262c" vertical={false} />
                  <XAxis
                    dataKey="t"
                    tickFormatter={(t) => fmtTime(t as number)}
                    tick={{ fill: "#71717a", fontSize: 12 }}
                    tickLine={false}
                    axisLine={false}
                    minTickGap={64}
                  />
                  <YAxis
                    tick={{ fill: "#71717a", fontSize: 12 }}
                    tickLine={false}
                    axisLine={false}
                    width={48}
                    tickFormatter={(v) => fmtCompact(v as number)}
                  />
                  <Tooltip content={<ChartTip />} cursor={{ stroke: "#3f3f46" }} />
                  <Area
                    type="monotone"
                    dataKey="rps"
                    stroke="#8b5cf6"
                    strokeWidth={2}
                    fill="#8b5cf6"
                    fillOpacity={0.12}
                    isAnimationActive={false}
                  />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          )}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-2">
        {/* Service health */}
        <Card>
          <CardHeader title="Service health" description="Gateway, core services, broker and realtime" />
          <CardContent>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
              {services.map((s) => (
                <HealthTile key={s.id} name={s.name} status={s.status} latency={s.latency_ms} detail={s.detail} />
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Recent job events */}
        <Card className="flex flex-col">
          <CardHeader
            title="Job events"
            description="Live feed · websocket room jobs"
            action={
              <a
                href="#/jobs"
                className="inline-flex h-6 items-center gap-1 rounded-md px-2 text-xs text-muted transition-colors duration-150 hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent"
              >
                View all <ArrowRight className="h-3 w-3" aria-hidden />
              </a>
            }
          />
          <CardContent className="flex-1 p-0">
            {events.length === 0 ? (
              <EmptyState
                icon={Inbox}
                title="No job events yet"
                description="Events appear here as jobs move through the queue. Create a job from the Jobs page to see the feed live."
              />
            ) : (
              <ul className="divide-y divide-border/60">
                {events.slice(0, 12).map((e, i) => (
                  <li key={`${e.job_id}-${e.at}-${i}`}>
                    <a
                      href={`#/jobs?job=${encodeURIComponent(e.job_id)}`}
                      className="flex items-center gap-3 px-4 py-2 transition-colors duration-150 hover:bg-surface-2/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent"
                    >
                      <span className="w-20 shrink-0 font-mono text-xs tabular-nums text-subtle">
                        {fmtTime(e.at)}
                      </span>
                      <span className="min-w-0 flex-1 truncate font-mono text-xs text-fg">{e.job_id}</span>
                      <StatusChip status={e.status} />
                      <span className="hidden w-24 truncate text-right font-mono text-xs text-muted sm:block">
                        {e.worker_id ?? "—"}
                      </span>
                    </a>
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Queue summary strip */}
      <Card>
        <CardContent className="flex flex-wrap items-center gap-x-8 gap-y-2 py-3">
          <span className="flex items-center gap-2 text-sm text-muted">
            <Layers className="h-4 w-4 text-subtle" aria-hidden />
            Topics: <span className="font-medium tabular-nums text-fg">{snapshot.topics.length}</span>
          </span>
          <span className="text-sm text-muted">
            Consumer lag:{" "}
            <span className="font-medium tabular-nums text-fg">
              {fmtNum(snapshot.topics.reduce((a, t) => a + t.consumer_groups.reduce((x, g) => x + g.lag, 0), 0))}
            </span>
          </span>
          <span className="text-sm text-muted">
            Workers:{" "}
            <span className="font-medium tabular-nums text-fg">{snapshot.workers.length}</span>
          </span>
          <a
            href="#/broker"
            className="ml-auto inline-flex items-center gap-1 text-sm text-accent-fg transition-colors duration-150 hover:text-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent rounded-sm"
          >
            Broker details <ArrowRight className="h-4 w-4" aria-hidden />
          </a>
        </CardContent>
      </Card>
    </div>
  );
}
