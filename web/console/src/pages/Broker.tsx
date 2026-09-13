import { useMemo } from "react";
import { GitBranch, Layers3, Waypoints } from "lucide-react";
import type { Route } from "@/lib/router";
import { useConsole } from "@/lib/store";
import { cn, fmtNum } from "@/lib/utils";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { EmptyState } from "@/components/ui/empty";
import { Table, TBody, TD, TH, THead, TR } from "@/components/ui/table";
import { Sparkline } from "@/components/Sparkline";
import type { TopicInfo } from "@/lib/types";

function topicLag(t: TopicInfo): number {
  return t.consumer_groups.reduce((a, g) => a + g.lag, 0);
}

function topicHwm(t: TopicInfo): number {
  return t.partitions.reduce((a, p) => a + p.high_watermark, 0);
}

function TopicDetail({ topic }: { topic: TopicInfo }) {
  const hwmTotal = topicHwm(topic);
  const lagTotal = topicLag(topic);
  const maxHwm = Math.max(...topic.partitions.map((p) => p.high_watermark), 1);
  const rateNow = topic.rate[topic.rate.length - 1] ?? 0;

  return (
    <div className="flex min-w-0 flex-col gap-4">
      <Card>
        <CardHeader
          title={<span className="font-mono">{topic.name}</span>}
          description={`${topic.partitions.length} partitions · ${topic.consumer_groups.length} consumer groups`}
          action={
            <Badge variant={lagTotal === 0 ? "success" : lagTotal < 500 ? "warning" : "error"}>
              {lagTotal === 0 ? "caught up" : `${fmtNum(lagTotal)} lag`}
            </Badge>
          }
        />
        <CardContent className="grid grid-cols-3 gap-4">
          <div>
            <p className="text-xs uppercase tracking-wide text-muted">High-water mark</p>
            <p className="mt-1 text-xl font-semibold tabular-nums text-fg">{fmtNum(hwmTotal)}</p>
          </div>
          <div>
            <p className="text-xs uppercase tracking-wide text-muted">Total lag</p>
            <p className="mt-1 text-xl font-semibold tabular-nums text-fg">{fmtNum(lagTotal)}</p>
          </div>
          <div>
            <p className="text-xs uppercase tracking-wide text-muted">Messages/sec</p>
            <p className="mt-1 text-xl font-semibold tabular-nums text-fg">{rateNow.toFixed(1)}</p>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Throughput" description="Messages produced per second · last 90s" />
        <CardContent>
          {topic.rate.length < 2 ? (
            <EmptyState
              icon={Waypoints}
              title="Building rate history"
              description="Message rates are computed from high-water mark growth. Give it a few seconds of polling."
            />
          ) : (
            <Sparkline data={topic.rate} tone="accent" className="h-16" />
          )}
        </CardContent>
      </Card>

      <Card>
        <CardHeader title="Partitions" description="High-water mark per partition" />
        <Table>
          <THead>
            <tr>
              <TH className="w-24">Partition</TH>
              <TH>Share</TH>
              <TH className="w-40 text-right">High-water mark</TH>
            </tr>
          </THead>
          <TBody>
            {topic.partitions.map((p) => (
              <TR key={p.id}>
                <TD className="font-mono text-xs text-muted">p{p.id}</TD>
                <TD>
                  <Progress
                    value={p.high_watermark}
                    max={maxHwm}
                    label={`Partition ${p.id} high-water mark`}
                  />
                </TD>
                <TD className="text-right font-mono text-sm tabular-nums text-fg">
                  {fmtNum(p.high_watermark)}
                </TD>
              </TR>
            ))}
          </TBody>
        </Table>
      </Card>

      <Card>
        <CardHeader title="Consumer groups" description="Committed offsets vs high-water mark" />
        <CardContent className="flex flex-col gap-4">
          {topic.consumer_groups.length === 0 && (
            <p className="text-sm text-muted">No consumer groups registered on this topic.</p>
          )}
          {topic.consumer_groups.map((g) => {
            const committedTotal = g.committed.reduce((a, c) => a + c, 0);
            const tone = g.lag === 0 ? "success" : g.lag < 500 ? "warning" : "error";
            return (
              <div key={g.name}>
                <div className="mb-1 flex items-center justify-between gap-2">
                  <span className="flex items-center gap-2 font-mono text-sm text-fg">
                    <GitBranch className="h-4 w-4 text-subtle" aria-hidden />
                    {g.name}
                  </span>
                  <span className="text-xs tabular-nums text-muted">
                    {fmtNum(committedTotal)} / {fmtNum(hwmTotal)} ·{" "}
                    <span className={g.lag > 0 ? "text-warning" : "text-success"}>
                      {fmtNum(g.lag)} behind
                    </span>
                  </span>
                </div>
                <Progress value={committedTotal} max={hwmTotal || 1} tone={tone} label={`${g.name} consumption`} />
              </div>
            );
          })}
        </CardContent>
      </Card>
    </div>
  );
}

export function BrokerPage({ route }: { route: Route }) {
  const snapshot = useConsole((s) => s.snapshot);

  const topics = useMemo(() => snapshot?.topics ?? [], [snapshot]);
  const selectedName = route.query.get("topic");
  const selected = topics.find((t) => t.name === selectedName) ?? topics[0];

  if (!snapshot) {
    return (
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-[320px_1fr]">
        <Skeleton className="h-80" />
        <Skeleton className="h-80" />
      </div>
    );
  }

  if (topics.length === 0) {
    return (
      <Card>
        <EmptyState
          icon={Layers3}
          title="No topics reported"
          description="The broker stats endpoint (localhost:9101/topics) did not return any topics. Check that the broker is running, or use demo mode."
        />
      </Card>
    );
  }

  return (
    <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-[320px_1fr]">
      <Card className="p-2">
        <p className="px-2 pb-2 pt-1 text-xs font-medium uppercase tracking-wide text-muted">Topics</p>
        <ul className="flex flex-col gap-1">
          {topics.map((t) => {
            const active = selected?.name === t.name;
            const lag = topicLag(t);
            const rate = t.rate[t.rate.length - 1];
            return (
              <li key={t.name}>
                <a
                  href={`#/broker?topic=${encodeURIComponent(t.name)}`}
                  aria-current={active ? "true" : undefined}
                  className={cn(
                    "flex items-center gap-3 rounded-md px-2 py-2 transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent",
                    active ? "bg-surface-2" : "hover:bg-surface-2/60",
                  )}
                >
                  <span className="min-w-0 flex-1">
                    <span className={cn("block truncate font-mono text-sm", active ? "text-fg" : "text-muted")}>
                      {t.name}
                    </span>
                    <span className="block text-xs text-subtle">
                      {t.partitions.length} partitions · {fmtNum(lag)} lag
                    </span>
                  </span>
                  <span className="text-right">
                    <span className="block font-mono text-xs tabular-nums text-fg">
                      {rate === undefined ? "—" : rate.toFixed(1)}
                    </span>
                    <span className="block text-xs text-subtle">msg/s</span>
                  </span>
                </a>
              </li>
            );
          })}
        </ul>
      </Card>

      {selected && <TopicDetail key={selected.name} topic={selected} />}
    </div>
  );
}
