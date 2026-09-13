import { useCallback, useState } from "react";
import { FlaskConical, Trash2 } from "lucide-react";
import type { Route } from "@/lib/router";
import { useConsole } from "@/lib/store";
import { fmtTime, uuid } from "@/lib/utils";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { EmptyState } from "@/components/ui/empty";
import { BreakItYourself } from "@/features/testlab/BreakItYourself";
import { E2eCheck } from "@/features/testlab/E2eCheck";
import { FailOnPurpose } from "@/features/testlab/FailOnPurpose";
import { LoadTest } from "@/features/testlab/LoadTest";
import type { LogEntry } from "@/features/testlab/lab";

// Test Lab — drive the platform from the browser. Every scenario runs against
// the live gateway in live mode and against the in-memory simulator in demo
// mode, so the page is fully exercisable offline.
//
// `#/testlab?autorun=e2e|load|fail` starts a scenario on mount; it exists for
// headless smoke-testing (`msedge --headless --dump-dom`) but is harmless.

const OUTCOME_DOT = { ok: "success", warn: "warning", fail: "error" } as const;

export function TestLabPage({ route }: { route: Route }) {
  const mode = useConsole((s) => s.mode);
  const [entries, setEntries] = useState<LogEntry[]>([]);

  const addLog = useCallback((e: Omit<LogEntry, "id" | "at">) => {
    setEntries((prev) => [{ ...e, id: uuid(), at: Date.now() }, ...prev].slice(0, 100));
  }, []);

  const autorun = route.query.get("autorun") ?? "";

  return (
    <div className="flex flex-col gap-6">
      <p className="text-sm text-muted">
        Exercise the platform yourself.{" "}
        {mode === "demo"
          ? "Demo mode: scenarios run against the in-memory simulator — the same flows, no cluster."
          : "Live mode: scenarios hit the real gateway on localhost:8080."}
      </p>

      <div className="grid grid-cols-1 items-start gap-4 lg:grid-cols-2">
        <E2eCheck log={addLog} autoRun={autorun === "e2e"} />
        <LoadTest log={addLog} autoRun={autorun === "load"} />
        <FailOnPurpose log={addLog} autoRun={autorun === "fail"} />
        <BreakItYourself />
      </div>

      <Card>
        <CardHeader
          title="Run log"
          description="Every scenario run, newest first. Kept for the life of this page."
          action={
            entries.length > 0 ? (
              <Button variant="ghost" size="sm" onClick={() => setEntries([])}>
                <Trash2 className="h-3.5 w-3.5" aria-hidden /> Clear
              </Button>
            ) : undefined
          }
        />
        {entries.length === 0 ? (
          <EmptyState
            icon={FlaskConical}
            title="No runs yet"
            description="Pick a scenario above — results land here."
          />
        ) : (
          <CardContent className="p-0">
            <ol className="flex flex-col font-mono text-xs" aria-label="Run log">
              {entries.map((e) => (
                <li
                  key={e.id}
                  className="flex items-center gap-3 border-b border-border/60 px-4 py-1.5 last:border-b-0"
                >
                  <span className="shrink-0 tabular-nums text-subtle">{fmtTime(e.at)}</span>
                  <Dot variant={OUTCOME_DOT[e.outcome]} />
                  <span className="shrink-0 text-fg">{e.scenario}</span>
                  <span className="min-w-0 flex-1 truncate text-muted">{e.detail}</span>
                  <Badge variant={e.outcome === "ok" ? "success" : e.outcome === "warn" ? "warning" : "error"}>
                    {e.outcome}
                  </Badge>
                </li>
              ))}
            </ol>
          </CardContent>
        )}
      </Card>
    </div>
  );
}
