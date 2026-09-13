import { useEffect, useMemo, useState } from "react";
import {
  Ban,
  ChevronLeft,
  ChevronRight,
  Layers,
  Plus,
  RotateCcw,
  Search,
} from "lucide-react";
import { replaceHash, type Route } from "@/lib/router";
import { useConsole } from "@/lib/store";
import { useNow } from "@/lib/useNow";
import { toast } from "@/lib/toast";
import { cn, relTime, shortId } from "@/lib/utils";
import {
  JOB_STATUSES,
  JOB_TYPE_META,
  type Job,
  type JobStatus,
  type JobType,
  type JobsQuery,
} from "@/lib/types";
import { buttonClasses } from "@/components/ui/button-styles";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Table, TBody, TD, TH, THead, TR } from "@/components/ui/table";
import { TableSkeleton } from "@/components/ui/skeleton";
import { EmptyState, ErrorState } from "@/components/ui/empty";
import { StatusChip, STATUS_META } from "@/components/StatusChip";
import { JobDrawer } from "@/features/jobs/JobDrawer";
import { CreateJobDialog } from "@/features/jobs/CreateJobDialog";

const PAGE_SIZE = 15;

function isJobStatus(s: string): s is JobStatus {
  return (JOB_STATUSES as string[]).includes(s);
}

function buildQuery(route: Route): JobsQuery {
  const q = route.query;
  const statuses = (q.get("status")?.split(",") ?? []).filter(isJobStatus);
  const t = q.get("type") ?? "all";
  const type: JobType | "all" =
    t === "send_email" || t === "resize_image" || t === "webhook" ? t : "all";
  return {
    statuses,
    type,
    search: q.get("q") ?? "",
    page: Math.max(1, Number(q.get("page") ?? "1") || 1),
    pageSize: PAGE_SIZE,
    dlq: q.get("tab") === "dlq",
  };
}

function hrefFor(route: Route, updates: Record<string, string | null>): string {
  const params = new URLSearchParams(route.query);
  for (const [k, v] of Object.entries(updates)) {
    if (v === null || v === "") params.delete(k);
    else params.set(k, v);
  }
  const qs = params.toString();
  return `#/jobs${qs ? `?${qs}` : ""}`;
}

export function JobsPage({ route }: { route: Route }) {
  const jobsPage = useConsole((s) => s.jobsPage);
  const loadJobs = useConsole((s) => s.loadJobs);
  const cancelJob = useConsole((s) => s.cancelJob);
  const requeueJob = useConsole((s) => s.requeueJob);
  const setCreateOpen = useConsole((s) => s.setCreateJobOpen);
  const now = useNow(1000);

  const query = useMemo(() => buildQuery(route), [route]);
  const tab = query.dlq ? "dlq" : "all";
  const selectedJobId = route.query.get("job");
  const mode = useConsole((s) => s.mode);

  useEffect(() => {
    if (mode === "connecting") return; // wait for the data source
    void loadJobs(query);
    const id = setInterval(() => void loadJobs(query, { silent: true }), 3000);
    return () => clearInterval(id);
  }, [query, loadJobs, mode]);

  // Search stays in local state while typing, then replaces the URL hash.
  const [searchInput, setSearchInput] = useState(query.search);
  useEffect(() => setSearchInput(query.search), [query.search]);
  useEffect(() => {
    if (searchInput === query.search) return;
    const id = setTimeout(() => {
      replaceHash(hrefFor(route, { q: searchInput || null, page: null }));
    }, 300);
    return () => clearTimeout(id);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchInput]);

  const toggleStatus = (s: JobStatus) => {
    const next = query.statuses.includes(s)
      ? query.statuses.filter((x) => x !== s)
      : [...query.statuses, s];
    route.navigate(hrefFor(route, { status: next.join(",") || null, page: null }));
  };

  const onCancel = async (job: Job) => {
    try {
      await cancelJob(job.id);
      toast({ variant: "success", title: "Job cancelled", description: job.id });
    } catch (err) {
      toast({
        variant: "error",
        title: "Cancel failed",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
    void loadJobs(query, { silent: true });
  };

  const onRequeue = async (job: Job) => {
    try {
      await requeueJob(job.id);
      toast({ variant: "success", title: "Job requeued", description: job.id });
    } catch (err) {
      toast({
        variant: "error",
        title: "Requeue failed",
        description: err instanceof Error ? err.message : "Unknown error",
      });
    }
    void loadJobs(query, { silent: true });
  };

  const totalPages = Math.max(1, Math.ceil(jobsPage.total / PAGE_SIZE));
  const hasFilters = query.statuses.length > 0 || query.type !== "all" || query.search !== "";
  const openJob = (id: string) => route.navigate(hrefFor(route, { job: id }));

  return (
    <div className="flex flex-col gap-4">
      <div className="flex flex-wrap items-center gap-2">
        <Tabs value={tab} onValueChange={(v) => route.navigate(hrefFor(route, { tab: v === "dlq" ? "dlq" : null, page: null, status: null }))}>
          <TabsList>
            <TabsTrigger value="all">Jobs</TabsTrigger>
            <TabsTrigger value="dlq">Dead letter queue</TabsTrigger>
          </TabsList>
        </Tabs>
        <div className="ml-auto flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => void loadJobs(query)} aria-label="Refresh jobs">
            <RotateCcw className="h-3.5 w-3.5" aria-hidden />
            Refresh
          </Button>
          <Button size="sm" onClick={() => setCreateOpen(true)}>
            <Plus className="h-4 w-4" aria-hidden />
            Create job
          </Button>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2">
        {(query.dlq ? (["DEAD"] as JobStatus[]) : JOB_STATUSES.filter((s) => s !== "DEAD")).map((s) => {
          const active = query.statuses.includes(s);
          return (
            <button
              key={s}
              type="button"
              aria-pressed={active}
              onClick={() => toggleStatus(s)}
              className={cn(
                "inline-flex h-6 items-center gap-1.5 rounded-sm border px-2 text-xs font-medium transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent",
                active
                  ? "border-accent/60 bg-accent/10 text-accent-fg"
                  : "border-border text-muted hover:bg-surface-2 hover:text-fg",
              )}
            >
              {STATUS_META[s].label}
            </button>
          );
        })}
        <span className="mx-1 hidden h-4 w-px bg-border sm:block" aria-hidden />
        <Select
          aria-label="Filter by job type"
          value={query.type}
          onChange={(e) =>
            route.navigate(hrefFor(route, { type: e.target.value === "all" ? null : e.target.value, page: null }))
          }
        >
          <option value="all">All types</option>
          {(Object.keys(JOB_TYPE_META) as JobType[]).map((t) => (
            <option key={t} value={t}>
              {JOB_TYPE_META[t].label}
            </option>
          ))}
        </Select>
        <div className="relative">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-4 w-4 -translate-y-1/2 text-subtle" aria-hidden />
          <Input
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            placeholder="Filter by job or worker id"
            aria-label="Filter by job or worker id"
            className="w-56 pl-8"
          />
        </div>
      </div>

      {/* Table */}
      <div className="rounded-lg border border-border bg-surface">
        {jobsPage.status === "loading" && jobsPage.items.length === 0 ? (
          <TableSkeleton rows={10} cols={tab === "dlq" ? 7 : 7} />
        ) : jobsPage.status === "error" && jobsPage.items.length === 0 ? (
          <ErrorState message={jobsPage.message} onRetry={() => void loadJobs(query)} />
        ) : jobsPage.items.length === 0 ? (
          <EmptyState
            icon={Layers}
            title={tab === "dlq" ? "Dead letter queue is empty" : "No jobs match"}
            description={
              tab === "dlq"
                ? "Jobs land here after exhausting all retries. Nothing is dead right now — the retry pipeline is holding up."
                : hasFilters
                  ? "No jobs match the current filters. Clear them to see everything in the queue."
                  : "The queue is empty. Create a job to put the workers to work."
            }
            action={
              hasFilters ? (
                <Button variant="secondary" size="sm" onClick={() => route.navigate("#/jobs")}>
                  Clear filters
                </Button>
              ) : (
                <Button size="sm" onClick={() => setCreateOpen(true)}>
                  <Plus className="h-4 w-4" aria-hidden />
                  Create job
                </Button>
              )
            }
          />
        ) : (
          <Table>
            <THead>
              <tr>
                <TH>Job</TH>
                <TH>Type</TH>
                <TH>Status</TH>
                <TH className="w-16">Pri</TH>
                <TH className="w-20">Attempts</TH>
                <TH>Worker</TH>
                {tab === "dlq" && <TH>Error</TH>}
                <TH className="w-24">Created</TH>
                <TH className="w-24 text-right">Actions</TH>
              </tr>
            </THead>
            <TBody>
              {jobsPage.items.map((job) => (
                <TR
                  key={job.id}
                  tabIndex={0}
                  role="link"
                  aria-label={`Open job ${job.id}`}
                  className="cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent"
                  onClick={() => openJob(job.id)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") openJob(job.id);
                  }}
                >
                  <TD>
                    <span className="font-mono text-xs text-fg" title={job.id}>
                      {shortId(job.id)}
                    </span>
                  </TD>
                  <TD className="whitespace-nowrap text-muted">{JOB_TYPE_META[job.type].label}</TD>
                  <TD>
                    <StatusChip status={job.status} />
                  </TD>
                  <TD>
                    <span
                      className={cn(
                        "font-mono text-xs tabular-nums",
                        job.priority >= 8 ? "text-warning" : "text-muted",
                      )}
                    >
                      {job.priority}
                    </span>
                  </TD>
                  <TD className="font-mono text-xs tabular-nums text-muted">
                    {job.attempts}/{job.max_attempts}
                  </TD>
                  <TD className="font-mono text-xs text-muted">{job.worker_id ?? "—"}</TD>
                  {tab === "dlq" && (
                    <TD className="max-w-56">
                      <span className="block truncate font-mono text-xs text-error" title={job.error ?? ""}>
                        {job.error ?? "—"}
                      </span>
                    </TD>
                  )}
                  <TD className="whitespace-nowrap text-xs tabular-nums text-muted">
                    {relTime(job.created_at, now)}
                  </TD>
                  <TD className="text-right">
                    {tab === "dlq" ? (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={(e) => {
                          e.stopPropagation();
                          void onRequeue(job);
                        }}
                        aria-label={`Requeue job ${job.id}`}
                      >
                        <RotateCcw className="h-3.5 w-3.5" aria-hidden />
                        Requeue
                      </Button>
                    ) : (
                      (job.status === "QUEUED" || job.status === "RETRYING") && (
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={(e) => {
                            e.stopPropagation();
                            void onCancel(job);
                          }}
                          aria-label={`Cancel job ${job.id}`}
                        >
                          <Ban className="h-3.5 w-3.5" aria-hidden />
                          Cancel
                        </Button>
                      )
                    )}
                  </TD>
                </TR>
              ))}
            </TBody>
          </Table>
        )}

        {/* Pagination */}
        <div className="flex items-center justify-between gap-4 border-t border-border px-3 py-2">
          <span className="text-xs text-muted">
            {jobsPage.total === 0
              ? "0 jobs"
              : `${(query.page - 1) * PAGE_SIZE + 1}–${Math.min(query.page * PAGE_SIZE, jobsPage.total)} of ${jobsPage.total} jobs`}
          </span>
          <div className="flex items-center gap-1">
            <a
              className={cn(
                buttonClasses("secondary", "icon-sm"),
                query.page <= 1 && "pointer-events-none opacity-40",
              )}
              href={hrefFor(route, { page: query.page > 1 ? String(query.page - 1) : null })}
              aria-label="Previous page"
            >
              <ChevronLeft className="h-4 w-4" />
            </a>
            <span className="px-2 text-xs tabular-nums text-muted">
              Page {query.page} of {totalPages}
            </span>
            <a
              className={cn(
                buttonClasses("secondary", "icon-sm"),
                query.page >= totalPages && "pointer-events-none opacity-40",
              )}
              href={hrefFor(route, { page: query.page < totalPages ? String(query.page + 1) : null })}
              aria-label="Next page"
            >
              <ChevronRight className="h-4 w-4" />
            </a>
          </div>
        </div>
      </div>

      <JobDrawer jobId={selectedJobId} onClose={() => route.navigate(hrefFor(route, { job: null }))} />
      <CreateJobDialog onCreated={() => void loadJobs(query, { silent: true })} />
    </div>
  );
}
