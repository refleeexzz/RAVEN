-- Migration 000004: scheduling — delayed jobs, replay audit and cron.
--
-- Owned by the jobs service. Three additions:
--
--   1. Delayed jobs: jobs.scheduled_at plus a new SCHEDULED status. A job
--      created with scheduled_at in the future sits in SCHEDULED (never
--      published to the broker) until the jobs-service dispatcher flips it
--      to QUEUED and publishes it when the time comes. The sweeper is not
--      involved: SCHEDULED jobs hold no lease.
--
--   2. Replay: jobs.replayed_from records the source job id when a job was
--      created by POST /api/jobs/{id}/replay — the audit link between the
--      copy and the original.
--
--   3. Cron: cron_schedules holds one row per recurring schedule. The
--      jobs-service cron loop advances next_run_at and inserts the job
--      instance in the SAME transaction, so a restart (or a second
--      replica) can never double-fire a schedule.

-- 1a. SCHEDULED joins the status enum. The constraint was created inline in
-- migration 000002, so it carries Postgres' default name.
ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCESS', 'FAILED',
                      'RETRYING', 'CANCELLED', 'DEAD', 'SCHEDULED'));

-- 1b + 2. New nullable columns: existing rows need no rewrite.
ALTER TABLE jobs
    ADD COLUMN scheduled_at timestamptz,   -- set only on SCHEDULED rows
    ADD COLUMN replayed_from text;         -- source job id when created by replay

-- Dispatcher scan: "due scheduled work, oldest first". Partial so only the
-- (small) SCHEDULED backlog lives in the index.
CREATE INDEX idx_jobs_scheduled_due
    ON jobs (scheduled_at) WHERE status = 'SCHEDULED';

-- 3. Cron schedules. cron_expr is the classic 5-field form
-- "min hour day-of-month month day-of-week" (numbers, '*', ',', '-', '/').
CREATE TABLE cron_schedules (
    id          text PRIMARY KEY,            -- 'cron_' + uuid
    owner_id    uuid,                        -- user who created it (NULL = system)
    name        text NOT NULL,
    cron_expr   text NOT NULL,
    type        text NOT NULL,               -- job type to spawn, e.g. send_email
    payload     jsonb NOT NULL,              -- payload of every spawned job
    priority    int NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 9),
    enabled     boolean NOT NULL DEFAULT true,
    next_run_at timestamptz NOT NULL,        -- next fire time (UTC)
    last_run_at timestamptz,                 -- last time an instance was spawned
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Scheduler scan: "enabled schedules that are due". Partial, same idea as
-- idx_jobs_scheduled_due.
CREATE INDEX idx_cron_schedules_due
    ON cron_schedules (next_run_at) WHERE enabled;

-- Owner-scoped listing (JOBS-02): users see only their own schedules.
CREATE INDEX idx_cron_schedules_owner
    ON cron_schedules (owner_id);
