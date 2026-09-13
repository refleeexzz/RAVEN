-- Migration 000002: jobs schema.
--
-- Owned by the jobs service; the worker service updates execution fields
-- (status, attempts, started_at, finished_at, error, worker_id) through the
-- same table. One shared schema, like migration 000001 documents.
--
-- Status flow (docs/contracts/ports-and-env.md):
--   QUEUED -> PROCESSING -> SUCCESS | FAILED -> RETRYING -> SUCCESS | DEAD
--   plus CANCELLED. In this implementation:
--     - FAILED is set by the jobs service when the broker is unreachable at
--       create time (the job never even queued).
--     - The worker moves QUEUED/RETRYING -> PROCESSING -> SUCCESS/RETRYING/DEAD.
--     - DEAD -> QUEUED only via RequeueJob (manual resurrection).

CREATE TABLE jobs (
    id              text PRIMARY KEY,            -- 'job_' + uuid
    type            text NOT NULL,               -- e.g. send_email, resize_image, webhook
    payload         jsonb NOT NULL,
    status          text NOT NULL DEFAULT 'QUEUED'
                    CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCESS', 'FAILED',
                                      'RETRYING', 'CANCELLED', 'DEAD')),
    priority        int NOT NULL DEFAULT 5 CHECK (priority BETWEEN 1 AND 10),
    attempts        int NOT NULL DEFAULT 0,      -- attempts executed so far
    max_attempts    int NOT NULL DEFAULT 4,
    idempotency_key text,                        -- NULL when the caller gave no key
    owner_id        uuid,                        -- user who created the job (NULL = unknown)
    created_at      timestamptz NOT NULL DEFAULT now(),
    started_at      timestamptz,                 -- set when a worker picks the job up
    finished_at     timestamptz,                 -- set on terminal states
    error           text NOT NULL DEFAULT '',    -- last failure message
    worker_id       text NOT NULL DEFAULT ''     -- last worker that ran the job
);

-- Same idempotency key -> same job. Partial: most rows have no key, so the
-- index stays small.
CREATE UNIQUE INDEX idx_jobs_idempotency_key
    ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Queue scans: "give me pending work, highest priority first".
CREATE INDEX idx_jobs_status_priority_created
    ON jobs (status, priority DESC, created_at);

-- ---------------------------------------------------------------------------
-- job_attempts: one row per execution attempt. Audit trail only; requeueing
-- a DEAD job resets jobs.attempts but never touches this history.
-- ---------------------------------------------------------------------------
CREATE TABLE job_attempts (
    id          bigserial PRIMARY KEY,
    job_id      text NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     int NOT NULL,
    started_at  timestamptz NOT NULL,
    finished_at timestamptz,
    error       text NOT NULL DEFAULT '',
    worker_id   text NOT NULL DEFAULT ''
);
CREATE INDEX idx_job_attempts_job_id ON job_attempts (job_id);
