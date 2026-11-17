-- one pending initial execution per released delayed job or cron occurrence.
CREATE TABLE scheduled_job_outbox (
    job_id text PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    execution_generation integer NOT NULL CHECK (execution_generation > 0),
    queued_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_scheduled_job_outbox_order ON scheduled_job_outbox (queued_at, job_id);
