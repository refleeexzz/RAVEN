-- Migration 000004 down: remove scheduling.
--
-- Fails loudly if any job is still SCHEDULED or references a replay source:
-- those states have no meaning without this migration. Finish or cancel the
-- scheduled backlog before rolling back.

DROP TABLE IF EXISTS cron_schedules;

DROP INDEX IF EXISTS idx_jobs_scheduled_due;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS scheduled_at,
    DROP COLUMN IF EXISTS replayed_from;

ALTER TABLE jobs DROP CONSTRAINT jobs_status_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_status_check
    CHECK (status IN ('QUEUED', 'PROCESSING', 'SUCCESS', 'FAILED',
                      'RETRYING', 'CANCELLED', 'DEAD'));
