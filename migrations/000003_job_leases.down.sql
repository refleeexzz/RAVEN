-- Migration 000003 down: drop job leases + generation fencing.

DROP INDEX IF EXISTS idx_jobs_lease_sweep;

ALTER TABLE jobs
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS lease_until,
    DROP COLUMN IF EXISTS execution_generation;
