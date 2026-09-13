-- Migration 000003: job leases + generation fencing (stranded job recovery).
--
-- Closes the kill -9 gap (docs/failure-scenarios.md): a worker that dies
-- mid-job used to strand the job in PROCESSING forever because broker
-- offsets commit at dispatch. Now every claimed job carries a lease:
--
--   heartbeat_at          last sign of life from the executing worker
--   lease_until           the job is considered stranded once now() passes
--                         this; only meaningful in PROCESSING/RETRYING
--   execution_generation  fencing token. The sweeper bumps it when it takes
--                         a stranded job over, so a late write from the old
--                         worker matches zero rows and is rejected.
--
-- Lease discipline (ADR 009):
--   - claim (worker fence): heartbeat_at = now(), lease_until = now() + lease
--   - renew while executing every lease/3 (same UPDATE, generation-guarded)
--   - RETRYING rows carry lease_until = now() + backoff + lease, so a worker
--     that dies before its retry timer fires is also recovered
--   - terminal states clear lease_until (nothing left to recover)
--   - the sweeper scans PROCESSING/RETRYING with lease_until < now()

ALTER TABLE jobs
    ADD COLUMN heartbeat_at timestamptz,          -- last worker heartbeat
    ADD COLUMN lease_until timestamptz,           -- stranded once now() passes this
    ADD COLUMN execution_generation integer NOT NULL DEFAULT 1; -- fencing token

-- Sweeper scan: "expired leases among live executions". Partial so terminal
-- rows (the vast majority over time) never enter the index.
CREATE INDEX idx_jobs_lease_sweep
    ON jobs (lease_until) WHERE status IN ('PROCESSING', 'RETRYING');
