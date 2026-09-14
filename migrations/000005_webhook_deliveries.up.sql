-- Migration 000005: webhook delivery observability.
--
-- One row per webhook delivery attempt the worker makes: success, HTTP
-- error, transport error and egress-guard refusal (JOBS-01) alike. The
-- worker writes straight to Postgres (it already owns a pool for the lease
-- machinery); the jobs service reads the table back for ListDeliveries.
-- Best effort by design: a failed insert is logged and the job flow goes
-- on untouched — observability must never block execution.
--
-- Columns:
--   attempt          job attempt number this delivery belongs to
--                    (jobs.attempts at finish time; requeues start again
--                    at 1, like job_attempts does)
--   status_code      HTTP status when a response came back, NULL otherwise
--   latency_ms       round-trip time to response headers, NULL when no
--                    request left the worker (blocked / invalid payload)
--   response_snippet first bytes of the response body, hard-capped at
--                    1 KiB (CHECK below is the database backstop; the
--                    worker truncates before insert)
--   blocked          true when the egress guard refused the target
--                    (private/loopback/link-local/reserved ranges, bad
--                    scheme, userinfo, redirect limit)
--   error            failure detail for blocked/transport/HTTP-error rows

CREATE TABLE webhook_deliveries (
    id               bigserial PRIMARY KEY,
    job_id           text NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt          int NOT NULL,
    url              text NOT NULL DEFAULT '',
    status_code      int,                        -- NULL: no response received
    latency_ms       int,                        -- NULL: no request dispatched
    response_snippet text NOT NULL DEFAULT ''
                     CHECK (octet_length(response_snippet) <= 1024),
    blocked          boolean NOT NULL DEFAULT false,
    error            text NOT NULL DEFAULT '',
    ts               timestamptz NOT NULL DEFAULT now()
);

-- The read path is "deliveries of one job, newest last": ListDeliveries
-- pages by (job_id, ts).
CREATE INDEX idx_webhook_deliveries_job_ts
    ON webhook_deliveries (job_id, ts);
