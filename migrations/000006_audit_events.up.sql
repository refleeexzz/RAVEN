-- Migration 000006: platform-wide audit trail (audit_events).
--
-- One append-only table, written by the gateway audit middleware through the
-- async writer in internal/audit. It complements (not replaces) the auth
-- service's own audit_logs table from migration 000001: audit_logs tracks
-- identity-internal events (refresh-token reuse, revocations), while
-- audit_events records every mutating API call that crosses the edge —
-- who did what, on which resource, with which outcome, correlated by
-- trace id.
--
-- Column notes:
--   actor_id      user id (uuid as text), or the sentinels 'anonymous' /
--                 'system'. text (not uuid) so the sentinels fit.
--   action        dotted verb, e.g. auth.login.success, auth.login.failure,
--                 jobs.create, jobs.cancel, jobs.requeue, users.update,
--                 users.delete.
--   outcome       success | failure | denied. 'denied' is reserved for
--                 authentication/authorization rejections (401/403) on
--                 protected routes; a failed login is a 'failure'.
--   ip            client IP (RemoteAddr host, no port).
--   trace_id      the request id minted/propagated by middleware.RequestID
--                 (X-Request-ID), so an audit row joins with logs and
--                 traces.
--   detail        route pattern, path params and the redacted request-body
--                 subset (password/token/secret keys never leave the edge).

CREATE TABLE audit_events (
    id            bigserial PRIMARY KEY,
    ts            timestamptz NOT NULL DEFAULT now(),
    actor_id      text NOT NULL DEFAULT 'anonymous',
    action        text NOT NULL,
    resource_type text NOT NULL DEFAULT '',
    resource_id   text NOT NULL DEFAULT '',
    outcome       text NOT NULL DEFAULT 'failure'
                  CHECK (outcome IN ('success', 'failure', 'denied')),
    ip            text NOT NULL DEFAULT '',
    user_agent    text NOT NULL DEFAULT '',
    trace_id      text NOT NULL DEFAULT '',
    detail        jsonb NOT NULL DEFAULT '{}'::jsonb
);

-- Console queries: "latest events first".
CREATE INDEX idx_audit_events_ts ON audit_events (ts DESC);

-- "Everything this actor did, latest first".
CREATE INDEX idx_audit_events_actor_ts ON audit_events (actor_id, ts DESC);

-- "Every failed login / every jobs.cancel, latest first".
CREATE INDEX idx_audit_events_action_ts ON audit_events (action, ts DESC);
