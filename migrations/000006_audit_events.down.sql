-- Rollback of 000006: drop the platform audit trail.
-- Indexes go away with the table; nothing else depends on audit_events.

DROP TABLE IF EXISTS audit_events;
