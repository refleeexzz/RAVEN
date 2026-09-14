-- Migration 000005 down: drop webhook delivery observability.
-- The index goes away with the table.

DROP TABLE IF EXISTS webhook_deliveries;
