-- Migration 000007 down: drop API keys.
-- The partial index goes away with the table.

DROP TABLE IF EXISTS api_keys;
