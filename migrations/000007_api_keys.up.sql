-- Migration 000007: API keys (programmatic access to the public API).
--
-- One row per issued key. Keys are the machine alternative to JWTs:
-- `Authorization: ApiKey rav_live_...` authenticates at the gateway with the
-- key's scopes acting as the permission set (scopes are a subset of the
-- platform permission vocabulary — a key can never widen its owner's power
-- beyond what the scopes say, and admin:* is never issuable as a scope).
--
-- Storage rules:
--   key_prefix   first 8 chars of the key body (after "rav_live_"), stored in
--                the clear so the console can show "rav_live_AbCdEfGh…" in
--                key lists. 8 chars of a 256-bit key identify nothing — the
--                full key is required to authenticate.
--   key_hash     hex SHA-256 of the FULL key. API keys are high-entropy
--                (256 bits from crypto/rand), so a plain fast hash is the
--                right KDF: brute-forcing the preimage is infeasible, and an
--                indexed equality lookup beats the per-request bcrypt cost
--                passwords need. The plaintext key is NEVER stored.
--   scopes       text[] of platform permissions (users:read, jobs:create,
--                ...). Checked against internal/auth at issue time.
--   last_used_at best-effort touch by the gateway (async, never blocks the
--                request); NULL means "never used".
--   revoked_at   soft revoke. Revoked rows are kept for the audit trail;
--                every read path filters `revoked_at IS NULL`.

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id     uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name         text NOT NULL,
    key_prefix   text NOT NULL,
    key_hash     text NOT NULL UNIQUE,
    scopes       text[] NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);

-- The UNIQUE constraint on key_hash already creates the lookup index used by
-- authentication (hash equality); listed here for documentation:
-- index on api_keys(key_hash).

-- "My keys, newest first" (the GET /api/keys read path).
CREATE INDEX idx_api_keys_owner_created
    ON api_keys (owner_id, created_at DESC)
    WHERE revoked_at IS NULL;
