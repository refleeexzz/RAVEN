-- Migration 000001: auth + users schema.
--
-- One schema (public) for the whole demo platform. Documented trade-off:
-- in a real system the auth service and the users service would own
-- separate schemas (or separate databases) with explicit ownership; here we
-- keep a single schema so the learning platform stays easy to inspect.
-- Ownership is documented per table below and enforced by code review, not
-- by the database.

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS citext;   -- case-insensitive email

-- ---------------------------------------------------------------------------
-- users: owned by the auth service. The users service reads it and updates
-- profile fields (display_name) only; credentials live here because auth
-- owns identity.
-- ---------------------------------------------------------------------------
CREATE TABLE users (
    id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email         citext NOT NULL UNIQUE,
    display_name  text,
    password_hash text NOT NULL,
    deleted_at    timestamptz, -- soft delete; NULL means active
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
-- The UNIQUE constraint on email already creates a btree index; it is listed
-- here for documentation: index on users(email).

-- ---------------------------------------------------------------------------
-- RBAC: roles, permissions, and the two join tables.
-- ---------------------------------------------------------------------------
CREATE TABLE roles (
    id   serial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

INSERT INTO roles (name) VALUES ('ADMIN'), ('USER'), ('WORKER'), ('SERVICE');

CREATE TABLE permissions (
    id   serial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

INSERT INTO permissions (name) VALUES
    ('users:read'), ('users:write'), ('users:delete'),
    ('jobs:create'), ('jobs:read'), ('jobs:cancel'),
    ('admin:*');

CREATE TABLE role_permissions (
    role_id       int NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    permission_id int NOT NULL REFERENCES permissions (id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

-- ADMIN gets the admin wildcard.
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r JOIN permissions p ON p.name = 'admin:*'
WHERE r.name = 'ADMIN';

-- USER gets self-service + job submission permissions.
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r
JOIN permissions p ON p.name IN ('users:read', 'jobs:create', 'jobs:read', 'jobs:cancel')
WHERE r.name = 'USER';

-- WORKER only reads jobs.
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r JOIN permissions p ON p.name = 'jobs:read'
WHERE r.name = 'WORKER';

-- SERVICE (machine-to-machine) gets every permission.
INSERT INTO role_permissions (role_id, permission_id)
SELECT r.id, p.id FROM roles r CROSS JOIN permissions p
WHERE r.name = 'SERVICE';

CREATE TABLE user_roles (
    user_id uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    role_id int  NOT NULL REFERENCES roles (id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);
-- New users get the USER role by default (assigned by the auth service at
-- registration time, inside the same transaction that creates the user).

-- ---------------------------------------------------------------------------
-- sessions: refresh tokens, owned by the auth service. Only the sha256 hash
-- of the refresh token is stored; tokens rotate on every refresh and the old
-- session row is revoked (reuse of a revoked token revokes ALL sessions of
-- that user).
-- ---------------------------------------------------------------------------
CREATE TABLE sessions (
    id                 uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id            uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    refresh_token_hash text NOT NULL UNIQUE,
    expires_at         timestamptz NOT NULL,
    revoked_at         timestamptz,
    created_at         timestamptz NOT NULL DEFAULT now(),
    user_agent         text,
    ip                 text
);
-- The UNIQUE constraint already indexes refresh_token_hash; listed for
-- documentation: index on sessions(refresh_token_hash).
CREATE INDEX idx_sessions_user_id ON sessions (user_id);

-- ---------------------------------------------------------------------------
-- audit_logs: security-relevant events (register/login/logout/refresh/
-- revoke + security events like refresh-token reuse).
-- ---------------------------------------------------------------------------
CREATE TABLE audit_logs (
    id         bigserial PRIMARY KEY,
    user_id    uuid, -- NULL when the actor is unknown (e.g. failed login)
    action     text NOT NULL,
    ip         text,
    user_agent text,
    metadata   jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_audit_logs_user_created ON audit_logs (user_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- user_profiles: owned by the users service (bio, avatar). One row per user,
-- created empty at registration time.
-- ---------------------------------------------------------------------------
CREATE TABLE user_profiles (
    user_id    uuid PRIMARY KEY REFERENCES users (id) ON DELETE CASCADE,
    bio        text,
    avatar_url text,
    updated_at timestamptz NOT NULL DEFAULT now()
);
