-- Migration 000001 down: drop the auth + users schema, in dependency order.

DROP TABLE IF EXISTS user_profiles;
DROP TABLE IF EXISTS audit_logs;
DROP TABLE IF EXISTS sessions;
DROP TABLE IF EXISTS user_roles;
DROP TABLE IF EXISTS role_permissions;
DROP TABLE IF EXISTS permissions;
DROP TABLE IF EXISTS roles;
DROP TABLE IF EXISTS users;

-- Extensions are dropped last; nothing else in this database should depend
-- on them once the tables above are gone (this is migration 000001).
DROP EXTENSION IF EXISTS citext;
DROP EXTENSION IF EXISTS pgcrypto;
