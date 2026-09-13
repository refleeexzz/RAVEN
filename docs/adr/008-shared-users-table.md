# ADR 008: One shared users table (auth owns it, users reads it)

Status: accepted for the demo, flagged as the first thing to fix for real use

## Context

The auth service owns identity: registration, login, passwords, sessions,
roles. The users service owns profiles and admin-style CRUD: list users,
update bio/avatar, soft-delete. Both need the same rows — a user is one
entity. The question is whether each service gets its own store or they
share one.

## Decision

Share one `users` table in one Postgres schema. Ownership is documented,
not enforced:

- **auth** writes identity: creates the row at registration (with the
  `USER` role and an empty profile row in the same transaction), owns
  `password_hash`, sessions and the RBAC tables.
- **users** reads the table and updates profile-ish fields
  (`display_name`); it owns `user_profiles` (bio, avatar). It writes
  password hashes only at creation time (`CreateUser`), using the same
  shared validation and bcrypt rules from `internal/auth`.

Both services connect to the same database with the same credentials. The
split is a code-review convention, written down in migration 000001's
header comment.

## Alternatives

- **Separate schemas/databases + events.** The textbook answer: auth owns
  `auth.users` (credentials), users owns `users.profiles`, and a
  `user_registered` event (we even have the broker for it) carries the new
  user id across. Rejected for the demo because it doubles the schema, adds
  an eventual-consistency window into the very first thing a user does
  (register, then immediately show up in the users list), and the failure
  modes (event lost → profile missing) are exactly the kind of distributed
  pain a learning project should opt into *deliberately*, not everywhere at
  once.
- **Merge auth and users into one identity service.** Honestly the simplest
  production answer at this scale — they share data, deploy cadence and
  blast radius. Kept separate here because the project's goal is practicing
  service boundaries, and auth-vs-users is the classic boundary to
  practice on.
- **users calls auth over gRPC** for every read. Clean ownership, but now
  listing users needs a join across a network hop per page. Chose not to.

## Consequences

Positive:

- Registration is atomic: user + role + profile + audit in one transaction.
  No "user exists in auth but not in users" window, ever.
- The demo stays inspectable: one database, two migrations, and `psql`
  shows you the whole identity story.
- Shared validation rules live in `internal/auth` and both services use
  them, so email/password rules cannot drift between the register path and
  the admin-create path.

Negative (being straight about it):

- **Ownership is a gentlemen's agreement.** Nothing at the database level
  stops the users service from updating `password_hash`, or auth from
  rewriting profiles. In a real system this is where the "microservices
  share a database" horror stories begin.
- **Coupled migrations and deployments.** A schema change to `users` touches
  both services' SQL at once; you cannot deploy them independently with
  confidence.
- **Blast radius is shared.** Postgres down takes both services out
  together (see [../failure-scenarios.md](../failure-scenarios.md)) — though
  with one database instance that was already true.
- Scaling them separately is fiction: they contend for the same rows.

**What production would do instead**, in order of pragmatism: (1) merge
into a single identity service, or (2) split stores and replicate the new
user's id+email with a `user_registered` event through the broker — the
infrastructure for option 2 already exists in this repo, which makes this a
good first refactor exercise.
