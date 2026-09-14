# raven — the official RAVEN CLI

`raven` is a small command-line tool for talking to the RAVEN platform from
your terminal. It speaks to the public REST API exposed by the gateway, so
anything you can do here you could also do with `curl` — just with less
typing and nicer output.

- **Zero dependencies.** Go standard library only. No cobra, no config
  files in five formats, no plugins.
- **Script-friendly.** Every command accepts `--json`, and exit codes are a
  stable contract (see below).
- **Human-friendly.** Aligned tables, readable errors, sensible defaults.

## Install

From the repository root:

```bash
go install ./cmd/raven
```

This drops a `raven` binary into your `GOBIN` (usually `~/go/bin`). Make
sure that directory is on your `PATH`.

To stamp a release version into the binary:

```bash
go install -ldflags "-X github.com/refleeexzz/RAVEN/internal/cli.Version=v0.1.0" ./cmd/raven
```

Check it works:

```bash
raven version
# raven v0.1.0 (go1.27.1, windows/amd64)
```

## Quick start

```bash
# 1. log in (asks for your password without echoing it)
raven login --email you@example.com

# 2. submit a job
raven jobs submit --type send_email --payload '{"to":"friend@example.com","subject":"hi"}'

# 3. watch it finish
raven jobs watch <id>

# 4. see what's going on
raven workers
raven health
```

By default the CLI talks to `http://localhost:8080/api`. Point it somewhere
else with the `RAVEN_API_URL` environment variable or the `--api-url` flag:

```bash
export RAVEN_API_URL=https://raven.example.com/api
raven health
```

## Global flags

These two work on **every** command. Flags can go before or after
positional arguments — `raven jobs watch abc --interval 5s` and
`raven jobs watch --interval 5s abc` are the same thing.

| Flag           | What it does                                                        |
| -------------- | ------------------------------------------------------------------- |
| `--json`       | Print machine-readable JSON instead of tables.                      |
| `--api-url` URL | Base URL of the gateway API. Beats `RAVEN_API_URL` and the config. |

## Environment variables

| Variable        | What it does                                                     |
| --------------- | ---------------------------------------------------------------- |
| `RAVEN_API_URL` | Gateway API base URL (default `http://localhost:8080/api`).      |
| `RAVEN_PASSWORD` | Password for `raven login` — skips the interactive prompt.      |
| `RAVEN_CONFIG`  | Config file path (default `~/.raven/config.json`).               |

## Exit codes

| Code | Meaning                                              |
| ---- | ---------------------------------------------------- |
| `0`  | Success.                                             |
| `1`  | Generic error (API error, network down, job failed). |
| `2`  | Bad usage: unknown command, bad flags, missing args. |
| `3`  | Auth problem: not logged in, or the API said 401/403. |

## Errors

API errors arrive in a standard envelope, and the CLI prints all of it —
code, message and the request id you can grep for in the gateway logs:

```
$ raven jobs get nope
raven: not_found: job not found (request id: 01J9Z8K7...)
```

## Commands

### `raven login`

```bash
raven login --email you@example.com [--password pw] [--api-url URL]
```

Logs in and saves the token pair to the config file. The password comes
from (in order): `--password`, `RAVEN_PASSWORD`, or an interactive prompt
with echo disabled. The password itself is **never** written to disk.

Tip for scripts: `RAVEN_PASSWORD=hunter2 raven login --email ci@example.com`.

### `raven logout`

```bash
raven logout
```

Revokes the refresh token on the server, then deletes the local tokens.
Even if the server call fails, the local credentials are removed (a warning
tells you the remote part didn't work).

### `raven jobs submit`

```bash
raven jobs submit --type send_email \
  --payload '{"to":"friend@example.com","subject":"hello from raven"}' \
  [--priority 5] [--max-attempts 3] [--idempotency-key my-key]
```

Creates a job. `--payload` must be valid JSON (defaults to `{}`). Known job
types at the time of writing: `send_email`, `resize_image`, `webhook` — but
the CLI doesn't check; the server does.

**Idempotency:** if you pass `--idempotency-key`, the gateway forwards it
and a retried submission with the same key returns the original job instead
of creating a duplicate. If you don't pass one, the CLI generates a random
`cli-…` key for you and prints it, so even a copy-pasted double-run of the
same command line is safe against network-level retries.

Example output:

```
job 018f3c2a-… submitted
id:       018f3c2a-…
type:     send_email
status:   QUEUED
priority: 5
attempts: 0/3
worker:   -
created:  2026-01-01 12:00:00
started:  -
finished: -
error:    -
payload: {
  "to": "friend@example.com",
  "subject": "hello from raven"
}
idempotency-key: cli-9f2ab1c7d4e5f6a0
```

### `raven jobs list`

```bash
raven jobs list [--status queued] [--type send_email] [--page 1] [--page-size 20]
```

Lists your jobs, newest relevant page first:

```
ID            TYPE        STATUS   PRI  TRIES  CREATED
018f3c2a-…    send_email  SUCCESS  5    1/3    2026-01-01 12:00:00
018f3b10-…    webhook     QUEUED   0    0/3    2026-01-01 11:59:41
page 1 · 20/page · 2 total
```

`--status` accepts lowercase (`queued`) or uppercase (`QUEUED`) names:
`queued`, `processing`, `success`, `failed`, `retrying`, `cancelled`,
`dead`. Page size caps at 100 (server-side limit).

### `raven jobs get <id>`

Shows one job, payload pretty-printed. `--json` gives you the raw object
for piping into `jq`.

### `raven jobs cancel <id>`

Cancels a queued or processing job and prints its new status
(`CANCELLED`). Cancelling a finished job is a server-side error and prints
the envelope like any other API failure.

### `raven jobs requeue <id>`

Moves a `DEAD` job (the dead-letter queue) back to `QUEUED` for another
run. Only dead jobs can be requeued — the server enforces it.

### `raven jobs watch <id>`

```bash
raven jobs watch 018f3c2a-… [--interval 2s] [--timeout 10m]
```

Polls the job and prints a line each time the status changes, until it
reaches a terminal state (`SUCCESS`, `FAILED`, `CANCELLED`, `DEAD`):

```
12:00:01  QUEUED
12:00:03  PROCESSING  (worker worker-7f9c)
12:00:05  SUCCESS
```

Exit code is `0` when the job succeeds and `1` otherwise, so scripts can do
`raven jobs watch $ID && echo shipped`. `--timeout 0` means "wait forever";
Ctrl-C stops watching without touching the job. With `--json`, each poll is
printed as one JSON object per line (JSONL).

### `raven workers`

Lists the live worker pool (from the gateway's Redis-backed registry):

```
ID          STARTED               LAST HEARTBEAT        PROCESSED  IN FLIGHT
worker-7f9c 2026-01-01T08:00:00Z 2026-01-01T12:00:05Z  1337       2
```

A worker disappears from this list a few seconds after it dies, so what you
see is effectively "who is alive right now".

### `raven health`

```bash
raven health
```

Aggregated health of every service, probed by the gateway in real time.
**No login required** — handy for monitoring scripts.

```
SERVICE      STATUS    LATENCY  DETAIL
gateway      ok        0ms      self
auth         ok        2ms      reachable
users        ok        2ms      reachable
jobs         ok        3ms      reachable
broker       ok        1ms      tcp listener up
websocket    ok        1ms      4 connections, 2 rooms
worker_pool  ok        0ms      3 workers
overall: ok (checked 2026-01-01 12:00:05)
```

Exits `0` when everything is `ok`, `1` when anything is `degraded` or
`down` — so `raven health || alert` just works.

### `raven version`

Prints the CLI version, the Go runtime it was built with, and the OS/arch.
`--json` for machines.

## The config file

Location: `~/.raven/config.json` (override with `RAVEN_CONFIG`). It holds
the API URL, your email, and the token pair from your last login:

```json
{
  "api_url": "http://localhost:8080/api",
  "email": "you@example.com",
  "access_token": "…",
  "refresh_token": "…"
}
```

The file is written with `0600` permissions (owner read/write only) inside
a `0700` directory. On Windows, file permissions are best-effort — Go maps
chmod onto the read-only attribute — and the real protection is your user
profile's ACL, which already keeps other accounts out. Treat the file like
a password store either way.

URL resolution order, highest priority first: `--api-url` flag →
`RAVEN_API_URL` → `api_url` in the config file →
`http://localhost:8080/api`.

## Notes and limits

- **No colors.** Output is plain text everywhere, so it survives pipes,
  CI logs and Windows terminals alike.
- **No auto token refresh yet.** When the access token expires, log in
  again (`raven login`). Refresh-token support is planned.
- **`GET /api/users/me` does not exist** in the current gateway, so there
  is no `raven whoami` yet. `raven login` already shows who you logged in
  as, and the email is kept in the config file.
- Access tokens expire. If you start getting `unauthorized` errors out of
  nowhere, that's your cue to log in again.
