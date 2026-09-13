# RAVEN Console

The operations dashboard for RAVEN — the Go distributed systems platform
(API gateway, auth, users, jobs, message broker, workers, WebSocket service).

It's a single-page app that answers one question fast: **is the platform
healthy, and what is the queue doing right now?**

Design follows Linear (app shell, sidebar, keyboard-first, hairline borders)
mixed with Grafana/Vercel (dense telemetry, small charts, compact tables).
Dark theme, one violet accent (`#8b5cf6`), Inter, lucide icons only.

## Run it

```bash
npm install
npm run dev
```

Then open http://localhost:7100. Other scripts:

```bash
npm run build   # tsc + vite build → dist/
npm run lint    # eslint
npm run preview # serve the production build
```

## Demo mode

The console boots by probing the gateway (`GET http://localhost:8080/api/jobs`,
2.5s timeout). If the gateway is down — which it is most of the time while you
hack on the frontend — it switches to **demo mode** automatically.

Demo mode is not a dead screenshot. A simulator (`src/lib/simulator.ts`) runs a
small world model on a 1s tick: jobs move through
QUEUED → PROCESSING → SUCCESS/FAILED → RETRYING → DEAD, workers send
heartbeats (one is flaky on purpose), topic high-water marks grow while
consumer groups fall behind and catch up, and the req/sec series has periodic
load bursts. Some jobs are poisoned so the DLQ always has a few rows.

Everything works offline: create job, cancel, requeue, filters, pagination,
the drawer, the charts. The topbar shows an amber **DEMO** badge so nobody
confuses it with production data. Sign-in is simulated too (any credentials).

When the real stack is up, the same UI runs on live data:

| Surface        | Endpoint                                   |
| -------------- | ------------------------------------------ |
| REST API       | `http://localhost:8080/api/...`            |
| Job events     | `ws://localhost:8084/ws` (room `jobs`)     |
| Broker stats   | `http://localhost:9101/topics`             |
| Realtime stats | `http://localhost:8084/debug/stats`        |
| Metrics        | `http://localhost:8080/metrics` (optional) |

The WebSocket client (`src/lib/live.ts`) reconnects with exponential backoff
(1s → 2s → 5s → 10s cap) and joins the `jobs` room on open.

Override endpoints with env vars if your stack runs elsewhere:
`VITE_GATEWAY_URL`, `VITE_BROKER_URL`, `VITE_REALTIME_URL`, `VITE_WS_URL`,
`VITE_GRAFANA_URL`, `VITE_JAEGER_URL`, `VITE_PROMETHEUS_URL`.

## Test Lab

The **Test Lab** page (`#/testlab`, wrench icon in the sidebar) lets you drive
the platform yourself instead of just watching it. Every scenario works in both
modes: live mode hits the real gateway, demo mode runs the same flows against
the simulator.

Four cards:

- **Quick E2E check** — one button runs the whole pipeline as a step timeline:
  reach the gateway (`GET /health`), check the session token, create a
  `send_email` job (fresh idempotency key per run), then poll it to a terminal
  status. You get per-step durations, the observed status transitions, and a
  final verdict. Not signed in? You get a small inline hint, not a wall — in
  live mode the create step will 401 without a session.
- **Mini load test** — pick the job count (10–500), the type (`send_email` /
  `resize_image`) and a concurrency (1–40). It creates the jobs through a small
  promise pool, shows a live progress bar with created / in-flight / success /
  failed counters and a per-second rate, then a summary with wall time, jobs/s
  and a status breakdown. In live mode statuses are polled per id through a
  rotating window (bounded requests per second).
- **Fail on purpose** — creates a `webhook` job pointing at
  `http://localhost:9/never-works`. Nothing listens there, so every attempt
  fails; watch it go RETRYING → DEAD. Once it is in the DLQ, a **Requeue from
  DLQ** button appears — requeue it and watch it die again. That is the point:
  a requeue retries the same payload, so fix the cause first.
- **Break it yourself** — copy-paste chaos (`kubectl` kill/scale commands) plus
  the Jaeger and Grafana links where the effects show up. Chaos needs kubectl
  access, so this card is an honest guide, not a button.

Every run lands in the **run log** at the bottom (timestamp, scenario,
outcome), kept for the life of the page.

Tip for headless smoke-testing: `#/testlab?autorun=e2e` (or `load`, `fail`)
starts a scenario on mount — handy with
`msedge --headless --virtual-time-budget=45000 --dump-dom`.

## Auth

The console is usable without an account — you land on the dashboard
(read-only in live mode, full demo when offline). **Sign in** (top right)
calls `POST /api/auth/login` and keeps the token in memory + sessionStorage.
Mutating actions (create / cancel / requeue) send it as a Bearer token.

## Stack and structure

Vite + React 18 + TypeScript (strict) + Tailwind 3.4 + lucide-react +
recharts + zustand. No component library — the UI primitives in
`src/components/ui/` are hand-built (button, card, badge, input, select,
table, dialog/drawer, tabs, toast, skeleton, progress, slider).

```
src/
  lib/        types, config, api client, ws client, simulator, sources, store
  components/ ui/ primitives + layout (shell, command palette) + auth dialog
  features/   jobs drawer + create-job dialog
  pages/      Overview, Jobs, Workers, Broker, Observability
```

Routing is a small hash router (`#/jobs?status=FAILED&page=2`), so filtered
views and open drawers are shareable links. Press `⌘K` / `Ctrl+K` for the
command palette.

## TODO

- Live mode: the worker registry comes from `GET /api/workers` (gateway reads
  the Redis `worker:*` hashes). Job events over the socket fill in the gaps
  between polls.
- DLQ requeue in live mode uses `POST /api/jobs/{id}/requeue` (DEAD jobs only).
- Overview chart in live mode needs the gateway to expose `/metrics`; without
  it the chart shows a designed empty state (everything else still works).
