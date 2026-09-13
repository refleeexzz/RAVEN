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

- Live mode: worker registry is derived from job events unless the gateway
  exposes `GET /api/workers`.
- DLQ requeue in live mode expects `POST /api/jobs/{id}/requeue`, which is not
  in the spec yet — the UI shows a clear error toast if the gateway 404s.
- Overview chart in live mode needs the gateway to expose `/metrics`; without
  it the chart shows a designed empty state (everything else still works).
