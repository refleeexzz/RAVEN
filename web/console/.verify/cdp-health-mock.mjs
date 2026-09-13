// Verifies the health-grid success path by intercepting /api/health/services
// with the documented v4 payload (the real gateway build is still rolling out).
// Usage: node cdp-health-mock.mjs  (dev server on 7101 must be running)

import { spawn, execSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const EDGE = "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe";
const PORT = 9224;
const APP = "http://localhost:7101/";
const OUT = "F:\\RAVEN — Distributed Systems Platform\\web\\console\\.verify";

const MOCK = JSON.stringify({
  checked_at: "2026-09-13T20:30:00Z",
  services: [
    { name: "gateway", status: "ok", latency_ms: 1, detail: "" },
    { name: "auth", status: "ok", latency_ms: 2, detail: "" },
    { name: "users", status: "ok", latency_ms: 1, detail: "" },
    { name: "jobs", status: "ok", latency_ms: 1, detail: "" },
    { name: "broker", status: "ok", latency_ms: 1, detail: "2 topics" },
    { name: "websocket", status: "ok", latency_ms: 3, detail: "4 connections" },
    { name: "worker_pool", status: "degraded", latency_ms: 41, detail: "1 of 5 workers stale" },
  ],
});

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const userDir = mkdtempSync(join(tmpdir(), "raven-cdp-health-"));
const edge = spawn(
  EDGE,
  ["--headless", "--disable-gpu", `--remote-debugging-port=${PORT}`, `--user-data-dir=${userDir}`, "about:blank"],
  { stdio: "ignore" },
);

let ws;
let msgId = 0;
const pending = new Map();
const send = (method, params = {}) =>
  new Promise((resolve, reject) => {
    const id = ++msgId;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });

try {
  let up = false;
  for (let i = 0; i < 40; i++) {
    try {
      await fetch(`http://localhost:${PORT}/json/version`);
      up = true;
      break;
    } catch {
      await sleep(250);
    }
  }
  if (!up) throw new Error("devtools endpoint never came up");

  const target = await (
    await fetch(`http://localhost:${PORT}/json/new?${encodeURIComponent(APP)}`, { method: "PUT" })
  ).json();
  ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((res, rej) => {
    ws.onopen = res;
    ws.onerror = rej;
  });

  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data);
    if (msg.id && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(msg.error.message));
      else resolve(msg.result);
      return;
    }
    // Intercept only the health endpoint; let everything else through.
    if (msg.method === "Fetch.requestPaused") {
      const { requestId, request } = msg.params;
      if (request.url.includes("/api/health/services")) {
        void send("Fetch.fulfillRequest", {
          requestId,
          responseCode: 200,
          responseHeaders: [
            { name: "Content-Type", value: "application/json" },
            { name: "Access-Control-Allow-Origin", value: "*" },
          ],
          body: Buffer.from(MOCK, "utf8").toString("base64"),
        }).catch(() => {});
      } else {
        void send("Fetch.continueRequest", { requestId }).catch(() => {});
      }
    }
  };

  await send("Runtime.enable");
  await send("Page.enable");
  await send("Fetch.enable", { patterns: [{ urlPattern: "*" }] });
  await send("Emulation.setDeviceMetricsOverride", { width: 1500, height: 800, deviceScaleFactor: 1, mobile: false });
  await send("Page.reload", { ignoreCache: true });

  await sleep(9000);

  const tiles = await send("Runtime.evaluate", {
    expression: `(() => { const h=[...document.querySelectorAll('h2')].find(e=>/Service health/.test(e.textContent)); if(!h) return 'NO GRID'; return h.closest('div.rounded-lg').innerText.replaceAll('\\n',' | ').slice(0,900); })()`,
    returnByValue: true,
  });
  const text = tiles.result?.value ?? "";
  console.log(`HEALTH: ${text}`);

  const okCount = (text.match(/\|\s*ok\s*\|/gi) ?? []).length;
  const degraded = /\|\s*degraded\s*\|/i.test(text);
  const workerLabel = /Worker pool/.test(text);
  const details = /2 topics/.test(text) && /4 connections/.test(text);
  console.log(`ok tiles: ${okCount} · degraded tile: ${degraded} · worker_pool mapped: ${workerLabel} · details passed through: ${details}`);
  if (okCount !== 6 || !degraded || !workerLabel || !details) {
    console.error("FAIL: success-path mapping wrong");
    process.exitCode = 1;
  }

  const shot = await send("Page.captureScreenshot", { format: "png" });
  writeFileSync(join(OUT, "bugfix-health-mock.png"), Buffer.from(shot.data, "base64"));
  console.log(process.exitCode ? "DONE (with failures)" : "DONE (health success path verified)");
} catch (err) {
  console.error(`ERROR: ${err instanceof Error ? err.message : err}`);
  process.exitCode = 1;
} finally {
  try {
    execSync(`taskkill /F /T /PID ${edge.pid}`, { stdio: "ignore" });
  } catch {
    /* gone */
  }
  process.exit(process.exitCode ?? 0);
}
