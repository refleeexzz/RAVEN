// Headless-Edge CDP verification for the four live-mode fixes.
// - loads the local dev build (http://localhost:7101) with real k8s backend
// - checks connection badge, stat cards, health tiles (bugs 2/3/4)
// - opens the sign-in dialog and TYPES a full email + password char by char
//   via trusted CDP key events, asserting focus never leaves the field (bug 1)
// Usage: node cdp-verify.mjs   (dev server on 7101 must be running)

import { spawn, execSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

const EDGE = "C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe";
const PORT = 9223;
const APP = "http://localhost:7101/";
const OUT = "F:\\RAVEN — Distributed Systems Platform\\web\\console\\.verify";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fail = (msg) => {
  console.error(`FAIL: ${msg}`);
  process.exitCode = 1;
};

const userDir = mkdtempSync(join(tmpdir(), "raven-cdp-"));
const edge = spawn(
  EDGE,
  ["--headless", "--disable-gpu", `--remote-debugging-port=${PORT}`, `--user-data-dir=${userDir}`, "about:blank"],
  { stdio: "ignore" },
);

let ws;
let msgId = 0;
const pending = new Map();

function send(method, params = {}) {
  return new Promise((resolve, reject) => {
    const id = ++msgId;
    pending.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });
}

async function evalJs(expression) {
  const r = await send("Runtime.evaluate", { expression, returnByValue: true });
  if (r.exceptionDetails) throw new Error(`evaluate failed: ${JSON.stringify(r.exceptionDetails.exception?.description ?? r.exceptionDetails.text)}`);
  return r.result?.value;
}

async function typeText(text) {
  for (const ch of text) {
    await send("Input.dispatchKeyEvent", { type: "char", text: ch });
    await sleep(70); // slower than a fast typist, fast enough to catch per-keystroke re-renders
  }
}

async function pressTab() {
  await send("Input.dispatchKeyEvent", { type: "rawKeyDown", key: "Tab", code: "Tab", windowsVirtualKeyCode: 9 });
  await send("Input.dispatchKeyEvent", { type: "keyUp", key: "Tab", code: "Tab", windowsVirtualKeyCode: 9 });
}

try {
  // wait for devtools endpoint
  let version = null;
  for (let i = 0; i < 40; i++) {
    try {
      version = await (await fetch(`http://localhost:${PORT}/json/version`)).json();
      break;
    } catch {
      await sleep(250);
    }
  }
  if (!version) throw new Error("Edge devtools endpoint never came up");

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
    }
  };

  await send("Runtime.enable");
  await send("Page.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: 1500, height: 1100, deviceScaleFactor: 1, mobile: false });

  // Let the app boot, probe the gateway, connect the socket, and poll twice.
  await sleep(9000);

  // ── Bug 3: connection badge should read "Live" (anon token), not a loop ──
  const header = await evalJs(`document.querySelector('header').innerText.replaceAll('\\n', ' | ')`);
  console.log(`HEADER: ${header}`);
  if (/Reconnecting/.test(header)) fail("badge still says Reconnecting");
  if (!/Live/.test(header)) fail("badge does not say Live");

  // ── Bug 4: stat cards should show sane Prometheus values ─────────────────
  const cards = await evalJs(
    `[...document.querySelectorAll('main .grid')][0]?.innerText.replaceAll('\\n', ' | ').slice(0, 600)`,
  );
  console.log(`CARDS: ${cards}`);
  if (/13,295|17339/.test(cards ?? "")) fail("garbage cumulative-counter numbers still shown");
  const rpsValue = await evalJs(
    `[...document.querySelectorAll('main p')].find(p => p.previousElementSibling?.textContent === 'Requests/sec' || p.parentElement?.innerText?.startsWith('Requests/sec'))?.textContent ?? null`,
  );
  console.log(`RPS CARD: ${rpsValue}`);

  // ── Bug 2: health tiles ──────────────────────────────────────────────────
  const tiles = await evalJs(
    `(() => { const h=[...document.querySelectorAll('h2')].find(e=>/Service health/.test(e.textContent)); if(!h) return 'NO GRID'; const card=h.closest('div.rounded-lg'); return card.innerText.replaceAll('\\n',' | ').slice(0,900); })()`,
  );
  console.log(`HEALTH: ${tiles}`);
  const downCount = (tiles?.match(/\|\s*down\s*\|/g) ?? []).length;
  if (downCount > 0) fail(`${downCount} tiles render "down" (should be ok/unknown)`);

  // chart has real points?
  const chartPts = await evalJs(`document.querySelectorAll('.recharts-area-curve').length`);
  console.log(`CHART PATHS: ${chartPts}`);

  // ── Bug 1: sign-in dialog typing ─────────────────────────────────────────
  await evalJs(
    `(() => { const b=[...document.querySelectorAll('header button')].find(b=>/sign in/i.test(b.textContent)); if(!b) throw new Error('sign-in button not found'); b.click(); return true; })()`,
  );
  await sleep(500);
  const focusOnOpen = await evalJs(`document.activeElement?.id || document.activeElement?.tagName`);
  console.log(`FOCUS ON OPEN: ${focusOnOpen}`);

  // click into the email field like a user (even though open-focus should be there)
  await evalJs(`document.getElementById('signin-email').focus()`);
  await typeText("ops@raven.dev");
  const emailVal = await evalJs(`document.getElementById('signin-email').value`);
  const focusAfterEmail = await evalJs(`document.activeElement?.id || document.activeElement?.tagName`);
  console.log(`EMAIL VALUE: "${emailVal}" · FOCUS: ${focusAfterEmail}`);
  if (emailVal !== "ops@raven.dev") fail(`email field holds "${emailVal}" — focus was stolen mid-typing`);
  if (focusAfterEmail !== "signin-email") fail(`focus left the email field while typing (now on ${focusAfterEmail})`);

  await pressTab();
  await sleep(200);
  const focusAfterTab = await evalJs(`document.activeElement?.id || document.activeElement?.tagName`);
  console.log(`FOCUS AFTER TAB: ${focusAfterTab}`);
  if (focusAfterTab !== "signin-password") fail(`Tab did not land on password (on ${focusAfterTab})`);
  await typeText("sup3r-secret!");
  const pwLen = await evalJs(`document.getElementById('signin-password').value.length`);
  const focusFinal = await evalJs(`document.activeElement?.id || document.activeElement?.tagName`);
  console.log(`PASSWORD LENGTH: ${pwLen} · FOCUS: ${focusFinal}`);
  if (pwLen !== 13) fail(`password field holds ${pwLen} chars, expected 13`);
  if (focusFinal !== "signin-password") fail(`focus left the password field while typing`);

  const shot = await send("Page.captureScreenshot", { format: "png" });
  writeFileSync(join(OUT, "bugfix-signin-typing.png"), Buffer.from(shot.data, "base64"));

  // close dialog without submitting (verification only)
  await evalJs(`document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }))`);
  await sleep(400);

  // overview screenshot
  await evalJs(`window.scrollTo(0, 0)`);
  const shot2 = await send("Page.captureScreenshot", { format: "png" });
  writeFileSync(join(OUT, "bugfix-overview-live.png"), Buffer.from(shot2.data, "base64"));

  console.log(process.exitCode ? "DONE (with failures)" : "DONE (all checks passed)");
} catch (err) {
  console.error(`ERROR: ${err instanceof Error ? err.message : err}`);
  process.exitCode = 1;
} finally {
  try {
    execSync(`taskkill /F /T /PID ${edge.pid}`, { stdio: "ignore" });
  } catch {
    /* already gone */
  }
  process.exit(process.exitCode ?? 0);
}
