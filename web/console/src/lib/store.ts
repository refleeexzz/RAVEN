import { create } from "zustand";
import { login, probeGateway } from "./api";
import { DemoSource, LiveSource, type Source } from "./source";
import type {
  ConnState,
  CreateJobInput,
  Job,
  JobEvent,
  JobsPageState,
  JobsQuery,
  MetricSample,
  Mode,
  Snapshot,
} from "./types";

export interface HistoryPoint {
  t: number;
  active: number;
  queue: number;
  ws: number | null;
}

interface ConsoleStore {
  mode: Mode;
  conn: ConnState;
  token: string | null;
  email: string | null;
  snapshot: Snapshot | null;
  history: HistoryPoint[];
  events: JobEvent[];
  jobsPage: JobsPageState;

  paletteOpen: boolean;
  signInOpen: boolean;
  createJobOpen: boolean;

  boot: () => Promise<void>;
  signIn: (email: string, password: string) => Promise<void>;
  signOut: () => void;
  loadJobs: (q: JobsQuery, opts?: { silent?: boolean }) => Promise<void>;
  getJob: (id: string) => Promise<Job | null>;
  createJob: (input: CreateJobInput) => Promise<Job>;
  cancelJob: (id: string) => Promise<void>;
  requeueJob: (id: string) => Promise<void>;
  metricsSummary: () => Promise<MetricSample[]>;
  setPaletteOpen: (open: boolean) => void;
  setSignInOpen: (open: boolean) => void;
  setCreateJobOpen: (open: boolean) => void;
}

let source: Source | null = null;
let bootPromise: Promise<void> | null = null;

export const useConsole = create<ConsoleStore>()((set, get) => ({
  mode: "connecting",
  conn: "connecting",
  token: null,
  email: null,
  snapshot: null,
  history: [],
  events: [],
  jobsPage: { status: "loading", items: [], total: 0 },
  paletteOpen: false,
  signInOpen: false,
  createJobOpen: false,

  async boot() {
    if (bootPromise) return bootPromise;
    bootPromise = (async () => {
      const token = sessionStorage.getItem("raven.token");
      const email = sessionStorage.getItem("raven.email");
      if (token && email) set({ token, email });

      // Live first: probe the gateway; if unreachable, boot the demo simulator.
      const live = await probeGateway();
      const src: Source = live ? new LiveSource(() => get().token) : new DemoSource();
      source = src;
      src.start({
        onEvent: (e) => set((s) => ({ events: [e, ...s.events].slice(0, 100) })),
        onSnapshot: (snap) =>
          set((s) => ({
            snapshot: snap,
            history: [
              ...s.history,
              {
                t: snap.at,
                active: snap.totals.active_jobs,
                queue: snap.totals.queue_depth,
                ws: snap.totals.ws_connections,
              },
            ].slice(-300),
          })),
        onConn: (conn) => set({ conn }),
      });
      set({ mode: live ? "live" : "demo" });
    })();
    return bootPromise;
  },

  async signIn(email, password) {
    if (get().mode === "demo") {
      // Demo mode: authentication is simulated, any credentials work.
      await new Promise((r) => setTimeout(r, 400));
      const token = `demo.${btoa(email).replace(/=+$/, "")}`;
      sessionStorage.setItem("raven.token", token);
      sessionStorage.setItem("raven.email", email);
      set({ token, email });
      return;
    }
    const res = await login(email, password);
    sessionStorage.setItem("raven.token", res.access_token);
    sessionStorage.setItem("raven.email", email);
    set({ token: res.access_token, email });
  },

  signOut() {
    sessionStorage.removeItem("raven.token");
    sessionStorage.removeItem("raven.email");
    set({ token: null, email: null });
  },

  async loadJobs(q, opts) {
    await get().boot(); // effects in pages can run before App's boot effect
    if (!source) return;
    if (!opts?.silent) {
      set((s) => ({
        jobsPage: { status: "loading", items: s.jobsPage.items, total: s.jobsPage.total },
      }));
    }
    try {
      const page = await source.queryJobs(q);
      set({ jobsPage: { status: "ready", items: page.items, total: page.total } });
    } catch (err) {
      set((s) => ({
        jobsPage: {
          status: "error",
          items: s.jobsPage.items,
          total: s.jobsPage.total,
          message: err instanceof Error ? err.message : "Failed to load jobs",
        },
      }));
    }
  },

  async getJob(id) {
    await get().boot();
    return source ? source.getJob(id) : null;
  },

  async createJob(input) {
    await get().boot();
    if (!source) throw new Error("Console is still starting");
    return source.createJob(input);
  },

  async cancelJob(id) {
    await get().boot();
    if (!source) throw new Error("Console is still starting");
    return source.cancelJob(id);
  },

  async requeueJob(id) {
    await get().boot();
    if (!source) throw new Error("Console is still starting");
    return source.requeueJob(id);
  },

  async metricsSummary() {
    await get().boot();
    if (!source) throw new Error("Console is still starting");
    return source.metricsSummary();
  },

  setPaletteOpen: (open) => set({ paletteOpen: open }),
  setSignInOpen: (open) => set({ signInOpen: open }),
  setCreateJobOpen: (open) => set({ createJobOpen: open }),
}));
