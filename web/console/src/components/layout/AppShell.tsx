import { useState, type ReactNode } from "react";
import {
  Activity,
  BarChart3,
  Cpu,
  FlaskConical,
  Layers,
  LogIn,
  LogOut,
  Menu,
  Search,
  Waypoints,
  Wrench,
  X,
} from "lucide-react";
import type { Route } from "@/lib/router";
import { useConsole } from "@/lib/store";
import { cn } from "@/lib/utils";
import { Badge, Dot } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";

const NAV = [
  { path: "/overview", label: "Overview", icon: Activity },
  { path: "/jobs", label: "Jobs", icon: Layers },
  { path: "/workers", label: "Workers", icon: Cpu },
  { path: "/broker", label: "Broker", icon: Waypoints },
  { path: "/observability", label: "Observability", icon: BarChart3 },
  { path: "/testlab", label: "Test Lab", icon: Wrench },
] as const;

function pageTitle(path: string): string {
  return NAV.find((n) => n.path === path)?.label ?? "Overview";
}

function ConnectionBadge() {
  const mode = useConsole((s) => s.mode);
  const conn = useConsole((s) => s.conn);

  if (mode === "connecting") {
    return (
      <Badge variant="outline">
        <Dot variant="muted" pulse /> Connecting
      </Badge>
    );
  }
  if (mode === "demo") {
    return (
      <span title="The gateway is unreachable, so the console is running on a simulated feed.">
        <Badge variant="warning">
          <FlaskConical className="h-3 w-3" aria-hidden /> DEMO
        </Badge>
      </span>
    );
  }
  if (conn === "connected") {
    return (
      <Badge variant="success">
        <Dot variant="success" /> Live
      </Badge>
    );
  }
  if (conn === "offline") {
    // Auth-refused socket (close 4401): no retry loop, just the hint.
    return (
      <span title="The realtime feed needs a session. Sign in to get live updates.">
        <Badge variant="warning">
          <LogIn className="h-3 w-3" aria-hidden /> Sign in for live updates
        </Badge>
      </span>
    );
  }
  if (conn === "connecting") {
    return (
      <Badge variant="outline">
        <Dot variant="muted" pulse /> Connecting
      </Badge>
    );
  }
  return (
    <Badge variant="warning">
      <Dot variant="warning" pulse /> Reconnecting
    </Badge>
  );
}

function NavItems({ onNavigate }: { onNavigate?: () => void }) {
  const activeJobs = useConsole((s) => s.snapshot?.totals.active_jobs ?? 0);
  const hash = window.location.hash.replace(/^#/, "").split("?")[0] || "/overview";
  return (
    <nav className="flex flex-col gap-0.5 px-2" aria-label="Platform">
      {NAV.map((item) => {
        const active = hash === item.path;
        return (
          <a
            key={item.path}
            href={`#${item.path}`}
            onClick={onNavigate}
            aria-current={active ? "page" : undefined}
            className={cn(
              "flex h-8 items-center gap-2 rounded-md px-2 text-sm transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent",
              active ? "bg-surface-2 font-medium text-fg" : "text-muted hover:bg-surface-2/60 hover:text-fg",
            )}
          >
            <item.icon className="h-4 w-4 shrink-0" aria-hidden />
            <span className="flex-1 truncate">{item.label}</span>
            {item.path === "/jobs" && activeJobs > 0 && (
              <span className="rounded-sm bg-accent/15 px-1.5 text-xs tabular-nums text-accent-fg">
                {activeJobs}
              </span>
            )}
          </a>
        );
      })}
    </nav>
  );
}

export function AppShell({ route, children }: { route: Route; children: ReactNode }) {
  const email = useConsole((s) => s.email);
  const mode = useConsole((s) => s.mode);
  const setSignInOpen = useConsole((s) => s.setSignInOpen);
  const setPaletteOpen = useConsole((s) => s.setPaletteOpen);
  const signOut = useConsole((s) => s.signOut);
  const [mobileNav, setMobileNav] = useState(false);

  const sidebar = (
    <div className="flex h-full flex-col">
      <div className="flex h-12 items-center gap-2 border-b border-border px-4">
        <img src="/favicon.svg" alt="" width={20} height={20} className="rounded-sm" />
        <span className="text-sm font-semibold tracking-tight">RAVEN</span>
        <span className="text-sm text-subtle">Console</span>
      </div>
      <div className="flex-1 overflow-y-auto py-3">
        <p className="px-4 pb-1 text-xs font-medium uppercase tracking-wide text-subtle">Platform</p>
        <NavItems onNavigate={() => setMobileNav(false)} />
      </div>
      <div className="border-t border-border px-4 py-3">
        <p className="text-xs text-subtle">
          {mode === "demo" ? "Demo feed · simulated" : mode === "live" ? "Gateway · localhost:8080" : "Starting…"}
        </p>
        <p className="mt-0.5 text-xs text-subtle">v0.1.0</p>
      </div>
    </div>
  );

  return (
    <div className="flex h-full">
      {/* Desktop sidebar */}
      <aside className="hidden w-60 shrink-0 border-r border-border bg-bg md:block">{sidebar}</aside>

      {/* Mobile nav drawer */}
      {mobileNav && (
        <div className="fixed inset-0 z-50 md:hidden">
          <div className="absolute inset-0 bg-black/60" onClick={() => setMobileNav(false)} aria-hidden />
          <aside className="absolute left-0 top-0 h-full w-60 border-r border-border bg-bg">{sidebar}</aside>
        </div>
      )}

      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-12 shrink-0 items-center gap-3 border-b border-border bg-bg px-4">
          <Button
            variant="ghost"
            size="icon"
            className="md:hidden"
            aria-label={mobileNav ? "Close navigation" : "Open navigation"}
            onClick={() => setMobileNav((v) => !v)}
          >
            {mobileNav ? <X className="h-4 w-4" /> : <Menu className="h-4 w-4" />}
          </Button>
          <h1 className="text-sm font-semibold">{pageTitle(route.path)}</h1>

          <div className="ml-auto flex items-center gap-2">
            <button
              type="button"
              onClick={() => setPaletteOpen(true)}
              className="hidden h-8 items-center gap-2 rounded-md border border-border bg-surface px-3 text-sm text-muted transition-colors duration-150 hover:text-fg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent sm:flex"
            >
              <Search className="h-4 w-4" aria-hidden />
              <span>Commands</span>
              <kbd className="rounded-sm border border-border bg-bg px-1 text-xs text-subtle">⌘K</kbd>
            </button>
            <ConnectionBadge />
            {email ? (
              <div className="flex items-center gap-2">
                <span className="hidden items-center gap-2 sm:flex" title={email}>
                  <span className="flex h-6 w-6 items-center justify-center rounded-full bg-accent text-xs font-medium text-white">
                    {email.slice(0, 1).toUpperCase()}
                  </span>
                  <span className="max-w-32 truncate text-sm text-muted">{email}</span>
                </span>
                <Button variant="ghost" size="icon" aria-label="Sign out" onClick={signOut}>
                  <LogOut className="h-4 w-4" />
                </Button>
              </div>
            ) : (
              <Button variant="outline" size="sm" onClick={() => setSignInOpen(true)}>
                <LogIn className="h-4 w-4" aria-hidden />
                Sign in
              </Button>
            )}
          </div>
        </header>

        <main className="min-h-0 flex-1 overflow-y-auto">
          <div className="mx-auto max-w-[1400px] p-4 sm:p-6">{children}</div>
        </main>
      </div>
    </div>
  );
}
