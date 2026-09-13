import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  Activity,
  BarChart3,
  Cpu,
  ExternalLink,
  Layers,
  LogIn,
  LogOut,
  Plus,
  Waypoints,
  Wrench,
  type LucideIcon,
} from "lucide-react";
import type { Route } from "@/lib/router";
import { useConsole } from "@/lib/store";
import { config } from "@/lib/config";
import { cn } from "@/lib/utils";

interface PaletteItem {
  id: string;
  label: string;
  hint: string;
  icon: LucideIcon;
  run: () => void;
}

export function CommandPalette({ route }: { route: Route }) {
  const open = useConsole((s) => s.paletteOpen);
  const setOpen = useConsole((s) => s.setPaletteOpen);
  const email = useConsole((s) => s.email);
  const setSignInOpen = useConsole((s) => s.setSignInOpen);
  const setCreateJobOpen = useConsole((s) => s.setCreateJobOpen);
  const signOut = useConsole((s) => s.signOut);

  const [query, setQuery] = useState("");
  const [index, setIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const items = useMemo<PaletteItem[]>(() => {
    const nav = [
      { path: "/overview", label: "Go to Overview", icon: Activity },
      { path: "/jobs", label: "Go to Jobs", icon: Layers },
      { path: "/workers", label: "Go to Workers", icon: Cpu },
      { path: "/broker", label: "Go to Broker", icon: Waypoints },
      { path: "/observability", label: "Go to Observability", icon: BarChart3 },
      { path: "/testlab", label: "Go to Test Lab", icon: Wrench },
    ].map((n) => ({
      id: n.path,
      label: n.label,
      hint: "Page",
      icon: n.icon,
      run: () => route.navigate(n.path),
    }));
    const actions: PaletteItem[] = [
      {
        id: "create-job",
        label: "Create job",
        hint: "Action",
        icon: Plus,
        run: () => {
          route.navigate("/jobs");
          setCreateJobOpen(true);
        },
      },
      email
        ? { id: "sign-out", label: `Sign out (${email})`, hint: "Session", icon: LogOut, run: signOut }
        : {
            id: "sign-in",
            label: "Sign in",
            hint: "Session",
            icon: LogIn,
            run: () => setSignInOpen(true),
          },
      {
        id: "grafana",
        label: "Open Grafana",
        hint: "External",
        icon: ExternalLink,
        run: () => window.open(config.grafanaUrl, "_blank", "noopener"),
      },
      {
        id: "jaeger",
        label: "Open Jaeger",
        hint: "External",
        icon: ExternalLink,
        run: () => window.open(config.jaegerUrl, "_blank", "noopener"),
      },
      {
        id: "prometheus",
        label: "Open Prometheus",
        hint: "External",
        icon: ExternalLink,
        run: () => window.open(config.prometheusUrl, "_blank", "noopener"),
      },
    ];
    return [...nav, ...actions];
  }, [email, route, setCreateJobOpen, setSignInOpen, signOut]);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return items;
    return items.filter((i) => i.label.toLowerCase().includes(q));
  }, [items, query]);

  useEffect(() => {
    if (open) {
      setQuery("");
      setIndex(0);
      // autofocus after mount
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  if (!open) return null;

  const close = () => setOpen(false);
  const runItem = (item: PaletteItem) => {
    close();
    item.run();
  };

  return createPortal(
    <div className="fixed inset-0 z-50 flex items-start justify-center p-4 pt-24">
      <div className="absolute inset-0 bg-black/60" onClick={close} aria-hidden />
      <div
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        className="relative w-full max-w-lg overflow-hidden rounded-xl border border-border bg-surface shadow-lg"
      >
        <input
          ref={inputRef}
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setIndex(0);
          }}
          onKeyDown={(e) => {
            if (e.key === "Escape") close();
            else if (e.key === "ArrowDown") {
              e.preventDefault();
              setIndex((i) => Math.min(filtered.length - 1, i + 1));
            } else if (e.key === "ArrowUp") {
              e.preventDefault();
              setIndex((i) => Math.max(0, i - 1));
            } else if (e.key === "Enter" && filtered[index]) {
              runItem(filtered[index]);
            }
          }}
          placeholder="Type a command or search pages…"
          aria-label="Search commands"
          className="h-12 w-full border-b border-border bg-transparent px-4 text-sm text-fg placeholder:text-subtle focus:outline-none"
        />
        <ul role="listbox" className="max-h-80 overflow-y-auto p-2">
          {filtered.length === 0 && (
            <li className="px-3 py-6 text-center text-sm text-muted">No commands match “{query}”.</li>
          )}
          {filtered.map((item, i) => (
            <li key={item.id}>
              <button
                type="button"
                role="option"
                aria-selected={i === index}
                onMouseEnter={() => setIndex(i)}
                onClick={() => runItem(item)}
                className={cn(
                  "flex w-full items-center gap-3 rounded-md px-3 py-2 text-left text-sm transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent",
                  i === index ? "bg-surface-2 text-fg" : "text-muted",
                )}
              >
                <item.icon className="h-4 w-4 shrink-0" aria-hidden />
                <span className="flex-1 truncate">{item.label}</span>
                <span className="text-xs text-subtle">{item.hint}</span>
              </button>
            </li>
          ))}
        </ul>
      </div>
    </div>,
    document.body,
  );
}
