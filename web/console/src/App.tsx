import { useEffect } from "react";
import { useHashRoute } from "@/lib/router";
import { useConsole } from "@/lib/store";
import { AppShell } from "@/components/layout/AppShell";
import { CommandPalette } from "@/components/layout/CommandPalette";
import { SignInDialog } from "@/components/auth/SignInDialog";
import { Toaster } from "@/components/ui/toaster";
import { OverviewPage } from "@/pages/Overview";
import { JobsPage } from "@/pages/Jobs";
import { WorkersPage } from "@/pages/Workers";
import { BrokerPage } from "@/pages/Broker";
import { ObservabilityPage } from "@/pages/Observability";

export default function App() {
  const route = useHashRoute();

  useEffect(() => {
    void useConsole.getState().boot();
  }, []);

  // Global ⌘K / Ctrl+K for the command palette
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        const s = useConsole.getState();
        s.setPaletteOpen(!s.paletteOpen);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  let page: React.ReactNode;
  switch (route.path) {
    case "/jobs":
      page = <JobsPage route={route} />;
      break;
    case "/workers":
      page = <WorkersPage />;
      break;
    case "/broker":
      page = <BrokerPage route={route} />;
      break;
    case "/observability":
      page = <ObservabilityPage />;
      break;
    default:
      page = <OverviewPage />;
  }

  return (
    <>
      <AppShell route={route}>{page}</AppShell>
      <CommandPalette route={route} />
      <SignInDialog />
      <Toaster />
    </>
  );
}
