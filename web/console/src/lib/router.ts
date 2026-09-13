import { useCallback, useEffect, useMemo, useState } from "react";

// Minimal hash router — filters and selected rows live in the URL so views
// are shareable and the back button works, without a routing dependency.

export interface Route {
  path: string;
  query: URLSearchParams;
  navigate: (to: string) => void;
}

function parseHash(hash: string): { path: string; query: URLSearchParams } {
  const raw = hash.replace(/^#/, "") || "/overview";
  const qIndex = raw.indexOf("?");
  const path = qIndex === -1 ? raw : raw.slice(0, qIndex);
  const qs = qIndex === -1 ? "" : raw.slice(qIndex + 1);
  return { path: path || "/overview", query: new URLSearchParams(qs) };
}

/** Replace the current hash without pushing a history entry (search typing). */
export function replaceHash(to: string): void {
  window.history.replaceState(null, "", to.startsWith("#") ? to : `#${to}`);
  window.dispatchEvent(new HashChangeEvent("hashchange"));
}

export function useHashRoute(): Route {
  const [hash, setHash] = useState(() => window.location.hash);

  useEffect(() => {
    const onChange = () => setHash(window.location.hash);
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  const navigate = useCallback((to: string) => {
    window.location.hash = to.startsWith("#") ? to.slice(1) : to;
  }, []);

  return useMemo(() => ({ ...parseHash(hash), navigate }), [hash, navigate]);
}
