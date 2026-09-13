import { useSyncExternalStore } from "react";

// Minimal toast store — no dependency, used via the <Toaster /> component.

export type ToastVariant = "default" | "success" | "error";

export interface ToastItem {
  id: number;
  title: string;
  description?: string;
  variant: ToastVariant;
}

let toasts: ToastItem[] = [];
let seq = 0;
const listeners = new Set<() => void>();

function emit(): void {
  listeners.forEach((cb) => cb());
}

export function toast(t: Omit<ToastItem, "id">): void {
  const id = ++seq;
  toasts = [...toasts.slice(-4), { ...t, id }];
  emit();
  setTimeout(() => dismissToast(id), 5000);
}

export function dismissToast(id: number): void {
  if (!toasts.some((t) => t.id === id)) return;
  toasts = toasts.filter((t) => t.id !== id);
  emit();
}

function subscribe(cb: () => void): () => void {
  listeners.add(cb);
  return () => listeners.delete(cb);
}

export function useToasts(): ToastItem[] {
  return useSyncExternalStore(subscribe, () => toasts);
}
