import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { Card } from "./ui/card";

export function StatCard({
  label,
  value,
  sub,
  icon: Icon,
  children,
}: {
  label: string;
  value: ReactNode;
  sub?: ReactNode;
  icon: LucideIcon;
  children?: ReactNode;
}) {
  return (
    <Card className="p-4">
      <div className="flex items-center justify-between gap-2">
        <span className="text-xs font-medium uppercase tracking-wide text-muted">{label}</span>
        <Icon className="h-4 w-4 text-subtle" aria-hidden />
      </div>
      <div className="mt-2 text-2xl font-semibold tabular-nums text-fg">{value}</div>
      {sub && <div className="mt-0.5 text-xs text-muted">{sub}</div>}
      {children && <div className="mt-2">{children}</div>}
    </Card>
  );
}
