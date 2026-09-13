import { forwardRef, type SelectHTMLAttributes } from "react";
import { ChevronDown } from "lucide-react";
import { cn } from "@/lib/utils";

// Styled native <select> — full keyboard/screen-reader behavior for free.
export const Select = forwardRef<HTMLSelectElement, SelectHTMLAttributes<HTMLSelectElement>>(
  function Select({ className, children, ...props }, ref) {
    return (
      <span className={cn("relative inline-flex items-center", className)}>
        <select
          ref={ref}
          className="h-8 appearance-none rounded-md border border-border bg-bg pl-3 pr-8 text-sm text-fg transition-colors duration-150 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent disabled:cursor-not-allowed disabled:opacity-50"
          {...props}
        >
          {children}
        </select>
        <ChevronDown className="pointer-events-none absolute right-2 h-4 w-4 text-muted" aria-hidden />
      </span>
    );
  },
);
