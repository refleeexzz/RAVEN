import { forwardRef, type InputHTMLAttributes } from "react";
import { cn } from "@/lib/utils";

// Styled native range input — keyboard accessible for free.
export const Slider = forwardRef<HTMLInputElement, InputHTMLAttributes<HTMLInputElement>>(
  function Slider({ className, ...props }, ref) {
    return (
      <input
        ref={ref}
        type="range"
        className={cn(
          "h-8 w-full cursor-pointer appearance-none bg-transparent accent-accent",
          "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent focus-visible:ring-offset-2 focus-visible:ring-offset-bg",
          className,
        )}
        {...props}
      />
    );
  },
);
