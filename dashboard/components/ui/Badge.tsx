import { cn } from "@/lib/utils";
import type { HTMLAttributes } from "react";

type BadgeTone = "neutral" | "green" | "amber" | "red" | "blue";

const TONE_CLASSES: Record<BadgeTone, string> = {
  neutral: "bg-slate-100 text-slate-600 border border-slate-200",
  green: "bg-[var(--healthy-soft)] text-[var(--healthy)] border border-green-200",
  amber: "bg-[var(--warning-soft)] text-[var(--warning)] border border-amber-200",
  red: "bg-[var(--failed-soft)] text-[var(--failed)] border border-red-200",
  blue: "bg-[var(--accent-soft)] text-[var(--accent)] border border-blue-200",
};

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone;
}

export function Badge({ tone = "neutral", className, ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2.5 py-0.5 text-xs font-medium",
        TONE_CLASSES[tone],
        className
      )}
      {...props}
    />
  );
}
