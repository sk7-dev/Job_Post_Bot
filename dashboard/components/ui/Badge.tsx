import { cn } from "@/lib/utils";
import type { HTMLAttributes } from "react";

type BadgeTone = "neutral" | "green" | "amber" | "red" | "blue";

const TONE_CLASSES: Record<BadgeTone, string> = {
  neutral: "bg-white/[0.05] text-zinc-300 border border-white/[0.08]",
  green: "bg-[var(--healthy)]/10 text-[var(--healthy)] border border-[var(--healthy)]/25",
  amber: "bg-[var(--warning)]/10 text-[var(--warning)] border border-[var(--warning)]/25",
  red: "bg-[var(--failed)]/10 text-[var(--failed)] border border-[var(--failed)]/25",
  blue: "bg-[var(--accent)]/10 text-[var(--accent)] border border-[var(--accent)]/25",
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
