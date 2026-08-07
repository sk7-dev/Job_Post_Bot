import { cn } from "@/lib/utils";
import type { HTMLAttributes } from "react";

type BadgeTone = "neutral" | "green" | "amber" | "red" | "blue";

const TONE_CLASSES: Record<BadgeTone, string> = {
  neutral:
    "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300",
  green:
    "bg-emerald-50 text-emerald-700 dark:bg-emerald-500/10 dark:text-emerald-400",
  amber:
    "bg-amber-50 text-amber-700 dark:bg-amber-500/10 dark:text-amber-400",
  red: "bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-400",
  blue: "bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-400",
};

interface BadgeProps extends HTMLAttributes<HTMLSpanElement> {
  tone?: BadgeTone;
}

export function Badge({ tone = "neutral", className, ...props }: BadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-medium",
        TONE_CLASSES[tone],
        className
      )}
      {...props}
    />
  );
}
