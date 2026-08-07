import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

type ChipTone = "neutral" | "include" | "exclude";

const TONE_CLASSES: Record<ChipTone, string> = {
  neutral: "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300",
  include: "bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-400",
  exclude: "bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-400",
};

export function FilterChip({ tone = "neutral", children }: { tone?: ChipTone; children: ReactNode }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-md px-2.5 py-1 text-sm font-medium",
        TONE_CLASSES[tone]
      )}
    >
      {children}
    </span>
  );
}
