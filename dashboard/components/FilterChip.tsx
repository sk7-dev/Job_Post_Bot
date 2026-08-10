import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

type ChipTone = "neutral" | "include" | "exclude";

const TONE_CLASSES: Record<ChipTone, string> = {
  neutral: "bg-slate-100 text-slate-600 border border-slate-200",
  include: "bg-[var(--accent-soft)] text-[var(--accent)] border border-blue-200",
  exclude: "bg-[var(--failed-soft)] text-[var(--failed)] border border-red-200",
};

export function FilterChip({ tone = "neutral", children }: { tone?: ChipTone; children: ReactNode }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-full px-3 py-1 text-sm font-medium",
        TONE_CLASSES[tone]
      )}
    >
      {children}
    </span>
  );
}
