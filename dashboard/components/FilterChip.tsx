import { cn } from "@/lib/utils";
import type { ReactNode } from "react";

type ChipTone = "neutral" | "include" | "exclude";

const TONE_CLASSES: Record<ChipTone, string> = {
  neutral: "bg-white/[0.05] text-zinc-300 border border-white/[0.08]",
  include: "bg-[var(--accent)]/10 text-[var(--accent)] border border-[var(--accent)]/25",
  exclude: "bg-[var(--failed)]/10 text-[var(--failed)] border border-[var(--failed)]/25",
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
