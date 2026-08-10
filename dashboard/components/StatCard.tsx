import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

export function StatCard({
  label,
  value,
  icon: Icon,
  tone = "neutral",
  hint,
  ringPercent,
}: {
  label: string;
  value: ReactNode;
  icon?: LucideIcon;
  tone?: "neutral" | "green" | "amber" | "red" | "blue";
  hint?: string;
  /** 0-100. When set, renders a small radial ring next to the value. */
  ringPercent?: number;
}) {
  const toneClass =
    tone === "green"
      ? "text-[var(--healthy)]"
      : tone === "amber"
        ? "text-[var(--warning)]"
        : tone === "red"
          ? "text-[var(--failed)]"
          : tone === "blue"
            ? "text-[var(--accent)]"
            : "text-zinc-100";

  return (
    <div className="flex min-h-[128px] flex-col justify-between rounded-2xl border border-[var(--border-subtle)] bg-gradient-to-b from-[var(--surface-elevated)] to-[var(--surface)] p-5 shadow-[0_15px_50px_rgba(0,0,0,0.25),0_1px_0_0_rgba(255,255,255,0.03)_inset]">
      <div className="flex items-start justify-between gap-2">
        <span className="text-[13px] text-zinc-500">{label}</span>
        {Icon ? <Icon className="size-4 shrink-0 text-zinc-600" aria-hidden /> : null}
      </div>
      <div className="flex items-end justify-between gap-3">
        <div className={cn("text-[28px] font-semibold tabular-nums leading-none", toneClass)}>{value}</div>
        {typeof ringPercent === "number" ? <RadialRing percent={ringPercent} /> : null}
      </div>
      {hint ? <div className="text-xs text-zinc-600">{hint}</div> : null}
    </div>
  );
}

function RadialRing({ percent }: { percent: number }) {
  const clamped = Math.max(0, Math.min(100, percent));
  const radius = 15;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (clamped / 100) * circumference;

  return (
    <svg width="36" height="36" viewBox="0 0 36 36" className="-rotate-90 shrink-0" aria-hidden>
      <circle cx="18" cy="18" r={radius} fill="none" stroke="rgba(255,255,255,0.08)" strokeWidth="3" />
      <circle
        cx="18"
        cy="18"
        r={radius}
        fill="none"
        stroke="var(--accent)"
        strokeWidth="3"
        strokeDasharray={circumference}
        strokeDashoffset={offset}
        strokeLinecap="round"
      />
    </svg>
  );
}
