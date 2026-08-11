import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";

type StatTone = "neutral" | "green" | "amber" | "red" | "blue" | "violet";

const ICON_CIRCLE_TONE: Record<StatTone, string> = {
  neutral: "bg-slate-400",
  green: "bg-[var(--healthy)]",
  amber: "bg-[var(--warning)]",
  red: "bg-[var(--failed)]",
  blue: "bg-[var(--accent)]",
  violet: "bg-[var(--violet)]",
};

export function StatCard({
  label,
  value,
  icon: Icon,
  tone = "neutral",
  hint,
  valueClassName,
  compact,
  className,
}: {
  label: string;
  value: ReactNode;
  icon?: LucideIcon;
  tone?: StatTone;
  hint?: ReactNode;
  valueClassName?: string;
  /** Condensed layout for tight spaces, e.g. a 2-column mobile KPI grid. */
  compact?: boolean;
  className?: string;
}) {
  if (compact) {
    return (
      <div
        className={cn(
          "rounded-xl border border-[var(--border-subtle)] bg-white p-2.5 shadow-[0_1px_2px_rgba(15,23,42,0.04),0_1px_3px_rgba(15,23,42,0.06)]",
          className
        )}
      >
        <div className="flex items-center gap-1.5">
          {Icon ? (
            <span className={cn("flex size-5 shrink-0 items-center justify-center rounded-full text-white", ICON_CIRCLE_TONE[tone])}>
              <Icon className="size-3" aria-hidden />
            </span>
          ) : null}
          <span className="truncate text-[11px] font-medium text-slate-500">{label}</span>
        </div>
        <div className="mt-1 truncate text-center text-lg font-bold leading-tight text-slate-900">{value}</div>
      </div>
    );
  }

  return (
    <div
      className={cn(
        "flex items-center gap-2.5 rounded-[14px] border border-[var(--border-subtle)] bg-white p-4 shadow-[0_1px_2px_rgba(15,23,42,0.04),0_1px_3px_rgba(15,23,42,0.06)] sm:p-5",
        className
      )}
    >
      {Icon ? (
        <span className={cn("flex size-8 shrink-0 items-center justify-center rounded-full text-white", ICON_CIRCLE_TONE[tone])}>
          <Icon className="size-3.5" aria-hidden />
        </span>
      ) : null}
      <div className="min-w-0">
        <div className="truncate text-[13px] font-medium text-slate-500">{label}</div>
        <div className={cn("font-bold leading-tight text-slate-900", valueClassName ?? "text-[22px] sm:text-[24px]")}>{value}</div>
        {hint ? <div className="mt-0.5 truncate text-xs text-slate-400">{hint}</div> : null}
      </div>
    </div>
  );
}
