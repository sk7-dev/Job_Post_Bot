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
}: {
  label: string;
  value: ReactNode;
  icon?: LucideIcon;
  tone?: StatTone;
  hint?: ReactNode;
  valueClassName?: string;
}) {
  return (
    <div className="flex items-center gap-2.5 rounded-[14px] border border-[var(--border-subtle)] bg-white p-4 shadow-[0_1px_2px_rgba(15,23,42,0.04),0_1px_3px_rgba(15,23,42,0.06)] sm:p-5">
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
