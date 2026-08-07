import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Card } from "./ui/Card";

export function StatCard({
  label,
  value,
  icon: Icon,
  tone = "neutral",
  hint,
}: {
  label: string;
  value: ReactNode;
  icon?: LucideIcon;
  tone?: "neutral" | "green" | "amber" | "red" | "blue";
  hint?: string;
}) {
  const toneClass =
    tone === "green"
      ? "text-emerald-600 dark:text-emerald-400"
      : tone === "amber"
        ? "text-amber-600 dark:text-amber-400"
        : tone === "red"
          ? "text-red-600 dark:text-red-400"
          : tone === "blue"
            ? "text-blue-600 dark:text-blue-400"
            : "text-zinc-900 dark:text-zinc-100";

  return (
    <Card className="p-4">
      <div className="flex items-start justify-between">
        <div className={cn("text-2xl font-semibold tabular-nums", toneClass)}>{value}</div>
        {Icon ? <Icon className="size-4 text-zinc-400 dark:text-zinc-600" aria-hidden /> : null}
      </div>
      <div className="mt-1 text-sm text-zinc-500 dark:text-zinc-400">{label}</div>
      {hint ? <div className="mt-0.5 text-xs text-zinc-400 dark:text-zinc-600">{hint}</div> : null}
    </Card>
  );
}
