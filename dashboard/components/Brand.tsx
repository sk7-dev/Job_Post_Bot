import { Target } from "lucide-react";
import { cn } from "@/lib/utils";

interface BrandProps {
  tagline?: boolean;
  compact?: boolean;
}

export function Brand({ tagline = false, compact = false }: BrandProps) {
  return (
    <div className="flex items-center gap-2.5">
      <span
        className={cn(
          "flex shrink-0 items-center justify-center rounded-[10px] bg-[var(--accent-soft)]",
          compact ? "size-7" : "size-8"
        )}
      >
        <Target className={cn(compact ? "size-3.5" : "size-4", "text-[var(--accent)]")} aria-hidden />
      </span>
      <div className="flex flex-col leading-tight">
        <span className="text-[14px] font-bold tracking-tight">
          <span className="text-[var(--text-primary)]">Job</span>
          <span className="text-[var(--accent)]">Watcher</span>
        </span>
        {tagline ? (
          <span className="text-[10px] font-medium text-[var(--text-muted)]">Automated job discovery</span>
        ) : null}
      </div>
    </div>
  );
}
