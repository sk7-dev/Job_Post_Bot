import { CheckCircle2, AlertTriangle, XCircle, type LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import type { ActivityRun } from "@/lib/types";
import { RelativeTime } from "./RelativeTime";
import { formatAbsoluteTime, formatDuration } from "@/lib/utils";

const STATUS_CONFIG: Record<ActivityRun["status"], { label: string; icon: LucideIcon; tone: string; soft: string }> = {
  success: { label: "Scan completed", icon: CheckCircle2, tone: "text-[var(--healthy)]", soft: "bg-[var(--healthy-soft)]" },
  partial: { label: "Scan partially completed", icon: AlertTriangle, tone: "text-[var(--warning)]", soft: "bg-[var(--warning-soft)]" },
  failed: { label: "Scan failed", icon: XCircle, tone: "text-[var(--failed)]", soft: "bg-[var(--failed-soft)]" },
};

export function LatestRun({ run }: { run: ActivityRun }) {
  const config = STATUS_CONFIG[run.status];
  const Icon = config.icon;

  return (
    <div>
      <div className="flex items-center gap-3">
        <span className={`flex size-11 shrink-0 items-center justify-center rounded-full ${config.soft}`}>
          <Icon className={`size-5 ${config.tone}`} aria-hidden />
        </span>
        <div className="min-w-0">
          <p className="text-sm font-semibold text-slate-900">{config.label}</p>
          <p className="mt-1 flex flex-wrap items-center gap-1.5 text-xs text-slate-400">
            <span className="rounded-full bg-[var(--accent-soft)] px-2 py-0.5 font-medium text-[var(--accent)]">
              <RelativeTime iso={run.timestamp} />
            </span>
            <span>· {formatAbsoluteTime(run.timestamp)}</span>
          </p>
        </div>
      </div>

      <dl className="mt-4 grid grid-cols-2 gap-2.5 sm:grid-cols-4">
        <Stat label="Fetched" value={run.jobs_fetched} />
        <Stat label="Matched" value={run.jobs_matched} />
        <Stat label="New" value={run.new_jobs} />
        <Stat label="Duration" value={formatDuration(run.duration_seconds)} />
      </dl>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: ReactNode }) {
  return (
    <div className="rounded-lg bg-slate-50 px-3 py-2">
      <dt className="text-[11px] text-slate-400">{label}</dt>
      <dd className="text-base font-semibold text-slate-900">{value}</dd>
    </div>
  );
}
