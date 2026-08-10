import { ChevronRight } from "lucide-react";
import { getActivity } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { ScanStatus } from "@/components/ScanStatus";
import { StatusBadge } from "@/components/StatusBadge";
import { cn, formatDateTimeShort, formatDuration } from "@/lib/utils";

export default async function ActivityPage() {
  const activity = await getActivity();

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-[30px] font-bold leading-tight text-[var(--text-primary)] sm:text-[34px]">Activity</h1>
          <p className="mt-1 text-sm text-slate-500">Recent scan runs, newest first.</p>
        </div>
        <ScanStatus />
      </div>

      {!activity.ok ? (
        <ErrorState kind={activity.kind} message={activity.error} />
      ) : activity.data.length === 0 ? (
        <EmptyState title="No scan history yet." description="Runs will appear here after the bot's next scheduled scan." />
      ) : (
        <div className="space-y-2">
          {[...activity.data].reverse().map((run, i) => (
            <details
              key={`${run.timestamp}-${i}`}
              className="group rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)]"
            >
              <summary className="flex cursor-pointer list-none flex-wrap items-center justify-between gap-2 px-5 py-4 [&::-webkit-details-marker]:hidden">
                <div className="flex items-center gap-2.5">
                  <ChevronRight className="size-4 shrink-0 text-slate-400 transition-transform group-open:rotate-90" aria-hidden />
                  <span className="text-sm font-medium text-slate-900">
                    {formatDateTimeShort(run.timestamp)}
                  </span>
                  <StatusBadge status={run.status} />
                </div>
                <p className="text-xs text-slate-500">
                  {run.jobs_fetched} fetched · {run.jobs_matched} matched · {run.new_jobs} new
                  {run.sources_failed > 0 ? ` · ${run.sources_failed} source${run.sources_failed === 1 ? "" : "s"} failed` : ""}
                  {run.alerts_sent > 0 ? ` · ${run.alerts_sent} alert${run.alerts_sent === 1 ? "" : "s"}` : ""}
                </p>
              </summary>
              <div className="border-t border-slate-100 px-5 py-4">
                <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm sm:grid-cols-4">
                  <div>
                    <dt className="text-xs text-slate-400">Duration</dt>
                    <dd className="text-slate-700">{formatDuration(run.duration_seconds)}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-slate-400">Sources</dt>
                    <dd className="text-slate-700">
                      {run.sources_succeeded}/{run.sources_total} succeeded
                    </dd>
                  </div>
                  <div>
                    <dt className="text-xs text-slate-400">Jobs matched</dt>
                    <dd className="text-slate-700">{run.jobs_matched}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-slate-400">Alerts sent</dt>
                    <dd className="text-slate-700">{run.alerts_sent}</dd>
                  </div>
                </dl>

                {run.sources && run.sources.length > 0 ? (
                  <ul className="mt-3 divide-y divide-slate-100 border-t border-slate-100">
                    {run.sources.map((s) => (
                      <li key={s.name} className="flex items-center justify-between gap-3 py-2.5 text-sm">
                        <span className={cn("truncate text-slate-700", s.status === "failed" && "text-[var(--failed)]")}>
                          {s.name}
                        </span>
                        <span className="flex items-center gap-2 text-xs text-slate-500">
                          {s.jobs_fetched} fetched · {s.jobs_matched} matched
                          <StatusBadge status={s.status} />
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="mt-3 text-xs text-slate-400">No per-source detail available for this run.</p>
                )}
              </div>
            </details>
          ))}
        </div>
      )}
    </div>
  );
}
