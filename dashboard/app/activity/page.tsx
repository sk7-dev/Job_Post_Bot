import { ChevronRight } from "lucide-react";
import { getActivity } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { StatusBadge } from "@/components/StatusBadge";
import { cn, formatDateTimeShort, formatDuration } from "@/lib/utils";

export default async function ActivityPage() {
  const activity = await getActivity();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Activity</h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">Recent scan runs, newest first.</p>
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
              className="group rounded-xl border border-zinc-200 dark:border-zinc-800"
            >
              <summary className="flex cursor-pointer list-none flex-wrap items-center justify-between gap-2 px-4 py-3.5 [&::-webkit-details-marker]:hidden">
                <div className="flex items-center gap-2.5">
                  <ChevronRight className="size-4 shrink-0 text-zinc-400 transition-transform group-open:rotate-90" aria-hidden />
                  <span className="text-sm font-medium text-zinc-900 dark:text-zinc-100">
                    {formatDateTimeShort(run.timestamp)}
                  </span>
                  <StatusBadge status={run.status} />
                </div>
                <p className="text-xs text-zinc-500 dark:text-zinc-400">
                  {run.jobs_fetched} fetched · {run.jobs_matched} matched · {run.new_jobs} new
                  {run.sources_failed > 0 ? ` · ${run.sources_failed} source${run.sources_failed === 1 ? "" : "s"} failed` : ""}
                  {run.alerts_sent > 0 ? ` · ${run.alerts_sent} alert${run.alerts_sent === 1 ? "" : "s"}` : ""}
                </p>
              </summary>
              <div className="border-t border-zinc-100 px-4 py-3 dark:border-zinc-800">
                <dl className="grid grid-cols-2 gap-x-4 gap-y-2 text-sm sm:grid-cols-4">
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Duration</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">{formatDuration(run.duration_seconds)}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Sources</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">
                      {run.sources_succeeded}/{run.sources_total} succeeded
                    </dd>
                  </div>
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Jobs matched</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">{run.jobs_matched}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Alerts sent</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">{run.alerts_sent}</dd>
                  </div>
                </dl>

                {run.sources && run.sources.length > 0 ? (
                  <ul className="mt-3 divide-y divide-zinc-100 border-t border-zinc-100 dark:divide-zinc-800 dark:border-zinc-800">
                    {run.sources.map((s) => (
                      <li key={s.name} className="flex items-center justify-between gap-3 py-2 text-sm">
                        <span className={cn("truncate text-zinc-700 dark:text-zinc-300", s.status === "failed" && "text-red-600 dark:text-red-400")}>
                          {s.name}
                        </span>
                        <span className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
                          {s.jobs_fetched} fetched · {s.jobs_matched} matched
                          <StatusBadge status={s.status} />
                        </span>
                      </li>
                    ))}
                  </ul>
                ) : (
                  <p className="mt-3 text-xs text-zinc-400 dark:text-zinc-600">No per-source detail available for this run.</p>
                )}
              </div>
            </details>
          ))}
        </div>
      )}
    </div>
  );
}
