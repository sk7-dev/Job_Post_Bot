import Link from "next/link";
import type { ReactNode } from "react";
import { Briefcase, TrendingUp, CalendarDays, type LucideIcon } from "lucide-react";
import { getActivity, getJobs, getOverview, getSources } from "@/lib/github-data";
import { StatCard } from "@/components/StatCard";
import { JobCard } from "@/components/JobCard";
import { SourceHealthRow } from "@/components/SourceHealthRow";
import { LatestRun } from "@/components/LatestRun";
import { ScanStatus } from "@/components/ScanStatus";
import { ScanNowButton } from "@/components/ScanNowButton";
import { EmptyState } from "@/components/EmptyState";
import { ErrorState } from "@/components/ErrorState";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";
import { LinkButton } from "@/components/ui/Button";
import { cn } from "@/lib/utils";

export default async function DashboardPage() {
  const [overview, jobs, sources, activity] = await Promise.all([
    getOverview(),
    getJobs(),
    getSources(),
    getActivity(),
  ]);

  const latestRun = activity.ok && activity.data.length > 0 ? activity.data[activity.data.length - 1] : null;
  const recentJobs = jobs.ok
    ? [...jobs.data]
        .sort((a, b) => new Date(b.first_seen).getTime() - new Date(a.first_seen).getTime())
        .slice(0, 4)
    : [];

  return (
    <div className="flex flex-col gap-4 sm:gap-6 md:h-[calc(100vh-3rem)] md:overflow-hidden">
      {/* ===== Mobile-only heading + scan action row ===== */}
      <div className="flex flex-col gap-3 md:hidden">
        <div>
          <h1 className="text-[24px] font-bold leading-tight tracking-tight text-[var(--text-primary)]">Dashboard</h1>
          <p className="mt-0.5 text-[13px] text-slate-500">At-a-glance view of your job watcher.</p>
        </div>
        <div className="flex items-center justify-between gap-3">
          <div className="min-w-0 overflow-hidden">
            <ScanStatus />
          </div>
          <ScanNowButton />
        </div>
      </div>

      {/* ===== Desktop-only heading, unchanged ===== */}
      <div className="hidden shrink-0 flex-wrap items-start justify-between gap-3 md:flex">
        <div>
          <h1 className="text-[26px] font-bold leading-tight text-[var(--text-primary)] sm:text-[30px] md:text-[34px]">Dashboard</h1>
          <p className="mt-1 text-sm text-slate-500">At-a-glance view of the job watcher bot.</p>
        </div>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
          <ScanStatus />
          <ScanNowButton />
        </div>
      </div>

      {/* ===== Mobile-only layout: KPI -> Recent Matches -> Last Scan -> Source Health ===== */}
      <div className="flex flex-col gap-4 md:hidden">
        {overview.ok ? (
          <div className="grid grid-cols-2 gap-2.5 min-[380px]:grid-cols-3">
            <KpiTile label="New Jobs" value={overview.data.new_jobs_last_scan} icon={Briefcase} tone="blue" />
            <KpiTile label="Jobs Today" value={overview.data.jobs_today} icon={CalendarDays} tone="green" />
            <KpiTile
              label="Healthy"
              value={`${overview.data.healthy_sources}/${overview.data.total_sources}`}
              icon={TrendingUp}
              tone={overview.data.failed_sources > 0 ? "amber" : "green"}
              className="col-span-2 min-[380px]:col-span-1"
            />
          </div>
        ) : (
          <ErrorState kind={overview.kind} message={overview.error} />
        )}

        <Card>
          <CardHeader>
            <CardTitle>Recent Matches</CardTitle>
          </CardHeader>
          <CardContent>
            {!jobs.ok ? (
              <ErrorState kind={jobs.kind} message={jobs.error} />
            ) : jobs.data.length === 0 ? (
              <EmptyState title="No matching jobs found." description="Matches will show up here once the bot finds something." />
            ) : (
              <>
                <div className="-mx-1.5 divide-y divide-slate-100/70">
                  {recentJobs.map((job) => (
                    <JobCard key={job.key} job={job} />
                  ))}
                </div>
                <LinkButton
                  href="/jobs"
                  variant="ghost"
                  size="sm"
                  className="mt-1 w-full justify-center text-slate-500"
                >
                  View more
                </LinkButton>
              </>
            )}
          </CardContent>
        </Card>

        <Card className="rounded-2xl">
          <CardHeader>
            <CardTitle>Last Scan</CardTitle>
            <Link
              href="/activity"
              className="shrink-0 text-xs font-medium text-slate-500 transition-colors hover:text-[var(--accent)]"
            >
              View previous runs →
            </Link>
          </CardHeader>
          <CardContent>
            {!activity.ok ? (
              <ErrorState kind={activity.kind} message={activity.error} />
            ) : !latestRun ? (
              <EmptyState title="No scan history yet." description="Runs will appear here after the bot's next scheduled scan." />
            ) : (
              <LatestRun run={latestRun} compact />
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Source Health</CardTitle>
            <Link href="/sources" className="text-sm font-medium text-[var(--accent)] hover:underline">
              View all →
            </Link>
          </CardHeader>
          <CardContent>
            {!sources.ok ? (
              <ErrorState kind={sources.kind} message={sources.error} />
            ) : sources.data.length === 0 ? (
              <EmptyState title="No sources configured." />
            ) : (
              <div className="divide-y divide-slate-100">
                {sources.data.map((source) => (
                  <SourceHealthRow key={source.name} source={source} />
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>

      {/* ===== Desktop layout: two columns, unchanged ===== */}
      <div className="hidden flex-1 gap-5 md:grid md:min-h-0 md:grid-cols-2">
        <div className="flex min-h-0 flex-col gap-5">
          <Card className="flex min-h-0 flex-1 flex-col overflow-hidden">
            <CardHeader className="shrink-0">
              <CardTitle>Recent Matches</CardTitle>
            </CardHeader>
            <CardContent className="flex min-h-0 flex-1 flex-col">
              {!jobs.ok ? (
                <ErrorState kind={jobs.kind} message={jobs.error} />
              ) : jobs.data.length === 0 ? (
                <EmptyState title="No matching jobs found." description="Matches will show up here once the bot finds something." />
              ) : (
                <>
                  <div className="min-h-0 flex-1 space-y-1 overflow-y-auto">
                    {recentJobs.map((job) => (
                      <JobCard key={job.key} job={job} />
                    ))}
                  </div>
                  <LinkButton href="/jobs" variant="outline" className="mt-3 w-full shrink-0">
                    View more
                  </LinkButton>
                </>
              )}
            </CardContent>
          </Card>
        </div>

        <div className="flex min-h-0 flex-col gap-5">
          {overview.ok ? (
            <div className="grid shrink-0 grid-cols-3 gap-3 sm:gap-4">
              <StatCard
                label="New Jobs"
                value={overview.data.new_jobs_last_scan}
                icon={Briefcase}
                tone="blue"
              />
              <StatCard label="Jobs Today" value={overview.data.jobs_today} icon={CalendarDays} tone="green" />
              <StatCard
                label="Healthy Sources"
                value={`${overview.data.healthy_sources} / ${overview.data.total_sources}`}
                icon={TrendingUp}
                tone={overview.data.failed_sources > 0 ? "amber" : "green"}
              />
            </div>
          ) : (
            <div className="shrink-0">
              <ErrorState kind={overview.kind} message={overview.error} />
            </div>
          )}

          <Card className="flex min-h-0 flex-1 flex-col overflow-hidden">
            <CardHeader className="shrink-0">
              <CardTitle>Last Scan</CardTitle>
              <Link href="/activity" className="text-sm font-medium text-[var(--accent)] hover:underline">
                View previous runs →
              </Link>
            </CardHeader>
            <CardContent className="min-h-0 flex-1 overflow-y-auto">
              {!activity.ok ? (
                <ErrorState kind={activity.kind} message={activity.error} />
              ) : !latestRun ? (
                <EmptyState title="No scan history yet." description="Runs will appear here after the bot's next scheduled scan." />
              ) : (
                <LatestRun run={latestRun} />
              )}
            </CardContent>
          </Card>

          <Card className="flex min-h-0 flex-1 flex-col overflow-hidden">
            <CardHeader className="shrink-0">
              <CardTitle>Source Health</CardTitle>
              <Link href="/sources" className="text-sm font-medium text-[var(--accent)] hover:underline">
                View all →
              </Link>
            </CardHeader>
            <CardContent className="min-h-0 flex-1 overflow-y-auto">
              {!sources.ok ? (
                <ErrorState kind={sources.kind} message={sources.error} />
              ) : sources.data.length === 0 ? (
                <EmptyState title="No sources configured." />
              ) : (
                <div className="divide-y divide-slate-100">
                  {sources.data.map((source) => (
                    <SourceHealthRow key={source.name} source={source} />
                  ))}
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

const KPI_ICON_TONE: Record<"blue" | "green" | "amber", string> = {
  blue: "bg-[var(--accent-soft)] text-[var(--accent)]",
  green: "bg-[var(--healthy-soft)] text-[var(--healthy)]",
  amber: "bg-[var(--warning-soft)] text-[var(--warning)]",
};

function KpiTile({
  label,
  value,
  icon: Icon,
  tone,
  className,
}: {
  label: string;
  value: ReactNode;
  icon: LucideIcon;
  tone: "blue" | "green" | "amber";
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col rounded-2xl border border-[var(--border-subtle)] bg-white p-2.5 shadow-[0_1px_2px_rgba(15,23,42,0.04),0_1px_3px_rgba(15,23,42,0.06)]",
        className
      )}
    >
      <div className="flex items-center gap-1">
        <span className={cn("flex size-5 shrink-0 items-center justify-center rounded-full", KPI_ICON_TONE[tone])}>
          <Icon className="size-3" aria-hidden />
        </span>
        <span className="truncate text-[11px] font-medium text-slate-500">{label}</span>
      </div>
      <p className="mt-2 text-center text-[22px] font-bold leading-none text-slate-900">{value}</p>
    </div>
  );
}
