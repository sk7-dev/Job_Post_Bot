import Link from "next/link";
import { Briefcase, TrendingUp, CalendarDays } from "lucide-react";
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
    <div className="flex flex-col gap-6 md:h-[calc(100vh-3rem)] md:overflow-hidden">
      <div className="flex shrink-0 flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-[30px] font-bold leading-tight text-[var(--text-primary)] sm:text-[34px]">Dashboard</h1>
          <p className="mt-1 text-sm text-slate-500">At-a-glance view of the job watcher bot.</p>
        </div>
        <div className="flex items-center gap-4">
          <ScanStatus />
          <ScanNowButton />
        </div>
      </div>

      <div className="grid flex-1 gap-5 md:min-h-0 md:grid-cols-2">
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
