import { Clock3, Radio, Sparkles, CalendarDays } from "lucide-react";
import { getJobs, getOverview, getSources } from "@/lib/github-data";
import { StatCard } from "@/components/StatCard";
import { JobCard } from "@/components/JobCard";
import { StatusBadge } from "@/components/StatusBadge";
import { RelativeTime } from "@/components/RelativeTime";
import { EmptyState } from "@/components/EmptyState";
import { ErrorState } from "@/components/ErrorState";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";

export default async function DashboardPage() {
  const [overview, jobs, sources] = await Promise.all([getOverview(), getJobs(), getSources()]);

  const healthyRingPercent =
    overview.ok && overview.data.total_sources > 0
      ? (overview.data.healthy_sources / overview.data.total_sources) * 100
      : undefined;

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-[26px] font-semibold leading-tight text-zinc-100">Dashboard</h1>
        <p className="mt-1 text-sm text-zinc-500">At-a-glance view of your job watcher bot.</p>
      </div>

      {overview.ok ? (
        <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
          <StatCard label="New Jobs" value={overview.data.new_jobs_last_scan} icon={Sparkles} tone="blue" />
          <StatCard label="Jobs Today" value={overview.data.jobs_today} icon={CalendarDays} />
          <StatCard
            label="Healthy Sources"
            value={`${overview.data.healthy_sources}/${overview.data.total_sources}`}
            icon={Radio}
            tone={overview.data.failed_sources > 0 ? "amber" : "green"}
            ringPercent={healthyRingPercent}
          />
          <StatCard label="Last Scan" value={<RelativeTime iso={overview.data.last_scan} />} icon={Clock3} />
        </div>
      ) : (
        <ErrorState kind={overview.kind} message={overview.error} />
      )}

      <div className="grid gap-5 lg:grid-cols-[1fr_360px]">
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
              <div className="space-y-1">
                {jobs.data.slice(0, 8).map((job) => (
                  <JobCard key={job.key} job={job} />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Source Health</CardTitle>
          </CardHeader>
          <CardContent>
            {!sources.ok ? (
              <ErrorState kind={sources.kind} message={sources.error} />
            ) : sources.data.length === 0 ? (
              <EmptyState title="No sources configured." />
            ) : (
              <ul className="divide-y divide-white/[0.04]">
                {sources.data.map((source) => (
                  <li key={source.name} className="flex items-center justify-between gap-3 py-3 text-sm">
                    <span className="truncate text-zinc-300">{source.name}</span>
                    <StatusBadge status={source.status} />
                  </li>
                ))}
              </ul>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
