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

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Dashboard</h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">At-a-glance view of your job watcher bot.</p>
      </div>

      {overview.ok ? (
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          <StatCard label="New Jobs" value={overview.data.new_jobs_last_scan} icon={Sparkles} tone="blue" />
          <StatCard label="Jobs Today" value={overview.data.jobs_today} icon={CalendarDays} />
          <StatCard
            label="Healthy Sources"
            value={`${overview.data.healthy_sources}/${overview.data.total_sources}`}
            icon={Radio}
            tone={overview.data.failed_sources > 0 ? "amber" : "green"}
          />
          <StatCard
            label="Last Scan"
            value={<RelativeTime iso={overview.data.last_scan} />}
            icon={Clock3}
          />
        </div>
      ) : (
        <ErrorState kind={overview.kind} message={overview.error} />
      )}

      <div className="grid gap-4 lg:grid-cols-3">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Recent matches</CardTitle>
          </CardHeader>
          <CardContent>
            {!jobs.ok ? (
              <ErrorState kind={jobs.kind} message={jobs.error} />
            ) : jobs.data.length === 0 ? (
              <EmptyState title="No matching jobs found." description="Matches will show up here once the bot finds something." />
            ) : (
              <div className="space-y-3">
                {jobs.data.slice(0, 8).map((job) => (
                  <JobCard key={job.key} job={job} />
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Source health</CardTitle>
          </CardHeader>
          <CardContent>
            {!sources.ok ? (
              <ErrorState kind={sources.kind} message={sources.error} />
            ) : sources.data.length === 0 ? (
              <EmptyState title="No sources configured." />
            ) : (
              <ul className="divide-y divide-zinc-100 dark:divide-zinc-800">
                {sources.data.map((source) => (
                  <li key={source.name} className="flex items-center justify-between gap-3 py-2.5 text-sm">
                    <span className="truncate text-zinc-700 dark:text-zinc-300">{source.name}</span>
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
