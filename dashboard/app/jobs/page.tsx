import { getJobs } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { JobsClient } from "./JobsClient";

export default async function JobsPage() {
  const jobs = await getJobs();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-[26px] font-semibold leading-tight text-zinc-100">Jobs</h1>
        <p className="mt-1 text-sm text-zinc-500">
          Every matching job discovered in the last 90 days.
        </p>
      </div>

      {!jobs.ok ? (
        <ErrorState kind={jobs.kind} message={jobs.error} />
      ) : jobs.data.length === 0 ? (
        <EmptyState title="No matching jobs found." description="Matches will show up here once the bot finds something." />
      ) : (
        <JobsClient jobs={jobs.data} />
      )}
    </div>
  );
}
