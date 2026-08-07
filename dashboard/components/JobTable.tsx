import { ExternalLink } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { Badge } from "./ui/Badge";
import { RelativeTime } from "./RelativeTime";

export function JobTable({ jobs }: { jobs: JobRecord[] }) {
  return (
    <div className="overflow-x-auto rounded-xl border border-zinc-200 dark:border-zinc-800">
      <table className="w-full min-w-[760px] text-left text-sm">
        <thead>
          <tr className="border-b border-zinc-200 text-xs font-medium text-zinc-500 dark:border-zinc-800 dark:text-zinc-400">
            <th className="px-4 py-3 font-medium">Job</th>
            <th className="px-4 py-3 font-medium">Company</th>
            <th className="px-4 py-3 font-medium">Location</th>
            <th className="px-4 py-3 font-medium">Match</th>
            <th className="px-4 py-3 font-medium">Source</th>
            <th className="px-4 py-3 font-medium">Found</th>
            <th className="px-4 py-3 font-medium text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800">
          {jobs.map((job) => (
            <tr key={job.key} className="hover:bg-zinc-50 dark:hover:bg-zinc-900/60">
              <td className="max-w-[260px] px-4 py-3">
                <div className="flex items-center gap-2">
                  <span className="truncate font-medium text-zinc-900 dark:text-zinc-100">{job.title}</span>
                  {job.is_new ? <Badge tone="blue">New</Badge> : null}
                </div>
              </td>
              <td className="max-w-[180px] truncate px-4 py-3 text-zinc-600 dark:text-zinc-400">{job.company}</td>
              <td className="max-w-[180px] truncate px-4 py-3 text-zinc-600 dark:text-zinc-400">{job.location || "—"}</td>
              <td className="px-4 py-3">
                {job.matched_keywords?.length ? (
                  <Badge tone="neutral">{job.matched_keywords[0]}</Badge>
                ) : (
                  <span className="text-zinc-400 dark:text-zinc-600">—</span>
                )}
              </td>
              <td className="max-w-[160px] truncate px-4 py-3 text-zinc-600 dark:text-zinc-400">{job.source_name}</td>
              <td className="whitespace-nowrap px-4 py-3 text-zinc-500 dark:text-zinc-400">
                <RelativeTime iso={job.first_seen} />
              </td>
              <td className="px-4 py-3 text-right">
                <a
                  href={job.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 rounded-lg border border-zinc-200 px-2.5 py-1.5 text-xs font-medium text-zinc-700 hover:bg-zinc-50 dark:border-zinc-700 dark:text-zinc-300 dark:hover:bg-zinc-800"
                >
                  View Job
                  <ExternalLink className="size-3" aria-hidden />
                </a>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
