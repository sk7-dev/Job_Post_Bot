import { ExternalLink } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { Badge } from "./ui/Badge";
import { RelativeTime } from "./RelativeTime";

export function JobTable({ jobs }: { jobs: JobRecord[] }) {
  return (
    <div className="overflow-x-auto rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)]">
      <table className="w-full min-w-[760px] text-left text-sm">
        <thead>
          <tr className="border-b border-slate-200 bg-slate-50 text-xs font-medium text-slate-500">
            <th className="px-5 py-3.5 font-medium">Job</th>
            <th className="px-5 py-3.5 font-medium">Company</th>
            <th className="px-5 py-3.5 font-medium">Location</th>
            <th className="px-5 py-3.5 font-medium">Match</th>
            <th className="px-5 py-3.5 font-medium">Source</th>
            <th className="px-5 py-3.5 font-medium">Found</th>
            <th className="px-5 py-3.5 font-medium text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {jobs.map((job) => (
            <tr key={job.key} className="transition-colors hover:bg-slate-50">
              <td className="max-w-[260px] px-5 py-3.5">
                <div className="flex items-center gap-2">
                  <span className="truncate font-medium text-slate-900">{job.title}</span>
                  {job.is_new ? <Badge tone="blue">New</Badge> : null}
                </div>
              </td>
              <td className="max-w-[180px] truncate px-5 py-3.5 text-slate-500">{job.company}</td>
              <td className="max-w-[180px] truncate px-5 py-3.5 text-slate-500">{job.location || "—"}</td>
              <td className="px-5 py-3.5">
                {job.matched_keywords?.length ? (
                  <Badge tone="neutral">{job.matched_keywords[0]}</Badge>
                ) : (
                  <span className="text-slate-400">—</span>
                )}
              </td>
              <td className="max-w-[160px] truncate px-5 py-3.5 text-slate-500">{job.source_name}</td>
              <td className="whitespace-nowrap px-5 py-3.5 text-slate-500">
                <RelativeTime iso={job.first_seen} />
              </td>
              <td className="px-5 py-3.5 text-right">
                <a
                  href={job.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 whitespace-nowrap rounded-lg border border-[var(--border-subtle)] bg-white px-2.5 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:bg-slate-50"
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
