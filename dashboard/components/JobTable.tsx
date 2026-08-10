import { ExternalLink } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { Badge } from "./ui/Badge";
import { RelativeTime } from "./RelativeTime";

export function JobTable({ jobs }: { jobs: JobRecord[] }) {
  return (
    <div className="overflow-x-auto rounded-2xl border border-[var(--border-subtle)] bg-[var(--surface)]">
      <table className="w-full min-w-[760px] text-left text-sm">
        <thead>
          <tr className="border-b border-white/[0.06] text-xs font-medium text-zinc-500">
            <th className="px-5 py-3.5 font-medium">Job</th>
            <th className="px-5 py-3.5 font-medium">Company</th>
            <th className="px-5 py-3.5 font-medium">Location</th>
            <th className="px-5 py-3.5 font-medium">Match</th>
            <th className="px-5 py-3.5 font-medium">Source</th>
            <th className="px-5 py-3.5 font-medium">Found</th>
            <th className="px-5 py-3.5 font-medium text-right">Action</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-white/[0.04]">
          {jobs.map((job) => (
            <tr key={job.key} className="transition-colors hover:bg-white/[0.03]">
              <td className="max-w-[260px] px-5 py-3.5">
                <div className="flex items-center gap-2">
                  <span className="truncate font-medium text-zinc-100">{job.title}</span>
                  {job.is_new ? <Badge tone="blue">New</Badge> : null}
                </div>
              </td>
              <td className="max-w-[180px] truncate px-5 py-3.5 text-zinc-500">{job.company}</td>
              <td className="max-w-[180px] truncate px-5 py-3.5 text-zinc-500">{job.location || "—"}</td>
              <td className="px-5 py-3.5">
                {job.matched_keywords?.length ? (
                  <Badge tone="neutral">{job.matched_keywords[0]}</Badge>
                ) : (
                  <span className="text-zinc-600">—</span>
                )}
              </td>
              <td className="max-w-[160px] truncate px-5 py-3.5 text-zinc-500">{job.source_name}</td>
              <td className="whitespace-nowrap px-5 py-3.5 text-zinc-500">
                <RelativeTime iso={job.first_seen} />
              </td>
              <td className="px-5 py-3.5 text-right">
                <a
                  href={job.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 whitespace-nowrap rounded-[9px] border border-[var(--border-subtle)] bg-white/[0.02] px-2.5 py-1.5 text-xs font-medium text-zinc-300 transition-colors hover:border-[var(--accent)]/30 hover:bg-white/[0.05] hover:text-zinc-100"
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
