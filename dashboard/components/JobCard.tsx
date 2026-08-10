import { ExternalLink, MapPin } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { Badge } from "./ui/Badge";
import { RelativeTime } from "./RelativeTime";

export function JobCard({ job }: { job: JobRecord }) {
  return (
    <div className="rounded-[10px] px-4 py-3.5 transition-colors duration-150 hover:bg-slate-50">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <a
              href={job.url}
              target="_blank"
              rel="noopener noreferrer"
              className="truncate text-[14px] font-semibold text-[var(--accent)] hover:underline"
            >
              {job.title}
            </a>
            {job.is_new ? <Badge tone="blue">New</Badge> : null}
          </div>
          <p className="mt-0.5 truncate text-[13px] text-slate-500">{job.company}</p>
        </div>
        <a
          href={job.url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-lg border border-[var(--border-subtle)] bg-white px-2.5 py-1.5 text-xs font-medium text-slate-600 transition-colors hover:border-slate-300 hover:bg-slate-50"
        >
          View Job
          <ExternalLink className="size-3" aria-hidden />
        </a>
      </div>
      <div className="mt-2.5 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-xs text-slate-400">
        {job.location ? (
          <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2 py-0.5 text-slate-600">
            <MapPin className="size-3" aria-hidden />
            {job.location}
          </span>
        ) : null}
        {job.matched_keywords?.length ? (
          <span className="inline-flex items-center gap-1.5">
            {job.matched_keywords.slice(0, 3).map((k) => (
              <Badge key={k} tone="neutral">
                {k}
              </Badge>
            ))}
          </span>
        ) : null}
        <RelativeTime iso={job.first_seen} />
      </div>
    </div>
  );
}
