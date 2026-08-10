import { ExternalLink, MapPin } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { Badge } from "./ui/Badge";
import { RelativeTime } from "./RelativeTime";

export function JobCard({ job }: { job: JobRecord }) {
  return (
    <div className="rounded-xl px-4 py-3.5 transition-all duration-150 hover:-translate-y-0.5 hover:bg-white/[0.03]">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <p className="truncate text-[14px] font-semibold text-zinc-100">{job.title}</p>
            {job.is_new ? <Badge tone="blue">New</Badge> : null}
          </div>
          <p className="mt-0.5 truncate text-[13px] text-zinc-500">{job.company}</p>
        </div>
        <a
          href={job.url}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded-[9px] border border-[var(--border-subtle)] bg-white/[0.02] px-2.5 py-1.5 text-xs font-medium text-zinc-300 transition-colors hover:border-[var(--accent)]/30 hover:bg-white/[0.05] hover:text-zinc-100"
        >
          View Job
          <ExternalLink className="size-3" aria-hidden />
        </a>
      </div>
      <div className="mt-2.5 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-xs text-zinc-500">
        {job.location ? (
          <span className="inline-flex items-center gap-1">
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
