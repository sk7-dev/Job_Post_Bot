import { ExternalLink, MapPin } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { Badge } from "./ui/Badge";
import { RelativeTime } from "./RelativeTime";

export function JobCard({ job }: { job: JobRecord }) {
  const mobileTag = job.matched_keywords?.[0];

  return (
    <div>
      {/* Mobile: whole row is the tap target, no separate button, title wraps to 2 lines. */}
      <a
        href={job.url}
        target="_blank"
        rel="noopener noreferrer"
        className="block min-h-11 rounded-[10px] px-3.5 py-3.5 transition-colors duration-150 active:bg-slate-100 md:hidden"
      >
        <div className="flex items-start justify-between gap-3">
          <p className="line-clamp-2 min-w-0 flex-1 text-[15px] font-semibold leading-snug text-[var(--text-primary)]">
            {job.title}
          </p>
          {job.is_new ? (
            <Badge tone="blue" className="shrink-0">
              New
            </Badge>
          ) : null}
        </div>
        <p className="mt-1.5 truncate text-[13px] text-slate-500">{job.company}</p>
        <div className="mt-2.5 flex flex-wrap items-center gap-x-1.5 gap-y-1 text-xs text-slate-400">
          {job.location ? (
            <span className="inline-flex min-w-0 items-center gap-1">
              <MapPin className="size-3 shrink-0" aria-hidden />
              <span className="truncate">{job.location}</span>
            </span>
          ) : null}
          {job.location ? (
            <span className="text-slate-300" aria-hidden>
              ·
            </span>
          ) : null}
          <RelativeTime iso={job.first_seen} />
          {mobileTag ? (
            <Badge tone="neutral" className="ml-0.5 px-2 py-0.5 text-[11px]">
              {mobileTag}
            </Badge>
          ) : null}
        </div>
      </a>

      {/* Desktop: title link + separate View Job button, unchanged. */}
      <div className="hidden rounded-[10px] px-4 py-3.5 transition-colors duration-150 hover:bg-slate-50 md:block">
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
    </div>
  );
}
