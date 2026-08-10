"use client";

import { useMemo, useState } from "react";
import { Search } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { JobTable } from "@/components/JobTable";
import { JobCard } from "@/components/JobCard";
import { EmptyState } from "@/components/EmptyState";

type SortOrder = "newest" | "oldest";

const SELECT_CLASS =
  "h-9 rounded-lg border border-[var(--border-subtle)] bg-white px-2.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30";

export function JobsClient({ jobs }: { jobs: JobRecord[] }) {
  const [query, setQuery] = useState("");
  const [company, setCompany] = useState("all");
  const [location, setLocation] = useState("all");
  const [source, setSource] = useState("all");
  const [sort, setSort] = useState<SortOrder>("newest");

  const companies = useMemo(
    () => Array.from(new Set(jobs.map((j) => j.company).filter(Boolean))).sort(),
    [jobs]
  );
  const locations = useMemo(
    () => Array.from(new Set(jobs.map((j) => j.location).filter(Boolean))).sort(),
    [jobs]
  );
  const sources = useMemo(
    () => Array.from(new Set(jobs.map((j) => j.source_name).filter(Boolean))).sort(),
    [jobs]
  );

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    let result = jobs.filter((job) => {
      if (company !== "all" && job.company !== company) return false;
      if (location !== "all" && job.location !== location) return false;
      if (source !== "all" && job.source_name !== source) return false;
      if (!q) return true;
      return (
        job.title.toLowerCase().includes(q) ||
        job.company.toLowerCase().includes(q) ||
        job.location.toLowerCase().includes(q)
      );
    });

    result = [...result].sort((a, b) => {
      const aTime = new Date(a.first_seen).getTime();
      const bTime = new Date(b.first_seen).getTime();
      return sort === "newest" ? bTime - aTime : aTime - bTime;
    });

    return result;
  }, [jobs, query, company, location, source, sort]);

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <div className="relative flex-1 min-w-[200px]">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-slate-400" aria-hidden />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search title, company, or location…"
            className="h-9 w-full rounded-lg border border-[var(--border-subtle)] bg-white pl-8 pr-3 text-sm text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30"
          />
        </div>

        <select value={company} onChange={(e) => setCompany(e.target.value)} className={SELECT_CLASS}>
          <option value="all">All companies</option>
          {companies.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select value={location} onChange={(e) => setLocation(e.target.value)} className={SELECT_CLASS}>
          <option value="all">All locations</option>
          {locations.map((l) => (
            <option key={l} value={l}>
              {l}
            </option>
          ))}
        </select>

        <select value={source} onChange={(e) => setSource(e.target.value)} className={SELECT_CLASS}>
          <option value="all">All sources</option>
          {sources.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>

        <select value={sort} onChange={(e) => setSort(e.target.value as SortOrder)} className={SELECT_CLASS}>
          <option value="newest">Newest first</option>
          <option value="oldest">Oldest first</option>
        </select>
      </div>

      <p className="text-xs text-slate-500">
        {filtered.length} of {jobs.length} job{jobs.length === 1 ? "" : "s"}
      </p>

      {filtered.length === 0 ? (
        <EmptyState title="No matching jobs found." description="Try clearing a filter or search term." />
      ) : (
        <>
          <div className="hidden md:block">
            <JobTable jobs={filtered} />
          </div>
          <div className="divide-y divide-slate-100 rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)] px-1 md:hidden">
            {filtered.map((job) => (
              <JobCard key={job.key} job={job} />
            ))}
          </div>
        </>
      )}
    </div>
  );
}
