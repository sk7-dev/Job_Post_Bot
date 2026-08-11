"use client";

import { useEffect, useMemo, useState } from "react";
import { ChevronDown, Search, SlidersHorizontal, X } from "lucide-react";
import type { JobRecord } from "@/lib/types";
import { JobTable } from "@/components/JobTable";
import { JobCard } from "@/components/JobCard";
import { EmptyState } from "@/components/EmptyState";

type SortOrder = "newest" | "oldest";

const SELECT_CLASS =
  "h-11 sm:h-9 w-full sm:w-[150px] rounded-lg border border-[var(--border-subtle)] bg-white px-2.5 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30";

export function JobsClient({ jobs }: { jobs: JobRecord[] }) {
  const [query, setQuery] = useState("");
  const [company, setCompany] = useState("all");
  const [location, setLocation] = useState("all");
  const [source, setSource] = useState("all");
  const [sort, setSort] = useState<SortOrder>("newest");
  const [sheetOpen, setSheetOpen] = useState(false);

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

  const activeFilters = useMemo(() => {
    const list: { key: string; label: string; clear: () => void }[] = [];
    if (company !== "all") list.push({ key: "company", label: company, clear: () => setCompany("all") });
    if (location !== "all") list.push({ key: "location", label: location, clear: () => setLocation("all") });
    if (source !== "all") list.push({ key: "source", label: source, clear: () => setSource("all") });
    return list;
  }, [company, location, source]);

  useEffect(() => {
    if (!sheetOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setSheetOpen(false);
    };
    document.addEventListener("keydown", onKey);
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = previousOverflow;
    };
  }, [sheetOpen]);

  return (
    <div className="space-y-4">
      {/* ===== Mobile-only: search + compact filter toolbar ===== */}
      <div className="flex flex-col gap-3 md:hidden">
        <div className="relative">
          <Search className="pointer-events-none absolute left-3.5 top-1/2 size-4 -translate-y-1/2 text-slate-400" aria-hidden />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search jobs…"
            className="h-11 w-full rounded-xl border border-[var(--border-subtle)] bg-white pl-10 pr-9 text-[15px] text-slate-700 placeholder:text-slate-400 shadow-[0_1px_2px_rgba(15,23,42,0.04)] focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30"
          />
          {query ? (
            <button
              type="button"
              onClick={() => setQuery("")}
              aria-label="Clear search"
              className="absolute right-2 top-1/2 flex size-7 -translate-y-1/2 items-center justify-center rounded-full text-slate-400 transition-colors active:bg-slate-100"
            >
              <X className="size-4" aria-hidden />
            </button>
          ) : null}
        </div>

        <button
          type="button"
          onClick={() => setSheetOpen(true)}
          className="inline-flex h-11 w-fit shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full border border-[var(--border-subtle)] bg-slate-50 px-3.5 text-[13px] font-medium text-slate-700 transition-colors active:bg-slate-100"
        >
          <SlidersHorizontal className="size-4" aria-hidden />
          Filters
          {activeFilters.length > 0 ? (
            <span className="flex size-[18px] items-center justify-center rounded-full bg-[var(--accent)] text-[10px] font-semibold text-white">
              {activeFilters.length}
            </span>
          ) : null}
        </button>

        {activeFilters.length > 0 ? (
          <div className="flex flex-wrap gap-1.5">
            {activeFilters.map((f) => (
              <button
                key={f.key}
                type="button"
                onClick={f.clear}
                className="inline-flex h-7 max-w-[180px] items-center gap-1 rounded-full bg-[var(--accent-soft)] pl-3 pr-2 text-xs font-medium text-[var(--accent)] transition-colors active:bg-blue-100"
              >
                <span className="truncate">{f.label}</span>
                <X className="size-3.5 shrink-0" aria-hidden />
              </button>
            ))}
          </div>
        ) : null}

        <div className="flex items-center justify-between">
          <p className="text-xs text-slate-500">
            {filtered.length} job{filtered.length === 1 ? "" : "s"}
          </p>
          <span className="relative inline-flex items-center text-xs text-slate-500">
            Sort:&nbsp;
            <select
              value={sort}
              onChange={(e) => setSort(e.target.value as SortOrder)}
              className="appearance-none bg-transparent py-1 pl-0 pr-4 text-xs font-semibold text-slate-700 focus:outline-none"
            >
              <option value="newest">Newest</option>
              <option value="oldest">Oldest</option>
            </select>
            <ChevronDown className="pointer-events-none absolute right-0 size-3 text-slate-400" aria-hidden />
          </span>
        </div>
      </div>

      {/* ===== Desktop-only: original stacked toolbar, unchanged ===== */}
      <div className="hidden gap-2 sm:flex-row sm:flex-wrap sm:items-center md:flex">
        <div className="relative w-full sm:min-w-[180px] sm:flex-1">
          <Search className="pointer-events-none absolute left-2.5 top-1/2 size-4 -translate-y-1/2 text-slate-400" aria-hidden />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search title, company, or location…"
            className="h-11 sm:h-9 w-full rounded-lg border border-[var(--border-subtle)] bg-white pl-8 pr-3 text-sm text-slate-700 placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30"
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

      <p className="hidden text-xs text-slate-500 md:block">
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

      {/* ===== Mobile-only: filters bottom sheet ===== */}
      {sheetOpen ? (
        <div className="fixed inset-0 z-50 md:hidden">
          <div
            className="absolute inset-0 bg-slate-900/40"
            onClick={() => setSheetOpen(false)}
            aria-hidden
          />
          <div
            role="dialog"
            aria-modal="true"
            aria-label="Filters"
            className="absolute inset-x-0 bottom-0 max-h-[85vh] overflow-y-auto rounded-t-[24px] border-t border-[var(--border-subtle)] bg-white px-5 pt-4 pb-[calc(1.25rem+env(safe-area-inset-bottom))] shadow-[0_-12px_30px_rgba(15,23,42,0.18)]"
          >
            <div className="mx-auto mb-3 h-1.5 w-10 rounded-full bg-slate-200" aria-hidden />
            <div className="flex items-center justify-between">
              <h2 className="text-base font-semibold text-slate-900">Filters</h2>
              <button
                type="button"
                onClick={() => setSheetOpen(false)}
                aria-label="Close filters"
                className="flex size-8 items-center justify-center rounded-full text-slate-400 transition-colors active:bg-slate-100"
              >
                <X className="size-4" aria-hidden />
              </button>
            </div>

            <div className="mt-4 flex flex-col gap-4">
              <SheetSelectField label="Company" value={company} onChange={setCompany} options={companies} placeholder="All companies" />
              <SheetSelectField label="Location" value={location} onChange={setLocation} options={locations} placeholder="All locations" />
              <SheetSelectField label="Source" value={source} onChange={setSource} options={sources} placeholder="All sources" />
            </div>

            <div className="mt-6 flex gap-2">
              <button
                type="button"
                onClick={() => {
                  setCompany("all");
                  setLocation("all");
                  setSource("all");
                }}
                className="h-11 flex-1 rounded-xl border border-[var(--border-subtle)] text-sm font-medium text-slate-600 transition-colors active:bg-slate-50"
              >
                Clear all
              </button>
              <button
                type="button"
                onClick={() => setSheetOpen(false)}
                className="h-11 flex-1 rounded-xl bg-[var(--accent)] text-sm font-semibold text-white transition-colors active:bg-[var(--accent-hover)]"
              >
                Done
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function SheetSelectField({
  label,
  value,
  onChange,
  options,
  placeholder,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  options: string[];
  placeholder: string;
}) {
  return (
    <label className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-500">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="h-11 w-full rounded-xl border border-[var(--border-subtle)] bg-slate-50 px-3 text-sm text-slate-700 focus:outline-none focus:ring-2 focus:ring-[var(--accent)]/30"
      >
        <option value="all">{placeholder}</option>
        {options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    </label>
  );
}
