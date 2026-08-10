"use client";

import { useState } from "react";
import { ChevronDown } from "lucide-react";
import type { SourceRecord } from "@/lib/types";
import { StatusBadge } from "./StatusBadge";
import { RelativeTime } from "./RelativeTime";
import { cn, formatDuration } from "@/lib/utils";

export function SourceTable({ sources }: { sources: SourceRecord[] }) {
  const [expanded, setExpanded] = useState<string | null>(null);

  return (
    <div className="divide-y divide-slate-100 rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)]">
      <div className="hidden grid-cols-[1.6fr_1fr_0.9fr_0.9fr_0.9fr_1fr_1.5rem] gap-3 bg-slate-50 px-5 py-3.5 text-xs font-medium text-slate-500 sm:grid">
        <span>Company</span>
        <span>Platform</span>
        <span>Status</span>
        <span>Fetched</span>
        <span>Matched</span>
        <span>Last scan</span>
        <span />
      </div>
      {sources.map((source) => {
        const isOpen = expanded === source.name;
        return (
          <div key={source.name}>
            <button
              type="button"
              onClick={() => setExpanded(isOpen ? null : source.name)}
              className="grid w-full grid-cols-2 gap-2 px-5 py-3.5 text-left text-sm transition-colors hover:bg-slate-50 sm:grid-cols-[1.6fr_1fr_0.9fr_0.9fr_0.9fr_1fr_1.5rem] sm:items-center sm:gap-3"
              aria-expanded={isOpen}
            >
              <span className="col-span-2 truncate font-medium text-slate-900 sm:col-span-1">
                {source.name}
              </span>
              <span className="text-slate-500">{source.type}</span>
              <span>
                <StatusBadge status={source.status} />
              </span>
              <span className="text-slate-500">{source.jobs_fetched}</span>
              <span className="text-slate-500">{source.jobs_matched}</span>
              <span className="text-slate-500">
                <RelativeTime iso={source.last_scan} />
              </span>
              <ChevronDown
                className={cn("size-4 text-slate-400 transition-transform", isOpen && "rotate-180")}
                aria-hidden
              />
            </button>
            {isOpen ? (
              <div className="border-t border-slate-100 bg-slate-50 px-5 py-4 text-sm">
                <dl className="grid grid-cols-2 gap-x-4 gap-y-2 sm:grid-cols-3">
                  <div>
                    <dt className="text-xs text-slate-400">Last scan</dt>
                    <dd className="text-slate-700">
                      <RelativeTime iso={source.last_scan} />
                    </dd>
                  </div>
                  <div>
                    <dt className="text-xs text-slate-400">Duration</dt>
                    <dd className="text-slate-700">{formatDuration(source.duration_ms / 1000)}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-slate-400">Type</dt>
                    <dd className="text-slate-700">{source.type}</dd>
                  </div>
                </dl>
                {source.error ? (
                  <div className="mt-3">
                    <p className="text-xs text-slate-400">Latest error</p>
                    <p className="mt-1 rounded-lg border border-red-200 bg-[var(--failed-soft)] px-3 py-2 font-mono text-xs text-[var(--failed)]">
                      {source.error}
                    </p>
                  </div>
                ) : null}
              </div>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}
