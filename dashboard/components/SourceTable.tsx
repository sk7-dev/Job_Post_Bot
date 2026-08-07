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
    <div className="divide-y divide-zinc-100 rounded-xl border border-zinc-200 dark:divide-zinc-800 dark:border-zinc-800">
      <div className="hidden grid-cols-[1.6fr_1fr_0.9fr_0.9fr_0.9fr_1fr_1.5rem] gap-3 px-4 py-3 text-xs font-medium text-zinc-500 sm:grid dark:text-zinc-400">
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
              className="grid w-full grid-cols-2 gap-2 px-4 py-3.5 text-left text-sm hover:bg-zinc-50 sm:grid-cols-[1.6fr_1fr_0.9fr_0.9fr_0.9fr_1fr_1.5rem] sm:items-center sm:gap-3 dark:hover:bg-zinc-900/60"
              aria-expanded={isOpen}
            >
              <span className="col-span-2 truncate font-medium text-zinc-900 sm:col-span-1 dark:text-zinc-100">
                {source.name}
              </span>
              <span className="text-zinc-500 dark:text-zinc-400">{source.type}</span>
              <span>
                <StatusBadge status={source.status} />
              </span>
              <span className="text-zinc-600 dark:text-zinc-400">{source.jobs_fetched}</span>
              <span className="text-zinc-600 dark:text-zinc-400">{source.jobs_matched}</span>
              <span className="text-zinc-500 dark:text-zinc-400">
                <RelativeTime iso={source.last_scan} />
              </span>
              <ChevronDown
                className={cn("size-4 text-zinc-400 transition-transform", isOpen && "rotate-180")}
                aria-hidden
              />
            </button>
            {isOpen ? (
              <div className="border-t border-zinc-100 bg-zinc-50/60 px-4 py-3 text-sm dark:border-zinc-800 dark:bg-zinc-900/40">
                <dl className="grid grid-cols-2 gap-x-4 gap-y-2 sm:grid-cols-3">
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Last scan</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">
                      <RelativeTime iso={source.last_scan} />
                    </dd>
                  </div>
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Duration</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">{formatDuration(source.duration_ms / 1000)}</dd>
                  </div>
                  <div>
                    <dt className="text-xs text-zinc-400 dark:text-zinc-600">Type</dt>
                    <dd className="text-zinc-700 dark:text-zinc-300">{source.type}</dd>
                  </div>
                </dl>
                {source.error ? (
                  <div className="mt-3">
                    <p className="text-xs text-zinc-400 dark:text-zinc-600">Latest error</p>
                    <p className="mt-1 rounded-lg bg-red-50 px-3 py-2 font-mono text-xs text-red-700 dark:bg-red-500/10 dark:text-red-400">
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
