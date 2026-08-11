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
    <>
      {/* Mobile: tappable cards, replaces the compressed table below md. */}
      <div className="space-y-2.5 md:hidden">
        {sources.map((source) => {
          const isOpen = expanded === source.name;
          const failed = source.status === "failed";
          return (
            <div
              key={source.name}
              className={cn(
                "overflow-hidden rounded-[14px] border",
                failed ? "border-red-200 bg-[var(--failed-soft)]" : "border-[var(--border-subtle)] bg-white"
              )}
            >
              <button
                type="button"
                onClick={() => setExpanded(isOpen ? null : source.name)}
                className="min-h-11 w-full px-4 py-3.5 text-left"
                aria-expanded={isOpen}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="break-words font-medium leading-snug text-slate-900">{source.name}</p>
                    <p className="mt-0.5 text-xs text-slate-500">{source.type}</p>
                  </div>
                  <div className="flex shrink-0 items-center gap-2">
                    <StatusBadge status={source.status} />
                    <ChevronDown
                      className={cn("size-4 text-slate-400 transition-transform", isOpen && "rotate-180")}
                      aria-hidden
                    />
                  </div>
                </div>

                <dl className="mt-3 grid grid-cols-2 gap-2">
                  <div className="rounded-lg bg-white/70 px-2.5 py-1.5">
                    <dt className="text-[10px] text-slate-400">Fetched</dt>
                    <dd className="text-sm font-semibold text-slate-900">{source.jobs_fetched}</dd>
                  </div>
                  <div className="rounded-lg bg-white/70 px-2.5 py-1.5">
                    <dt className="text-[10px] text-slate-400">Matched</dt>
                    <dd className="text-sm font-semibold text-slate-900">{source.jobs_matched}</dd>
                  </div>
                </dl>

                <p className="mt-2.5 text-xs text-slate-400">
                  Last scan: <RelativeTime iso={source.last_scan} className="font-medium text-slate-600" />
                </p>
              </button>

              {isOpen ? (
                <div className={cn("border-t px-4 py-3.5 text-sm", failed ? "border-red-200" : "border-slate-100 bg-slate-50")}>
                  <dl className="grid grid-cols-2 gap-x-4 gap-y-2.5">
                    <div>
                      <dt className="text-[11px] text-slate-400">Duration</dt>
                      <dd className="text-slate-700">{formatDuration(source.duration_ms / 1000)}</dd>
                    </div>
                    <div>
                      <dt className="text-[11px] text-slate-400">Platform</dt>
                      <dd className="text-slate-700">{source.type}</dd>
                    </div>
                  </dl>
                  {source.error ? (
                    <div className="mt-3">
                      <p className="text-[11px] text-slate-400">Latest error</p>
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

      {/* Desktop: existing compressed table, unchanged. */}
      <div className="hidden divide-y divide-slate-100 rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)] md:block">
        <div className="grid grid-cols-[1.6fr_1fr_0.9fr_0.9fr_0.9fr_1fr_1.5rem] gap-3 bg-slate-50 px-5 py-3.5 text-xs font-medium text-slate-500">
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
                className="grid w-full grid-cols-[1.6fr_1fr_0.9fr_0.9fr_0.9fr_1fr_1.5rem] items-center gap-3 px-5 py-3.5 text-left text-sm transition-colors hover:bg-slate-50"
                aria-expanded={isOpen}
              >
                <span className="truncate font-medium text-slate-900">{source.name}</span>
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
                  <dl className="grid grid-cols-3 gap-x-4 gap-y-2">
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
                      <p className="text-xs text-slate-600">Latest error</p>
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
    </>
  );
}
