import { Radar } from "lucide-react";

export function PageHeader() {
  return (
    <header className="flex h-16 shrink-0 items-center border-b border-[var(--border-subtle)] bg-[var(--sidebar-bg)] px-4 sm:px-6 md:hidden">
      <div className="flex items-center gap-2">
        <Radar className="size-4 text-[var(--accent)]" aria-hidden />
        <span className="text-[13px] font-medium text-slate-700">Job Watcher</span>
      </div>
    </header>
  );
}
