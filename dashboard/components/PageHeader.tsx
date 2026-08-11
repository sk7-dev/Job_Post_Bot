import { Radar } from "lucide-react";

export function PageHeader() {
  return (
    <header className="sticky top-0 z-30 flex h-14 shrink-0 items-center border-b border-[var(--border-subtle)] bg-[var(--sidebar-bg)]/95 px-4 backdrop-blur-sm sm:px-6 md:hidden">
      <div className="flex items-center gap-2">
        <span className="flex size-7 shrink-0 items-center justify-center rounded-full bg-[var(--accent-soft)]">
          <Radar className="size-3.5 text-[var(--accent)]" aria-hidden />
        </span>
        <span className="text-[13px] font-semibold text-slate-900">Job Watcher</span>
      </div>
    </header>
  );
}
