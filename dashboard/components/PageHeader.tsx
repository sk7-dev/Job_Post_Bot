import { Radar } from "lucide-react";
import { getOverview } from "@/lib/github-data";
import { RelativeTime } from "./RelativeTime";
import { StatusDot } from "./StatusBadge";

export async function PageHeader() {
  const overview = await getOverview();
  const lastScan = overview.ok ? overview.data.last_scan : null;
  const status = overview.ok ? overview.data.scan_status : "unknown";

  return (
    <header className="sticky top-0 z-30 flex h-14 items-center justify-between border-b border-zinc-200 bg-white/95 px-4 backdrop-blur sm:px-6 md:pl-6 dark:border-zinc-800 dark:bg-zinc-950/95">
      <div className="flex items-center gap-2 md:hidden">
        <Radar className="size-4.5 text-zinc-900 dark:text-zinc-100" aria-hidden />
        <span className="text-sm font-semibold text-zinc-900 dark:text-zinc-100">Job Watcher</span>
      </div>
      <div className="hidden md:block text-sm font-medium text-zinc-500 dark:text-zinc-400">
        Read-only view of your job watcher bot
      </div>
      <div className="flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
        <StatusDot status={status} />
        <span>
          Last scan:{" "}
          {lastScan ? (
            <RelativeTime iso={lastScan} className="font-medium text-zinc-700 dark:text-zinc-300" />
          ) : (
            <span className="font-medium text-zinc-700 dark:text-zinc-300">Unknown</span>
          )}
        </span>
      </div>
    </header>
  );
}
