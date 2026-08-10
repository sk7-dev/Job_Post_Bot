import { Radar } from "lucide-react";
import { getOverview } from "@/lib/github-data";
import { RelativeTime } from "./RelativeTime";
import { StatusDot } from "./StatusBadge";

export async function PageHeader() {
  const overview = await getOverview();
  const lastScan = overview.ok ? overview.data.last_scan : null;
  const status = overview.ok ? overview.data.scan_status : "unknown";

  return (
    <header className="flex h-16 shrink-0 items-center justify-between border-b border-white/[0.06] px-4 sm:px-6 md:px-8">
      <div className="flex items-center gap-2 md:hidden">
        <Radar className="size-4 text-[var(--accent)]" aria-hidden />
        <span className="text-[13px] font-medium text-zinc-200">Job Watcher</span>
      </div>
      <div className="hidden text-[13px] text-zinc-500 md:block">
        Read-only view of your job watcher bot
      </div>
      <div className="flex items-center gap-2 text-xs text-zinc-500">
        <StatusDot status={status} />
        <span>
          Last scan:{" "}
          {lastScan ? (
            <RelativeTime iso={lastScan} className="font-medium text-zinc-300" />
          ) : (
            <span className="font-medium text-zinc-300">Unknown</span>
          )}
        </span>
      </div>
    </header>
  );
}
