import { getOverview } from "@/lib/github-data";
import { RelativeTime } from "./RelativeTime";
import { StatusDot } from "./StatusBadge";

export async function ScanStatus() {
  const overview = await getOverview();
  const lastScan = overview.ok ? overview.data.last_scan : null;
  const status = overview.ok ? overview.data.scan_status : "unknown";

  return (
    <div className="flex shrink-0 items-center gap-2 text-xs text-slate-500 sm:text-sm">
      <StatusDot status={status} />
      <span>
        Last scan:{" "}
        {lastScan ? (
          <RelativeTime iso={lastScan} className="font-medium text-slate-700" />
        ) : (
          <span className="font-medium text-slate-700">Unknown</span>
        )}
      </span>
    </div>
  );
}
