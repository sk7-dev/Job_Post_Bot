import { ChevronRight } from "lucide-react";
import type { SourceRecord } from "@/lib/types";
import { StatusDot } from "./StatusBadge";

const STATUS_LABEL: Record<SourceRecord["status"], string> = {
  healthy: "Healthy",
  warning: "Warning",
  failed: "Failed",
};

export function SourceHealthRow({ source }: { source: SourceRecord }) {
  return (
    <div className="flex items-center justify-between gap-3 py-3 text-sm">
      <div className="flex min-w-0 items-center gap-2.5">
        <StatusDot status={source.status} />
        <span className="truncate font-medium text-slate-700">{source.name}</span>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <span className="text-xs text-slate-500">{STATUS_LABEL[source.status] ?? source.status}</span>
        <ChevronRight className="size-4 text-slate-300" aria-hidden />
      </div>
    </div>
  );
}
