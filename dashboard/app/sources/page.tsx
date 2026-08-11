import { CheckCircle2, Download, XCircle } from "lucide-react";
import { getSources } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { ScanStatus } from "@/components/ScanStatus";
import { StatCard } from "@/components/StatCard";
import { SourceTable } from "@/components/SourceTable";

export default async function SourcesPage() {
  const sources = await getSources();

  const healthyCount = sources.ok ? sources.data.filter((s) => s.status === "healthy").length : 0;
  const failedCount = sources.ok ? sources.data.filter((s) => s.status === "failed").length : 0;
  const totalFetched = sources.ok ? sources.data.reduce((sum, s) => sum + s.jobs_fetched, 0) : 0;

  return (
    <div className="space-y-4 sm:space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-[26px] font-bold leading-tight text-[var(--text-primary)] sm:text-[30px] md:text-[34px]">
            Sources
          </h1>
          <p className="mt-1 text-sm text-slate-500">
            <span className="sm:hidden">Tap a source for details.</span>
            <span className="hidden sm:inline">
              Status of every configured career site from the most recent scan. Tap a row for details.
            </span>
          </p>
        </div>
        <ScanStatus />
      </div>

      {sources.ok && sources.data.length > 0 ? (
        <div className="grid grid-cols-3 gap-1.5 md:hidden">
          <StatCard compact label="Healthy" value={healthyCount} icon={CheckCircle2} tone="green" />
          <StatCard compact label="Failed" value={failedCount} icon={XCircle} tone={failedCount > 0 ? "red" : "neutral"} />
          <StatCard compact label="Fetched" value={totalFetched} icon={Download} tone="blue" />
        </div>
      ) : null}

      {!sources.ok ? (
        <ErrorState kind={sources.kind} message={sources.error} />
      ) : sources.data.length === 0 ? (
        <EmptyState title="No sources configured." />
      ) : (
        <SourceTable sources={sources.data} />
      )}
    </div>
  );
}
