import { getSources } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { ScanStatus } from "@/components/ScanStatus";
import { SourceTable } from "@/components/SourceTable";

export default async function SourcesPage() {
  const sources = await getSources();

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="text-[30px] font-bold leading-tight text-[var(--text-primary)] sm:text-[34px]">Sources</h1>
          <p className="mt-1 text-sm text-slate-500">
            Status of every configured career site from the most recent scan. Tap a row for details.
          </p>
        </div>
        <ScanStatus />
      </div>

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
