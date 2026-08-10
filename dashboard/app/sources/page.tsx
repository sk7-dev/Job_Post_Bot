import { getSources } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { SourceTable } from "@/components/SourceTable";

export default async function SourcesPage() {
  const sources = await getSources();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-[26px] font-semibold leading-tight text-zinc-100">Sources</h1>
        <p className="mt-1 text-sm text-zinc-500">
          Status of every configured career site from the most recent scan. Tap a row for details.
        </p>
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
