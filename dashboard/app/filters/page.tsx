import { Info } from "lucide-react";
import { getConfig } from "@/lib/github-data";
import { ErrorState } from "@/components/ErrorState";
import { EmptyState } from "@/components/EmptyState";
import { FilterChip } from "@/components/FilterChip";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/Card";

export default async function FiltersPage() {
  const config = await getConfig();

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">Filters</h1>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          Current matching rules, read directly from <code className="font-mono text-xs">config.json</code>.
        </p>
      </div>

      {!config.ok ? (
        <ErrorState kind={config.kind} message={config.error} />
      ) : (
        <div className="space-y-4">
          <div className="flex items-start gap-2 rounded-xl border border-blue-100 bg-blue-50 px-4 py-3 text-sm text-blue-800 dark:border-blue-500/20 dark:bg-blue-500/10 dark:text-blue-300">
            <Info className="mt-0.5 size-4 shrink-0" aria-hidden />
            <p>
              Filters are managed through <code className="font-mono text-xs">config.json</code> in the repository —
              this page is read-only. Edit the file and commit it to change matching behavior.
            </p>
          </div>

          <Card>
            <CardHeader>
              <CardTitle>Included title keywords</CardTitle>
            </CardHeader>
            <CardContent>
              {config.data.filters?.title_keywords_any?.length ? (
                <div className="flex flex-wrap gap-2">
                  {config.data.filters.title_keywords_any.map((kw) => (
                    <FilterChip key={kw} tone="include">
                      {kw}
                    </FilterChip>
                  ))}
                </div>
              ) : (
                <EmptyState title="No title keyword filter set." description="Every title will match." />
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Excluded keywords</CardTitle>
            </CardHeader>
            <CardContent>
              {config.data.filters?.excluded_keywords_any?.length ? (
                <div className="flex flex-wrap gap-2">
                  {config.data.filters.excluded_keywords_any.map((kw) => (
                    <FilterChip key={kw} tone="exclude">
                      {kw}
                    </FilterChip>
                  ))}
                </div>
              ) : (
                <EmptyState title="No excluded keywords set." />
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Locations</CardTitle>
            </CardHeader>
            <CardContent>
              {config.data.filters?.locations_any?.length ? (
                <div className="flex flex-wrap gap-2">
                  {config.data.filters.locations_any.map((loc) => (
                    <FilterChip key={loc} tone="neutral">
                      {loc}
                    </FilterChip>
                  ))}
                </div>
              ) : (
                <EmptyState title="No location filter set." description="Every location will match." />
              )}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Configured sources</CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-2xl font-semibold text-zinc-900 dark:text-zinc-100">
                {config.data.sources?.length ?? 0}
              </p>
              <p className="text-sm text-zinc-500 dark:text-zinc-400">career sites configured for scraping</p>
            </CardContent>
          </Card>
        </div>
      )}
    </div>
  );
}
