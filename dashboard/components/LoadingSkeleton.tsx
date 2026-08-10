import { cn } from "@/lib/utils";

function Pulse({ className }: { className?: string }) {
  return <div className={cn("animate-pulse rounded-md bg-slate-200", className)} />;
}

export function StatsSkeleton() {
  return (
    <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
      {Array.from({ length: 4 }).map((_, i) => (
        <div key={i} className="min-h-[128px] rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)] p-5">
          <Pulse className="h-6 w-12 mb-2" />
          <Pulse className="h-3 w-20" />
        </div>
      ))}
    </div>
  );
}

export function TableSkeleton({ rows = 6 }: { rows?: number }) {
  return (
    <div className="rounded-[14px] border border-[var(--border-subtle)] bg-[var(--surface)] divide-y divide-slate-100">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="flex items-center gap-4 px-5 py-3.5">
          <Pulse className="h-4 w-1/4" />
          <Pulse className="h-4 w-1/6" />
          <Pulse className="h-4 w-1/6" />
          <Pulse className="h-4 w-1/6" />
        </div>
      ))}
    </div>
  );
}

export function ListSkeleton({ rows = 5 }: { rows?: number }) {
  return (
    <div className="space-y-2">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="rounded-xl bg-[var(--surface)] p-4">
          <Pulse className="h-4 w-1/3 mb-2" />
          <Pulse className="h-3 w-2/3" />
        </div>
      ))}
    </div>
  );
}
