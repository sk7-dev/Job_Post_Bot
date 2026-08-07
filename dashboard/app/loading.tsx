import { StatsSkeleton, ListSkeleton } from "@/components/LoadingSkeleton";

export default function Loading() {
  return (
    <div className="space-y-6">
      <StatsSkeleton />
      <ListSkeleton />
    </div>
  );
}
