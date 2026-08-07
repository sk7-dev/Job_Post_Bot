import { formatAbsoluteTime, formatRelativeTime } from "@/lib/utils";

export function RelativeTime({ iso, className }: { iso: string | null | undefined; className?: string }) {
  return (
    <time dateTime={iso ?? undefined} title={formatAbsoluteTime(iso)} suppressHydrationWarning className={className}>
      {formatRelativeTime(iso)}
    </time>
  );
}
