import { Inbox, type LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export function EmptyState({
  title,
  description,
  icon: Icon = Inbox,
  className,
}: {
  title: string;
  description?: string;
  icon?: LucideIcon;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center gap-2 rounded-2xl border border-dashed border-white/[0.08] px-6 py-14 text-center",
        className
      )}
    >
      <Icon className="size-8 text-zinc-600" aria-hidden />
      <p className="text-sm font-medium text-zinc-100">{title}</p>
      {description ? (
        <p className="max-w-sm text-sm text-zinc-500">{description}</p>
      ) : null}
    </div>
  );
}
