import { AlertOctagon, FileWarning, PlugZap, Settings, type LucideIcon } from "lucide-react";
import type { FetchKind } from "@/lib/types";
import { cn } from "@/lib/utils";

const KIND_COPY: Record<FetchKind, { title: string; icon: LucideIcon }> = {
  config: { title: "Dashboard isn't configured yet", icon: Settings },
  network: { title: "Couldn't reach GitHub", icon: PlugZap },
  not_found: { title: "Data file not found", icon: FileWarning },
  parse: { title: "Data file looks malformed", icon: AlertOctagon },
  empty: { title: "Data file is empty", icon: FileWarning },
};

export function ErrorState({
  kind = "network",
  message,
  className,
}: {
  kind?: FetchKind;
  message: string;
  className?: string;
}) {
  const copy = KIND_COPY[kind];
  const Icon = copy.icon;
  return (
    <div
      className={cn(
        "flex flex-col items-center justify-center gap-2 rounded-2xl border border-[var(--border-subtle)] bg-white/[0.02] px-6 py-14 text-center",
        className
      )}
    >
      <Icon className="size-8 text-zinc-500" aria-hidden />
      <p className="text-sm font-medium text-zinc-100">{copy.title}</p>
      <p className="max-w-sm text-sm text-zinc-500">{message}</p>
    </div>
  );
}
