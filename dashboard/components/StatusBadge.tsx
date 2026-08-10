import { CheckCircle2, AlertTriangle, XCircle, CircleDot } from "lucide-react";
import { Badge } from "./ui/Badge";

type AnyStatus = "healthy" | "warning" | "failed" | "success" | "partial" | string;

const CONFIG: Record<string, { label: string; tone: "green" | "amber" | "red"; icon: typeof CheckCircle2 }> = {
  healthy: { label: "Healthy", tone: "green", icon: CheckCircle2 },
  success: { label: "Success", tone: "green", icon: CheckCircle2 },
  warning: { label: "Warning", tone: "amber", icon: AlertTriangle },
  partial: { label: "Partial", tone: "amber", icon: AlertTriangle },
  failed: { label: "Failed", tone: "red", icon: XCircle },
};

export function StatusBadge({ status, className }: { status: AnyStatus; className?: string }) {
  const config = CONFIG[status] ?? { label: status, tone: "neutral" as const, icon: CircleDot };
  const Icon = config.icon;
  return (
    <Badge tone={config.tone as "green" | "amber" | "red"} className={className}>
      <Icon className="size-3" aria-hidden />
      {config.label}
    </Badge>
  );
}

export function StatusDot({ status }: { status: AnyStatus }) {
  const dotClass =
    status === "healthy" || status === "success"
      ? "bg-[var(--healthy)]"
      : status === "warning" || status === "partial"
        ? "bg-[var(--warning)] shadow-[0_0_6px_rgba(232,184,91,0.6)]"
        : status === "failed"
          ? "bg-[var(--failed)]"
          : "bg-zinc-500";
  return <span className={`inline-block size-1.5 rounded-full ${dotClass}`} aria-hidden />;
}
