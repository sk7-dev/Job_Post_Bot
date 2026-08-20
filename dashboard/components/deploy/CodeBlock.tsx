import { cn } from "@/lib/utils";
import { CopyButton } from "./CopyButton";

export function CodeBlock({ code, className }: { code: string; className?: string }) {
  return (
    <div
      className={cn(
        "group flex items-center justify-between gap-3 rounded-lg bg-slate-900 px-3.5 py-2.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]",
        className
      )}
    >
      <code className="min-w-0 overflow-x-auto whitespace-pre font-mono text-[13px] leading-relaxed text-slate-100">
        {code}
      </code>
      <CopyButton value={code} />
    </div>
  );
}
