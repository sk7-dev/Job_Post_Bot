import { cn } from "@/lib/utils";
import type { ButtonHTMLAttributes, AnchorHTMLAttributes } from "react";

const BASE =
  "inline-flex items-center justify-center gap-1.5 rounded-[9px] text-sm font-medium transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/40 disabled:opacity-50 disabled:pointer-events-none";

const VARIANTS = {
  primary: "bg-white text-zinc-900 hover:bg-zinc-200",
  secondary:
    "border border-[var(--border-subtle)] bg-white/[0.03] text-zinc-200 hover:bg-white/[0.06] hover:border-[var(--accent)]/30 hover:shadow-[0_0_16px_rgba(143,174,248,0.12)]",
  ghost:
    "text-zinc-400 hover:bg-white/[0.05] hover:text-zinc-100 hover:border-[var(--accent)]/20 border border-transparent",
  outline:
    "border border-[var(--border-subtle)] text-zinc-300 hover:bg-white/[0.04] hover:border-[var(--accent)]/30",
};

const SIZES = {
  sm: "h-8 px-3 text-xs",
  md: "h-9 px-3.5",
};

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: keyof typeof VARIANTS;
  size?: keyof typeof SIZES;
}

export function Button({
  variant = "secondary",
  size = "md",
  className,
  ...props
}: ButtonProps) {
  return (
    <button
      className={cn(BASE, VARIANTS[variant], SIZES[size], className)}
      {...props}
    />
  );
}

interface LinkButtonProps extends AnchorHTMLAttributes<HTMLAnchorElement> {
  variant?: keyof typeof VARIANTS;
  size?: keyof typeof SIZES;
}

export function LinkButton({
  variant = "secondary",
  size = "md",
  className,
  ...props
}: LinkButtonProps) {
  return (
    <a className={cn(BASE, VARIANTS[variant], SIZES[size], className)} {...props} />
  );
}
