import { cn } from "@/lib/utils";
import type { ButtonHTMLAttributes, AnchorHTMLAttributes } from "react";

const BASE =
  "inline-flex items-center justify-center gap-1.5 whitespace-nowrap rounded-lg text-sm font-medium transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--accent)]/30 focus-visible:ring-offset-2 focus-visible:ring-offset-white disabled:opacity-50 disabled:pointer-events-none";

const VARIANTS = {
  primary: "bg-[var(--accent)] text-white hover:bg-[var(--accent-hover)]",
  secondary:
    "border border-[var(--border-subtle)] bg-white text-[var(--text-secondary)] hover:bg-slate-50 hover:border-slate-300",
  ghost:
    "text-[var(--text-muted)] hover:bg-slate-50 hover:text-[var(--text-primary)] border border-transparent",
  outline:
    "border border-[var(--border-subtle)] text-[var(--text-secondary)] hover:bg-slate-50",
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
