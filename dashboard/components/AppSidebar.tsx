"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Rocket, ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { Brand } from "./Brand";
import { NAV_ITEMS } from "./nav-items";

export function AppSidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden shrink-0 self-start rounded-[14px] border border-[var(--border-subtle)] bg-[var(--sidebar-bg)] shadow-[0_1px_2px_rgba(15,23,42,0.04),0_1px_3px_rgba(15,23,42,0.06)] md:sticky md:top-6 md:flex md:h-[calc(100vh-3rem)] md:w-[272px] md:flex-col">
      <div className="flex h-16 items-center px-6">
        <Brand tagline />
      </div>
      <nav className="flex-1 space-y-1.5 px-4 py-3">
        {NAV_ITEMS.map((item) => {
          const active = pathname === item.href;
          const Icon = item.icon;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "group relative flex items-center gap-3 rounded-lg px-3.5 py-2.5 text-sm font-medium transition-colors",
                active
                  ? "bg-[var(--accent-soft)] text-[var(--accent)]"
                  : "text-slate-500 hover:bg-slate-50 hover:text-slate-700"
              )}
            >
              {active ? (
                <span className="absolute left-0 top-1/2 h-4 w-0.5 -translate-y-1/2 rounded-full bg-[var(--accent)]" aria-hidden />
              ) : null}
              <Icon
                className={cn("size-4 shrink-0", active ? "text-[var(--accent)]" : "text-slate-400 group-hover:text-slate-600")}
                aria-hidden
              />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="border-t border-[var(--border-subtle)] px-4 py-4">
        <Link
          href="/deploy"
          className={cn(
            "group relative flex items-center gap-3 overflow-hidden rounded-lg bg-gradient-to-br from-[var(--accent)] to-indigo-600 px-3.5 py-3 shadow-[0_4px_14px_rgba(37,99,235,0.25)] transition-all hover:-translate-y-0.5 hover:shadow-[0_8px_20px_rgba(37,99,235,0.32)]",
            pathname === "/deploy" && "ring-2 ring-[var(--accent)]/40 ring-offset-2 ring-offset-[var(--sidebar-bg)]"
          )}
        >
          <span
            aria-hidden
            className="pointer-events-none absolute -right-4 -top-6 size-16 rounded-full bg-white/10 blur-xl transition-transform duration-300 group-hover:scale-125"
          />
          <span className="flex size-8 shrink-0 items-center justify-center rounded-md bg-white/15">
            <Rocket className="size-4 text-white" aria-hidden />
          </span>
          <span className="min-w-0 flex-1">
            <span className="block text-sm font-semibold text-white">Deploy it yourself</span>
            <span className="block text-[11px] text-blue-100/80">Run your own instance</span>
          </span>
          <ChevronRight
            className="size-4 shrink-0 text-white/70 transition-transform group-hover:translate-x-0.5"
            aria-hidden
          />
        </Link>
        <p className="mt-3 text-xs text-slate-400">Read-only view</p>
      </div>
    </aside>
  );
}
