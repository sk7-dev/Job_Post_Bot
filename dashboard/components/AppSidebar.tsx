"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Radar } from "lucide-react";
import { cn } from "@/lib/utils";
import { NAV_ITEMS } from "./nav-items";

export function AppSidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden shrink-0 self-start rounded-[14px] border border-[var(--border-subtle)] bg-[var(--sidebar-bg)] shadow-[0_1px_2px_rgba(15,23,42,0.04),0_1px_3px_rgba(15,23,42,0.06)] md:sticky md:top-6 md:flex md:h-[calc(100vh-3rem)] md:w-[272px] md:flex-col">
      <div className="flex h-16 items-center gap-2 px-6">
        <Radar className="size-4 text-[var(--accent)]" aria-hidden />
        <span className="text-[13px] font-medium tracking-wide text-slate-700">Job Watcher</span>
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
      <div className="px-6 py-5 text-xs text-slate-400">Read-only view</div>
    </aside>
  );
}
