"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Radar } from "lucide-react";
import { cn } from "@/lib/utils";
import { NAV_ITEMS } from "./nav-items";

export function AppSidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden shrink-0 self-start rounded-[18px] border border-white/[0.08] bg-[var(--sidebar-bg)] shadow-[0_25px_80px_rgba(0,0,0,0.45)] md:sticky md:top-8 md:flex md:h-[calc(100vh-4rem)] md:w-60 md:flex-col">
      <div className="flex h-16 items-center gap-2 px-6">
        <Radar className="size-4 text-[var(--accent)]" aria-hidden />
        <span className="text-[13px] font-medium tracking-wide text-zinc-200">Job Watcher</span>
      </div>
      <nav className="flex-1 space-y-1 px-4 py-3">
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
                  ? "bg-white/[0.06] text-zinc-100"
                  : "text-zinc-500 hover:bg-white/[0.03] hover:text-zinc-200"
              )}
            >
              {active ? (
                <span className="absolute left-0 top-1/2 h-4 w-0.5 -translate-y-1/2 rounded-full bg-[var(--accent)]" aria-hidden />
              ) : null}
              <Icon
                className={cn("size-4 shrink-0", active ? "text-[var(--accent)]" : "text-zinc-500 group-hover:text-zinc-300")}
                aria-hidden
              />
              {item.label}
            </Link>
          );
        })}
      </nav>
      <div className="px-6 py-5 text-xs text-zinc-600">Read-only view</div>
    </aside>
  );
}
