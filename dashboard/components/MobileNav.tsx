"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { NAV_ITEMS } from "./nav-items";

export function MobileNav() {
  const pathname = usePathname();

  return (
    <nav
      className="fixed inset-x-4 bottom-[calc(1rem+env(safe-area-inset-bottom))] z-40 flex items-center justify-between gap-1 rounded-[28px] border border-[var(--border-subtle)] bg-white px-2 py-2.5 shadow-[0_10px_30px_rgba(15,23,42,0.14),0_2px_8px_rgba(15,23,42,0.06)] md:hidden"
    >
      {NAV_ITEMS.map((item) => {
        const active = pathname === item.href;
        const Icon = item.icon;
        return (
          <Link
            key={item.href}
            href={item.href}
            className="flex min-h-11 flex-1 items-center justify-center"
          >
            <span
              className={cn(
                "flex w-16 flex-col items-center justify-center gap-0.5 rounded-full py-1.5 transition-all duration-200",
                active && "bg-gradient-to-br from-[var(--accent)] to-indigo-600 shadow-[0_4px_10px_rgba(37,99,235,0.35)]"
              )}
            >
              <Icon
                className={cn("size-5 transition-colors duration-200", active ? "text-white" : "text-slate-600")}
                aria-hidden
              />
              <span
                className={cn(
                  "text-[10px] font-medium leading-none transition-colors duration-200",
                  active ? "text-white" : "text-slate-400"
                )}
              >
                {item.label}
              </span>
            </span>
          </Link>
        );
      })}
    </nav>
  );
}
