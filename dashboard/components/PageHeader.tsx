import Link from "next/link";
import { Rocket } from "lucide-react";
import { Brand } from "./Brand";

export function PageHeader() {
  return (
    <header className="sticky top-0 z-30 flex h-14 shrink-0 items-center justify-between border-b border-[var(--border-subtle)] bg-[var(--sidebar-bg)]/95 px-4 backdrop-blur-sm sm:px-6 md:hidden">
      <Brand compact />
      <Link
        href="/deploy"
        className="flex items-center gap-1.5 rounded-full bg-gradient-to-br from-[var(--accent)] to-indigo-600 py-1.5 pl-2.5 pr-3 text-xs font-semibold text-white shadow-[0_2px_8px_rgba(37,99,235,0.3)] transition-transform active:scale-95"
      >
        <Rocket className="size-3.5" aria-hidden />
        <span className="hidden min-[360px]:inline">Deploy it yourself</span>
        <span className="min-[360px]:hidden">Deploy</span>
      </Link>
    </header>
  );
}
