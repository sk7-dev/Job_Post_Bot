import { Brand } from "./Brand";

export function PageHeader() {
  return (
    <header className="sticky top-0 z-30 flex h-14 shrink-0 items-center border-b border-[var(--border-subtle)] bg-[var(--sidebar-bg)]/95 px-4 backdrop-blur-sm sm:px-6 md:hidden">
      <Brand compact />
    </header>
  );
}
