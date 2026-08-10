import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { AppSidebar } from "@/components/AppSidebar";
import { MobileNav } from "@/components/MobileNav";
import { PageHeader } from "@/components/PageHeader";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Job Watcher",
  description: "Read-only dashboard for the job watcher bot",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="min-h-full bg-[var(--page-bg)] text-zinc-100">
        <div
          aria-hidden
          className="pointer-events-none fixed inset-0 -z-10 overflow-hidden bg-[var(--page-bg)]"
        >
          <div className="absolute right-[-8%] top-[-18%] h-[640px] w-[920px] rounded-full bg-[#3b5bdb]/[0.14] blur-[150px]" />
          <div className="absolute right-[2%] bottom-[-22%] h-[520px] w-[640px] rounded-full bg-[#6b46c1]/[0.09] blur-[150px]" />
        </div>

        <div className="mx-auto flex w-full max-w-[1640px] flex-col gap-4 px-2 py-3 sm:px-4 sm:py-5 md:flex-row md:px-6 md:py-8">
          <AppSidebar />
          <div className="relative flex min-h-[82vh] min-w-0 flex-1 flex-col overflow-hidden rounded-[18px] border border-white/[0.08] bg-[var(--shell-bg)] shadow-[0_25px_80px_rgba(0,0,0,0.45)]">
            <PageHeader />
            <main className="flex-1 px-4 pb-24 pt-6 sm:px-6 md:px-8 md:pb-10">{children}</main>
          </div>
        </div>

        <MobileNav />
      </body>
    </html>
  );
}
