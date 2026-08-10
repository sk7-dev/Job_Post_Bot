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
      <body className="min-h-full bg-[var(--page-bg)] text-[var(--text-primary)]">
        <div className="mx-auto flex min-h-screen w-full max-w-[1640px] flex-col md:flex-row md:gap-5 md:p-6">
          <AppSidebar />
          <div className="flex min-w-0 flex-1 flex-col">
            <PageHeader />
            <main className="flex-1 px-4 pb-24 pt-6 sm:px-6 md:px-0 md:pb-0 md:pt-0">{children}</main>
          </div>
        </div>

        <MobileNav />
      </body>
    </html>
  );
}
