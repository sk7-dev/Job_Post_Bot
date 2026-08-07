import { Activity, Briefcase, LayoutDashboard, Radio, SlidersHorizontal, type LucideIcon } from "lucide-react";

export interface NavItem {
  href: string;
  label: string;
  icon: LucideIcon;
}

export const NAV_ITEMS: NavItem[] = [
  { href: "/", label: "Dashboard", icon: LayoutDashboard },
  { href: "/jobs", label: "Jobs", icon: Briefcase },
  { href: "/sources", label: "Sources", icon: Radio },
  { href: "/filters", label: "Filters", icon: SlidersHorizontal },
  { href: "/activity", label: "Activity", icon: Activity },
];
