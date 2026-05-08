"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { cx, theme } from "@/lib/theme";

const NAV_ITEMS = [
  { href: "/", label: "Home" },
  { href: "/workflow", label: "Workflow" },
  { href: "/screener", label: "Screener" },
  { href: "/markets/metro_19820", label: "Command" },
  { href: "/compare", label: "Compare" },
  { href: "/map", label: "Map" },
  { href: "/dashboard", label: "Dashboard" },
  { href: "/admin/source-freshness", label: "Data" },
];

function isActive(pathname: string, href: string): boolean {
  if (href === "/") {
    return pathname === "/";
  }

  return pathname === href || pathname.startsWith(`${href}/`);
}

export function AppNavigation() {
  const pathname = usePathname();

  return (
    <header className={theme.nav}>
      <div className={theme.navInner}>
        <Link href="/" className="group min-w-fit">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-2xl border border-[#7CFFB2]/40 bg-[#7CFFB2]/10 text-sm font-black text-[#7CFFB2] shadow-[0_0_30px_rgba(124,255,178,0.18)]">
              OS
            </div>
            <div>
              <div className="text-sm font-semibold uppercase tracking-[0.22em] text-white">
                OneStream Pulse
              </div>
              <div className="text-xs text-[#7CFFB2]/80">
                Real estate market intelligence
              </div>
            </div>
          </div>
        </Link>

        <nav className="flex gap-2 overflow-x-auto pb-1 lg:pb-0">
          {NAV_ITEMS.map((item) => {
            const active = isActive(pathname, item.href);

            return (
              <Link
                key={item.href}
                href={item.href}
                className={cx(
                  "min-w-fit rounded-full border px-3.5 py-2 text-sm font-semibold transition",
                  active
                    ? "border-[#7CFFB2] bg-[#7CFFB2] text-[#06110A] shadow-[0_0_28px_rgba(124,255,178,0.25)]"
                    : "border-white/10 bg-white/[0.035] text-[#DDE7F3] hover:border-white/25 hover:bg-white/[0.07]",
                )}
              >
                {item.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
