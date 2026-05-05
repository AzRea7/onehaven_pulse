"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const navItems = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/map", label: "Market Map" },
  { href: "/markets", label: "Markets" },
  { href: "/compare", label: "Compare" },
  { href: "/screener", label: "Screener" },
  { href: "/admin/source-freshness", label: "Source Freshness" },
];

function isActive(pathname: string, href: string): boolean {
  if (href === "/dashboard") {
    return pathname === "/" || pathname === "/dashboard";
  }

  return pathname === href || pathname.startsWith(`${href}/`);
}

export function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="hidden min-h-screen w-72 shrink-0 border-r border-slate-800 bg-slate-950 px-4 py-5 lg:block">
      <Link href="/dashboard" className="block px-3">
        <p className="text-lg font-semibold tracking-tight text-white">
          OneHaven Pulse
        </p>
        <p className="mt-1 text-xs uppercase tracking-[0.2em] text-slate-500">
          Market Engine
        </p>
      </Link>

      <nav className="mt-8 space-y-1">
        {navItems.map((item) => {
          const active = isActive(pathname, item.href);

          return (
            <Link
              key={item.href}
              href={item.href}
              className={
                active
                  ? "block rounded-xl bg-slate-800 px-3 py-2 text-sm font-medium text-white"
                  : "block rounded-xl px-3 py-2 text-sm font-medium text-slate-400 transition hover:bg-slate-900 hover:text-white"
              }
            >
              {item.label}
            </Link>
          );
        })}
      </nav>
    </aside>
  );
}
