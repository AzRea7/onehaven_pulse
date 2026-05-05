"use client";

import Link from "next/link";
import { useQuery } from "@tanstack/react-query";

import { getHealth } from "@/lib/api";

export function TopNav() {
  const healthQuery = useQuery({
    queryKey: ["api-health"],
    queryFn: getHealth,
    staleTime: 60_000,
  });

  const apiLabel = healthQuery.isSuccess
    ? "API healthy"
    : healthQuery.isLoading
      ? "Checking API"
      : "API unavailable";

  return (
    <header className="sticky top-0 z-20 border-b border-slate-800 bg-slate-950/90 backdrop-blur">
      <div className="flex h-16 items-center justify-between gap-4 px-5 lg:px-8">
        <div>
          <p className="text-sm font-medium text-slate-400">OneHaven</p>
          <p className="text-base font-semibold text-white">
            Market Intelligence Platform
          </p>
        </div>

        <div className="flex items-center gap-3">
          <Link
            href="/markets/metro_19820"
            className="hidden rounded-full border border-slate-700 px-3 py-1.5 text-sm text-slate-300 transition hover:border-slate-500 hover:text-white md:inline-flex"
          >
            Detroit
          </Link>

          <span
            className={
              healthQuery.isSuccess
                ? "rounded-full border border-emerald-800 bg-emerald-950 px-3 py-1.5 text-xs font-medium text-emerald-200"
                : healthQuery.isLoading
                  ? "rounded-full border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs font-medium text-slate-300"
                  : "rounded-full border border-red-800 bg-red-950 px-3 py-1.5 text-xs font-medium text-red-200"
            }
          >
            {apiLabel}
          </span>
        </div>
      </div>
    </header>
  );
}
