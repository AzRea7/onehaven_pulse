"use client";

import { useQuery } from "@tanstack/react-query";

import { AppShell } from "@/components/layout/AppShell";
import { getHealth } from "@/lib/api";

export default function HomePage() {
  const healthQuery = useQuery({
    queryKey: ["api-health"],
    queryFn: getHealth,
  });

  return (
    <AppShell>
      <section className="space-y-8">
        <div className="space-y-3">
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-400">
            Market Engine Foundation
          </p>

          <h1 className="max-w-3xl text-4xl font-bold tracking-tight text-white md:text-5xl">
            OneHaven Pulse
          </h1>

          <p className="max-w-3xl text-lg leading-8 text-slate-300">
            Real estate market-cycle intelligence for investors. This frontend
            foundation is ready for dashboards, maps, market details, and
            screening workflows.
          </p>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-sm">
          <div className="flex items-start justify-between gap-6">
            <div>
              <h2 className="text-lg font-semibold text-white">
                API Connection
              </h2>
              <p className="mt-2 text-sm text-slate-400">
                The frontend uses an environment-based API client with runtime
                response validation.
              </p>
            </div>

            <div className="rounded-full border border-slate-700 px-3 py-1 text-sm">
              {healthQuery.isLoading && "Checking"}
              {healthQuery.isError && "Unavailable"}
              {healthQuery.isSuccess && healthQuery.data.status}
            </div>
          </div>

          {healthQuery.isError ? (
            <p className="mt-4 text-sm text-red-300">
              API health check failed. Confirm the backend is running and
              NEXT_PUBLIC_API_URL points to the correct API URL.
            </p>
          ) : null}

          {healthQuery.isSuccess ? (
            <pre className="mt-4 overflow-auto rounded-xl bg-slate-950 p-4 text-sm text-slate-300">
              {JSON.stringify(healthQuery.data, null, 2)}
            </pre>
          ) : null}
        </div>
      </section>
    </AppShell>
  );
}
