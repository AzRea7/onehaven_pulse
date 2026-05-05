import Link from "next/link";

import { AppShell } from "@/components/layout/AppShell";
import { EmptyState } from "@/components/ui/EmptyState";
import { MetricCard } from "@/components/ui/MetricCard";

const quickMarkets = [
  {
    geoId: "us",
    label: "United States",
    note: "National market context",
  },
  {
    geoId: "metro_19820",
    label: "Detroit-Warren-Dearborn, MI",
    note: "Strong partial-data MVP market",
  },
];

export default function MarketsPage() {
  return (
    <AppShell>
      <section className="space-y-8">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Markets
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
            Market search and discovery
          </h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Story 6.4 adds the real market detail page. Full market search comes
            later with the screener/search story.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <MetricCard label="Route" value="/markets" />
          <MetricCard label="Dynamic detail" value="/markets/[geo_id]" />
          <MetricCard label="Primary test market" value="metro_19820" />
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          {quickMarkets.map((market) => (
            <Link
              key={market.geoId}
              href={`/markets/${market.geoId}`}
              className="rounded-2xl border border-slate-800 bg-slate-900 p-5 transition hover:border-slate-600"
            >
              <p className="text-lg font-semibold text-white">{market.label}</p>
              <p className="mt-1 text-sm text-slate-400">{market.geoId}</p>
              <p className="mt-3 text-sm text-slate-500">{market.note}</p>
            </Link>
          ))}
        </div>

        <EmptyState
          title="Full market search coming soon"
          message="Story 6.6 will add server-backed filters and search. For now, quick links validate market detail rendering."
        />
      </section>
    </AppShell>
  );
}
