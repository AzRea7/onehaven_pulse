"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import {
  ScreenerFilters,
  type ScreenerFiltersState,
} from "@/components/screener/ScreenerFilters";
import { ScreenerResultsTable } from "@/components/screener/ScreenerResultsTable";
import { AppShell } from "@/components/layout/AppShell";
import { EmptyState } from "@/components/ui/EmptyState";
import { ErrorState } from "@/components/ui/ErrorState";
import { LoadingState } from "@/components/ui/LoadingState";
import { MetricCard } from "@/components/ui/MetricCard";
import { screenMarkets } from "@/lib/api";

const DEFAULT_FILTERS: ScreenerFiltersState = {
  geoType: "metro",
  state: "",
  cyclePhase: "",
  investorSignal: "",
  minConfidence: "0.5",
  minPriceGrowth: "",
  maxPriceGrowth: "",
  minRentGrowth: "",
  maxInventoryGrowth: "",
  maxPaymentToIncome: "",
};

function parseOptionalNumber(value: string): number | undefined {
  const trimmed = value.trim();

  if (!trimmed) {
    return undefined;
  }

  const parsed = Number(trimmed);

  if (Number.isNaN(parsed)) {
    return undefined;
  }

  return parsed;
}

export default function ScreenerPage() {
  const [filters, setFilters] = useState<ScreenerFiltersState>(DEFAULT_FILTERS);

  const queryArgs = useMemo(
    () => ({
      geoType: filters.geoType || undefined,
      state: filters.state.trim() || undefined,
      cyclePhase: filters.cyclePhase || undefined,
      investorSignal: filters.investorSignal || undefined,
      minConfidence: parseOptionalNumber(filters.minConfidence),
      minPriceGrowth: parseOptionalNumber(filters.minPriceGrowth),
      maxPriceGrowth: parseOptionalNumber(filters.maxPriceGrowth),
      minRentGrowth: parseOptionalNumber(filters.minRentGrowth),
      maxInventoryGrowth: parseOptionalNumber(filters.maxInventoryGrowth),
      maxPaymentToIncome: parseOptionalNumber(filters.maxPaymentToIncome),
      limit: 50,
      offset: 0,
    }),
    [filters],
  );

  const screenerQuery = useQuery({
    queryKey: ["market-screener", queryArgs],
    queryFn: () => screenMarkets(queryArgs),
  });

  return (
    <AppShell>
      <section className="space-y-8">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Screener
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
            Market screener
          </h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Filter markets by cycle phase, signal, confidence, price growth,
            rent growth, affordability, and inventory pressure.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-4">
          <MetricCard
            label="Results"
            value={screenerQuery.data?.total ?? "—"}
            helperText="Total matching markets."
          />
          <MetricCard
            label="Geo type"
            value={filters.geoType || "Any"}
            helperText="Current jurisdiction level."
          />
          <MetricCard
            label="Minimum confidence"
            value={filters.minConfidence || "Any"}
            helperText="0 to 1 scale."
          />
          <MetricCard
            label="Limit"
            value={50}
            helperText="First page of results."
          />
        </div>

        <ScreenerFilters filters={filters} onChange={setFilters} />

        {screenerQuery.isLoading ? (
          <LoadingState title="Loading screener results" />
        ) : null}

        {screenerQuery.isError ? (
          <ErrorState
            title="Could not load screener results"
            error={screenerQuery.error}
          />
        ) : null}

        {screenerQuery.isSuccess && screenerQuery.data.items.length === 0 ? (
          <EmptyState
            title="No markets match these filters"
            message="Try lowering the confidence threshold or clearing metric filters."
          />
        ) : null}

        {screenerQuery.isSuccess && screenerQuery.data.items.length > 0 ? (
          <ScreenerResultsTable items={screenerQuery.data.items} />
        ) : null}
      </section>
    </AppShell>
  );
}
