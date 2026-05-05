"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { CompareCharts } from "@/components/compare/CompareCharts";
import { CompareLatestTable } from "@/components/compare/CompareLatestTable";
import { CompareMetricSelector } from "@/components/compare/CompareMetricSelector";
import { MarketSearchBox } from "@/components/compare/MarketSearchBox";
import { SelectedMarketsPanel } from "@/components/compare/SelectedMarketsPanel";
import { AppShell } from "@/components/layout/AppShell";
import { EmptyState } from "@/components/ui/EmptyState";
import { ErrorState } from "@/components/ui/ErrorState";
import { LoadingState } from "@/components/ui/LoadingState";
import { MetricCard } from "@/components/ui/MetricCard";
import type { MarketIdentity, MarketListItem } from "@/lib/api";
import { compareMarkets } from "@/lib/api";
import type { CompareMetricId } from "@/lib/compareMetrics";
import { DEFAULT_COMPARE_METRICS } from "@/lib/compareMetrics";

const DEFAULT_MARKETS: MarketIdentity[] = [
  {
    geo_id: "us",
    geo_type: "national",
    name: "United States",
    display_name: "United States",
    state_code: null,
    state_name: null,
    county_fips: null,
    cbsa_code: null,
    zcta: null,
    country_code: "US",
    latitude: null,
    longitude: null,
  },
  {
    geo_id: "metro_19820",
    geo_type: "metro",
    name: "Detroit-Warren-Dearborn, MI",
    display_name: "Detroit-Warren-Dearborn, MI",
    state_code: null,
    state_name: null,
    county_fips: null,
    cbsa_code: "19820",
    zcta: null,
    country_code: "US",
    latitude: null,
    longitude: null,
  },
];

export default function ComparePage() {
  const [selectedMarkets, setSelectedMarkets] =
    useState<MarketIdentity[]>(DEFAULT_MARKETS);
  const [selectedMetrics, setSelectedMetrics] = useState<CompareMetricId[]>(
    DEFAULT_COMPARE_METRICS,
  );

  const selectedGeoIds = useMemo(
    () => selectedMarkets.map((market) => market.geo_id),
    [selectedMarkets],
  );

  const canCompare = selectedMarkets.length >= 2 && selectedMarkets.length <= 5;

  const comparisonQuery = useQuery({
    queryKey: [
      "market-compare",
      selectedGeoIds.join(","),
      selectedMetrics.join(","),
    ],
    queryFn: () =>
      compareMarkets({
        geoIds: selectedGeoIds,
        metrics: selectedMetrics,
        startDate: "2024-01-01",
      }),
    enabled: canCompare,
  });

  function addMarket(market: MarketListItem) {
    if (selectedMarkets.length >= 5) {
      return;
    }

    if (selectedMarkets.some((item) => item.geo_id === market.geo_id)) {
      return;
    }

    setSelectedMarkets((current) => [...current, market]);
  }

  function removeMarket(geoId: string) {
    setSelectedMarkets((current) =>
      current.filter((market) => market.geo_id !== geoId),
    );
  }

  return (
    <AppShell>
      <section className="space-y-8">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Compare
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
            Compare markets
          </h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Compare 2–5 markets by latest score, signal, confidence, and
            time-series metrics.
          </p>
        </div>

        <div className="grid gap-4 md:grid-cols-4">
          <MetricCard
            label="Selected markets"
            value={`${selectedMarkets.length}/5`}
            helperText="Limit enforced by the UI and API."
          />
          <MetricCard
            label="Selected metrics"
            value={selectedMetrics.length}
            helperText="Used for latest and chart comparison."
          />
          <MetricCard
            label="Date window"
            value="Since Jan 2024"
            helperText="Bounded request for MVP performance."
          />
          <MetricCard
            label="Compare status"
            value={canCompare ? "ready" : "needs 2 markets"}
            tone={canCompare ? "good" : "warning"}
          />
        </div>

        <div className="grid gap-5 xl:grid-cols-[360px_minmax(0,1fr)]">
          <div className="space-y-5">
            <SelectedMarketsPanel
              markets={selectedMarkets}
              onRemove={removeMarket}
            />

            <MarketSearchBox
              selectedGeoIds={selectedGeoIds}
              onSelect={addMarket}
            />

            <CompareMetricSelector
              selectedMetrics={selectedMetrics}
              onChange={setSelectedMetrics}
            />
          </div>

          <div className="space-y-5">
            {!canCompare ? (
              <EmptyState
                title="Select at least two markets"
                message="Choose 2–5 markets to run a comparison."
              />
            ) : null}

            {comparisonQuery.isLoading ? (
              <LoadingState title="Loading comparison" />
            ) : null}

            {comparisonQuery.isError ? (
              <ErrorState
                title="Could not load market comparison"
                error={comparisonQuery.error}
              />
            ) : null}

            {comparisonQuery.isSuccess ? (
              <>
                <CompareLatestTable comparison={comparisonQuery.data} />
                <CompareCharts comparison={comparisonQuery.data} />
              </>
            ) : null}
          </div>
        </div>
      </section>
    </AppShell>
  );
}
