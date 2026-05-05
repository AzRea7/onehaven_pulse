"use client";

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { NationalContextSummary } from "@/components/dashboard/NationalContextSummary";
import { SourceFreshnessSummary } from "@/components/dashboard/SourceFreshnessSummary";
import { AppShell } from "@/components/layout/AppShell";
import { ErrorState } from "@/components/ui/ErrorState";
import { LineChartCard } from "@/components/ui/LineChartCard";
import { LoadingState } from "@/components/ui/LoadingState";
import { MetricCard } from "@/components/ui/MetricCard";
import {
  getMarketContext,
  getMarketDetail,
  getMarketTimeSeries,
  getSourceFreshness,
} from "@/lib/api";
import { formatDate, formatNumber, formatPercent } from "@/lib/format";

const NATIONAL_GEO_ID = "us";

const NATIONAL_TIMESERIES_METRICS = [
  "home_price_yoy",
  "rent_yoy",
  "mortgage_rate_30y",
  "unemployment_rate",
  "composite_cycle_score",
];

function useNationalDashboardData() {
  const marketDetailQuery = useQuery({
    queryKey: ["market-detail", NATIONAL_GEO_ID],
    queryFn: () => getMarketDetail(NATIONAL_GEO_ID),
  });

  const contextQuery = useQuery({
    queryKey: ["market-context", NATIONAL_GEO_ID],
    queryFn: () => getMarketContext(NATIONAL_GEO_ID),
  });

  const timeseriesQuery = useQuery({
    queryKey: [
      "market-timeseries",
      NATIONAL_GEO_ID,
      NATIONAL_TIMESERIES_METRICS.join(","),
    ],
    queryFn: () =>
      getMarketTimeSeries({
        geoId: NATIONAL_GEO_ID,
        metrics: NATIONAL_TIMESERIES_METRICS,
        startDate: "2024-01-01",
      }),
  });

  const freshnessQuery = useQuery({
    queryKey: ["source-freshness"],
    queryFn: getSourceFreshness,
  });

  return {
    marketDetailQuery,
    contextQuery,
    timeseriesQuery,
    freshnessQuery,
  };
}

export default function DashboardPage() {
  const {
    marketDetailQuery,
    contextQuery,
    timeseriesQuery,
    freshnessQuery,
  } = useNationalDashboardData();

  const isLoading =
    marketDetailQuery.isLoading ||
    contextQuery.isLoading ||
    timeseriesQuery.isLoading ||
    freshnessQuery.isLoading;

  const firstError =
    marketDetailQuery.error ||
    contextQuery.error ||
    timeseriesQuery.error ||
    freshnessQuery.error;

  const chartRows = useMemo(() => {
    const items = timeseriesQuery.data?.items ?? [];

    return items.map((item) => ({
      period: formatDate(item.period_month),
      period_month: item.period_month,
      home_price_yoy: item.values.home_price_yoy ?? null,
      rent_yoy: item.values.rent_yoy ?? null,
      mortgage_rate_30y: item.values.mortgage_rate_30y ?? null,
      unemployment_rate: item.values.unemployment_rate ?? null,
      composite_cycle_score: item.values.composite_cycle_score ?? null,
    }));
  }, [timeseriesQuery.data]);

  return (
    <AppShell>
      <section className="space-y-8">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Dashboard
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
            National housing market overview
          </h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Current U.S. market-cycle context, macro pressure, price/rent
            momentum, labor signal, and data freshness.
          </p>
        </div>

        {isLoading ? <LoadingState title="Loading national dashboard" /> : null}

        {firstError ? (
          <ErrorState
            title="Could not load national dashboard"
            error={firstError}
          />
        ) : null}

        {marketDetailQuery.data && contextQuery.data ? (
          <>
            <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
              <MetricCard
                label="Cycle phase"
                value={marketDetailQuery.data.cycle_phase}
                helperText={`Latest period: ${formatDate(
                  marketDetailQuery.data.latest_period,
                )}`}
                tone="neutral"
              />

              <MetricCard
                label="Investor signal"
                value={marketDetailQuery.data.investor_signal}
                helperText={`Confidence: ${formatPercent(
                  (marketDetailQuery.data.confidence_score ?? 0) * 100,
                  { digits: 0 },
                )}`}
                tone="neutral"
              />

              <MetricCard
                label="Composite score"
                value={formatNumber(
                  marketDetailQuery.data.score_breakdown
                    ?.composite_cycle_score,
                  { maximumFractionDigits: 0 },
                )}
                helperText="Higher scores imply stronger cycle conditions."
              />

              <MetricCard
                label="Data completeness"
                value={formatPercent(
                  marketDetailQuery.data.score_breakdown?.data_completeness,
                  { digits: 0 },
                )}
                helperText="Share of score components currently available."
              />
            </div>

            <NationalContextSummary context={contextQuery.data} />
          </>
        ) : null}

        {timeseriesQuery.data ? (
          <div className="grid gap-5 xl:grid-cols-2">
            <LineChartCard
              title="Home price growth"
              description="Year-over-year national home-price growth."
              data={chartRows}
              xKey="period"
              yKey="home_price_yoy"
              valueSuffix="%"
            />

            <LineChartCard
              title="Rent growth"
              description="Year-over-year national rent growth."
              data={chartRows}
              xKey="period"
              yKey="rent_yoy"
              valueSuffix="%"
            />

            <LineChartCard
              title="Mortgage rate"
              description="30-year fixed mortgage rate, monthly average."
              data={chartRows}
              xKey="period"
              yKey="mortgage_rate_30y"
              valueSuffix="%"
            />

            <LineChartCard
              title="Unemployment"
              description="National unemployment rate."
              data={chartRows}
              xKey="period"
              yKey="unemployment_rate"
              valueSuffix="%"
            />

            <LineChartCard
              title="Composite cycle score"
              description="National market-cycle score over time."
              data={chartRows}
              xKey="period"
              yKey="composite_cycle_score"
            />
          </div>
        ) : null}

        {freshnessQuery.data ? (
          <SourceFreshnessSummary items={freshnessQuery.data} />
        ) : null}
      </section>
    </AppShell>
  );
}
