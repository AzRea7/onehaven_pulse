"use client";

import { use } from "react";
import { useQuery } from "@tanstack/react-query";

import { AppShell } from "@/components/layout/AppShell";
import { MarketChartsGrid } from "@/components/markets/MarketChartsGrid";
import { MarketEvidencePanel } from "@/components/markets/MarketEvidencePanel";
import { MarketHeader } from "@/components/markets/MarketHeader";
import { MarketRisksPanel } from "@/components/markets/MarketRisksPanel";
import { MarketSourceFreshnessPanel } from "@/components/markets/MarketSourceFreshnessPanel";
import { ScoreBreakdownPanel } from "@/components/markets/ScoreBreakdownPanel";
import { ErrorState } from "@/components/ui/ErrorState";
import { LoadingState } from "@/components/ui/LoadingState";
import {
  getMarketContext,
  getMarketCoverage,
  getMarketDetail,
  getMarketTimeSeries,
} from "@/lib/api";

const MARKET_DETAIL_METRICS = [
  "home_price_yoy",
  "zhvi_yoy",
  "rent_yoy",
  "zori_yoy",
  "payment_to_income_ratio",
  "price_to_income_ratio",
  "unemployment_rate",
  "building_permits",
  "composite_cycle_score",
];

type MarketDetailPageProps = {
  params: Promise<{
    geo_id: string;
  }>;
};

export default function MarketDetailPage({ params }: MarketDetailPageProps) {
  const { geo_id } = use(params);

  const detailQuery = useQuery({
    queryKey: ["market-detail", geo_id],
    queryFn: () => getMarketDetail(geo_id),
  });

  const coverageQuery = useQuery({
    queryKey: ["market-coverage", geo_id],
    queryFn: () => getMarketCoverage(geo_id),
  });

  const contextQuery = useQuery({
    queryKey: ["market-context", geo_id],
    queryFn: () => getMarketContext(geo_id),
  });

  const timeseriesQuery = useQuery({
    queryKey: ["market-timeseries", geo_id, MARKET_DETAIL_METRICS.join(",")],
    queryFn: () =>
      getMarketTimeSeries({
        geoId: geo_id,
        metrics: MARKET_DETAIL_METRICS,
        startDate: "2024-01-01",
      }),
  });

  const isLoading =
    detailQuery.isLoading ||
    coverageQuery.isLoading ||
    contextQuery.isLoading ||
    timeseriesQuery.isLoading;

  const firstError =
    detailQuery.error ||
    coverageQuery.error ||
    contextQuery.error ||
    timeseriesQuery.error;

  return (
    <AppShell>
      <section className="space-y-8">
        {isLoading ? <LoadingState title="Loading market detail" /> : null}

        {firstError ? (
          <ErrorState title={`Could not load market ${geo_id}`} error={firstError} />
        ) : null}

        {detailQuery.data && coverageQuery.data && contextQuery.data ? (
          <>
            <MarketHeader
              detail={detailQuery.data}
              coverage={coverageQuery.data}
              context={contextQuery.data}
            />

            <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
              <div className="space-y-5">
                <MarketEvidencePanel context={contextQuery.data} />
                {timeseriesQuery.data ? (
                  <MarketChartsGrid timeseries={timeseriesQuery.data} />
                ) : null}
              </div>

              <div className="space-y-5">
                <ScoreBreakdownPanel detail={detailQuery.data} />
                <MarketRisksPanel context={contextQuery.data} />
                <MarketSourceFreshnessPanel
                  items={detailQuery.data.source_freshness ?? []}
                />
              </div>
            </div>
          </>
        ) : null}
      </section>
    </AppShell>
  );
}
