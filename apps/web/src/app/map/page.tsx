"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { AppShell } from "@/components/layout/AppShell";
import { MapDetailDrawer } from "@/components/map/MapDetailDrawer";
import { MapLegend } from "@/components/map/MapLegend";
import { MapTooltip } from "@/components/map/MapTooltip";
import { MarketGeoMap } from "@/components/map/MarketGeoMap";
import { MetricSelector } from "@/components/map/MetricSelector";
import { EmptyState } from "@/components/ui/EmptyState";
import { ErrorState } from "@/components/ui/ErrorState";
import { LoadingState } from "@/components/ui/LoadingState";
import { MetricCard } from "@/components/ui/MetricCard";
import type { MarketMapFeature } from "@/lib/api";
import { getMarketMap } from "@/lib/api";
import type { MapMetricId } from "@/lib/mapMetrics";
import { getMapMetricDefinition } from "@/lib/mapMetrics";
import { formatMapValue } from "@/lib/mapStyle";

export default function MapPage() {
  const [metric, setMetric] = useState<MapMetricId>("composite_cycle_score");
  const [hoveredFeature, setHoveredFeature] = useState<MarketMapFeature | null>(
    null,
  );
  const [selectedFeature, setSelectedFeature] =
    useState<MarketMapFeature | null>(null);

  const metricDefinition = getMapMetricDefinition(metric);

  const mapQuery = useQuery({
    queryKey: ["market-map", "metro", metric],
    queryFn: () =>
      getMarketMap({
        geoType: "metro",
        metric,
      }),
    staleTime: 5 * 60 * 1000,
  });

  const features = mapQuery.data?.features ?? [];

  const mapStats = useMemo(() => {
    const known = features.filter(
      (feature) =>
        feature.properties.value !== null &&
        feature.properties.value !== undefined,
    );

    const values = known
      .map((feature) => feature.properties.value)
      .filter((value): value is number => typeof value === "number");

    return {
      total: features.length,
      known: known.length,
      unknown: features.length - known.length,
      min: values.length ? Math.min(...values) : null,
      max: values.length ? Math.max(...values) : null,
      latestPeriod:
        features.find((feature) => feature.properties.period_month)
          ?.properties.period_month ?? null,
    };
  }, [features]);

  return (
    <AppShell>
      <section className="space-y-8">
        <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
              Market Map
            </p>
            <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
              Metro market map
            </h1>
            <p className="mt-3 max-w-3xl text-slate-400">
              Explore market-cycle metrics across metro areas. Unknown or
              missing data is rendered as a neutral state, not as a negative
              signal.
            </p>
          </div>

          <div className="w-full max-w-sm">
            <MetricSelector value={metric} onChange={setMetric} />
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-4">
          <MetricCard
            label="Metric"
            value={metricDefinition.label}
            helperText={metricDefinition.description}
          />
          <MetricCard
            label="Markets"
            value={mapStats.total}
            helperText="Metro features returned by the API."
          />
          <MetricCard
            label="Known values"
            value={mapStats.known}
            helperText={`${mapStats.unknown} unknown or null.`}
            tone={mapStats.known > 0 ? "good" : "warning"}
          />
          <MetricCard
            label="Value range"
            value={`${formatMapValue(
              mapStats.min,
              metricDefinition.valueSuffix,
            )} – ${formatMapValue(mapStats.max, metricDefinition.valueSuffix)}`}
            helperText={mapStats.latestPeriod ?? "Latest available period"}
            tone="neutral"
          />
        </div>

        {mapQuery.isLoading ? (
          <LoadingState title="Loading market map" />
        ) : null}

        {mapQuery.isError ? (
          <ErrorState title="Could not load market map" error={mapQuery.error} />
        ) : null}

        {mapQuery.isSuccess && features.length === 0 ? (
          <EmptyState
            title="No map features available"
            message="The selected metric returned no map-ready features. Try a different metric or check source coverage."
          />
        ) : null}

        {mapQuery.isSuccess && features.length > 0 ? (
          <div className="grid gap-5 xl:grid-cols-[minmax(0,1fr)_360px]">
            <div className="space-y-5">
              <MarketGeoMap
                features={features}
                metric={metric}
                selectedFeature={selectedFeature}
                onHover={setHoveredFeature}
                onSelect={setSelectedFeature}
              />
            </div>

            <div className="space-y-5">
              <MapTooltip
                feature={hoveredFeature ?? selectedFeature}
                metric={metric}
              />
              <MapLegend metricLabel={metricDefinition.label} />
              <MapDetailDrawer
                feature={selectedFeature}
                metric={metric}
                onClose={() => setSelectedFeature(null)}
              />
            </div>
          </div>
        ) : null}
      </section>
    </AppShell>
  );
}
