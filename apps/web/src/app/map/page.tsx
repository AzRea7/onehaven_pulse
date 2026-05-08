"use client";

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { AppShell } from "@/components/layout/AppShell";
import { MapDetailDrawer } from "@/components/map/MapDetailDrawer";
import { MarketGeoMap } from "@/components/map/MarketGeoMap";
import { MapLegend } from "@/components/map/MapLegend";
import { MapTooltip } from "@/components/map/MapTooltip";
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

type MapScope = "country" | "state";

const STATE_OPTIONS = [
  "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
  "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
  "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
  "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
  "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
];

export default function MapPage() {
  const [metric, setMetric] = useState<MapMetricId>("composite_cycle_score");
  const [scope, setScope] = useState<MapScope>("country");
  const [stateCode, setStateCode] = useState("MI");
  const [hoveredFeature, setHoveredFeature] = useState<MarketMapFeature | null>(null);
  const [selectedFeature, setSelectedFeature] = useState<MarketMapFeature | null>(null);

  const metricDefinition = getMapMetricDefinition(metric);
  const activeState = scope === "state" ? stateCode : undefined;

  useEffect(() => {
    setHoveredFeature(null);
    setSelectedFeature(null);
  }, [metric, scope, stateCode]);

  const mapQuery = useQuery({
    queryKey: ["market-map", "metro", metric, scope, activeState ?? "US"],
    queryFn: () =>
      getMarketMap({
        geoType: "metro",
        metric,
        state: activeState,
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
        features.find((feature) => feature.properties.period_month)?.properties
          .period_month ?? null,
    };
  }, [features]);

  return (
    <AppShell>
      <section className="space-y-8">
        <div className="flex flex-col gap-5 xl:flex-row xl:items-start xl:justify-between">
          <div>
            <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
              Market Map
            </p>
            <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
              Real estate market boundary map
            </h1>
            <p className="mt-3 max-w-3xl text-slate-400">
              View real market boundaries, switch between whole-country and
              single-state scope, and inspect investment signals directly on
              the map. State mode loads only that state's market geometries
              from the API.
            </p>
          </div>

          <div className="grid w-full max-w-4xl gap-4 md:grid-cols-3">
            <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
              <p className="text-sm font-medium text-slate-300">Map scope</p>
              <div className="mt-3 grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={() => setScope("country")}
                  className={`rounded-xl px-3 py-2 text-sm font-medium transition ${
                    scope === "country"
                      ? "bg-white text-slate-950"
                      : "bg-slate-950 text-slate-300 hover:bg-slate-800"
                  }`}
                >
                  Whole USA
                </button>
                <button
                  type="button"
                  onClick={() => setScope("state")}
                  className={`rounded-xl px-3 py-2 text-sm font-medium transition ${
                    scope === "state"
                      ? "bg-white text-slate-950"
                      : "bg-slate-950 text-slate-300 hover:bg-slate-800"
                  }`}
                >
                  State
                </button>
              </div>
            </div>

            <label className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
              <span className="text-sm font-medium text-slate-300">State filter</span>
              <select
                value={stateCode}
                onChange={(event) => setStateCode(event.target.value)}
                disabled={scope !== "state"}
                className="mt-3 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition focus:border-slate-400 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {STATE_OPTIONS.map((code) => (
                  <option key={code} value={code}>
                    {code}
                  </option>
                ))}
              </select>
              <p className="mt-2 text-xs text-slate-500">
                Loads only markets for the selected state when State mode is active.
              </p>
            </label>

            <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
              <MetricSelector value={metric} onChange={setMetric} />
            </div>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-4">
          <MetricCard
            label="Metric"
            value={metricDefinition.label}
            helperText={metricDefinition.description}
          />
          <MetricCard
            label="Scope"
            value={scope === "state" ? stateCode : "USA"}
            helperText={
              scope === "state"
                ? "Only markets in the selected state are loaded."
                : "All available metro markets in the U.S. are loaded."
            }
          />
          <MetricCard
            label="Markets loaded"
            value={mapStats.total}
            helperText={`${mapStats.known} with known values, ${mapStats.unknown} unknown.`}
            tone={mapStats.total > 0 ? "good" : "warning"}
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

        {mapQuery.isLoading ? <LoadingState title="Loading market map" /> : null}

        {mapQuery.isError ? (
          <ErrorState title="Could not load market map" error={mapQuery.error} />
        ) : null}

        {mapQuery.isSuccess && features.length === 0 ? (
          <EmptyState
            title="No map features available"
            message={
              scope === "state"
                ? `No map-ready metro boundaries were returned for ${stateCode}.`
                : "The selected metric returned no map-ready features."
            }
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
