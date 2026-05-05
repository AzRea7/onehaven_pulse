import type { MarketMapFeature } from "@/lib/api";
import { getMapMetricDefinition } from "@/lib/mapMetrics";
import { formatMapValue, getFeatureDisplayName } from "@/lib/mapStyle";

type MapTooltipProps = {
  feature: MarketMapFeature | null;
  metric: string;
};

export function MapTooltip({ feature, metric }: MapTooltipProps) {
  const metricDefinition = getMapMetricDefinition(metric);

  if (!feature) {
    return (
      <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
        <p className="text-sm font-medium text-slate-400">
          Hover over a market
        </p>
        <p className="mt-1 text-xs text-slate-500">
          Market details will appear here.
        </p>
      </div>
    );
  }

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
      <p className="text-sm font-semibold text-white">
        {getFeatureDisplayName(feature)}
      </p>
      <p className="mt-1 text-xs text-slate-500">
        {feature.properties.geo_id}
      </p>

      <div className="mt-4 space-y-2 text-sm">
        <div className="flex items-center justify-between gap-4">
          <span className="text-slate-400">{metricDefinition.label}</span>
          <span className="font-medium text-white">
            {formatMapValue(
              feature.properties.value,
              metricDefinition.valueSuffix,
            )}
          </span>
        </div>

        <div className="flex items-center justify-between gap-4">
          <span className="text-slate-400">Cycle phase</span>
          <span className="font-medium text-slate-200">
            {feature.properties.cycle_phase ?? "Unknown"}
          </span>
        </div>

        <div className="flex items-center justify-between gap-4">
          <span className="text-slate-400">Investor signal</span>
          <span className="font-medium text-slate-200">
            {feature.properties.investor_signal ?? "Unknown"}
          </span>
        </div>
      </div>
    </div>
  );
}
