"use client";

import Link from "next/link";

import { CycleBadge } from "@/components/ui/CycleBadge";
import { InvestorSignalBadge } from "@/components/ui/InvestorSignalBadge";
import type { MarketMapFeature } from "@/lib/api";
import { getMapMetricDefinition } from "@/lib/mapMetrics";
import { formatMapValue, getFeatureDisplayName } from "@/lib/mapStyle";

type MapDetailDrawerProps = {
  feature: MarketMapFeature | null;
  metric: string;
  onClose: () => void;
};

export function MapDetailDrawer({
  feature,
  metric,
  onClose,
}: MapDetailDrawerProps) {
  const metricDefinition = getMapMetricDefinition(metric);

  if (!feature) {
    return null;
  }

  return (
    <aside className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div className="flex items-start justify-between gap-4">
        <div>
          <p className="text-xs font-medium uppercase tracking-[0.2em] text-slate-500">
            Selected market
          </p>
          <h2 className="mt-2 text-xl font-semibold text-white">
            {getFeatureDisplayName(feature)}
          </h2>
          <p className="mt-1 text-sm text-slate-500">
            {feature.properties.geo_id}
          </p>
        </div>

        <button
          type="button"
          onClick={onClose}
          className="rounded-lg border border-slate-700 px-2 py-1 text-sm text-slate-300 transition hover:border-slate-500 hover:text-white"
        >
          Close
        </button>
      </div>

      <div className="mt-5 flex flex-wrap gap-2">
        <CycleBadge phase={feature.properties.cycle_phase} />
        <InvestorSignalBadge signal={feature.properties.investor_signal} />
      </div>

      <div className="mt-6 rounded-xl border border-slate-800 bg-slate-950 p-4">
        <p className="text-sm text-slate-400">{metricDefinition.label}</p>
        <p className="mt-2 text-2xl font-semibold text-white">
          {formatMapValue(
            feature.properties.value,
            metricDefinition.valueSuffix,
          )}
        </p>
        <p className="mt-2 text-sm text-slate-500">
          Period: {feature.properties.period_month ?? "latest available"}
        </p>
        <p className="mt-1 text-sm text-slate-500">
          Status: {feature.properties.data_status ?? "unknown"}
        </p>
      </div>

      <Link
        href={`/markets/${feature.properties.geo_id}`}
        className="mt-5 inline-flex w-full justify-center rounded-xl bg-white px-4 py-2 text-sm font-medium text-slate-950 transition hover:bg-slate-200"
      >
        Open market detail
      </Link>
    </aside>
  );
}
