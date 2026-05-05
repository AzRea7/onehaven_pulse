"use client";

import type { MapMetricId } from "@/lib/mapMetrics";
import { MAP_METRICS } from "@/lib/mapMetrics";

type MetricSelectorProps = {
  value: MapMetricId;
  onChange: (metric: MapMetricId) => void;
};

export function MetricSelector({ value, onChange }: MetricSelectorProps) {
  return (
    <label className="block">
      <span className="text-sm font-medium text-slate-300">Map metric</span>
      <select
        value={value}
        onChange={(event) => onChange(event.target.value as MapMetricId)}
        className="mt-2 w-full rounded-xl border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-white outline-none transition focus:border-slate-400"
      >
        {MAP_METRICS.map((metric) => (
          <option key={metric.id} value={metric.id}>
            {metric.label}
          </option>
        ))}
      </select>
    </label>
  );
}
