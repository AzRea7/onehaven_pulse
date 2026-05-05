"use client";

import type { CompareMetricId } from "@/lib/compareMetrics";
import { COMPARE_METRICS } from "@/lib/compareMetrics";

type CompareMetricSelectorProps = {
  selectedMetrics: CompareMetricId[];
  onChange: (metrics: CompareMetricId[]) => void;
};

export function CompareMetricSelector({
  selectedMetrics,
  onChange,
}: CompareMetricSelectorProps) {
  function toggle(metric: CompareMetricId) {
    if (selectedMetrics.includes(metric)) {
      const next = selectedMetrics.filter((item) => item !== metric);
      onChange(next.length > 0 ? next : selectedMetrics);
      return;
    }

    onChange([...selectedMetrics, metric]);
  }

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <p className="text-base font-semibold text-white">Metrics</p>
      <p className="mt-1 text-sm text-slate-400">
        Select metrics to compare across markets.
      </p>

      <div className="mt-4 grid gap-2">
        {COMPARE_METRICS.map((metric) => {
          const active = selectedMetrics.includes(metric.id);

          return (
            <button
              key={metric.id}
              type="button"
              onClick={() => toggle(metric.id)}
              className={
                active
                  ? "rounded-xl border border-sky-700 bg-sky-950 px-4 py-3 text-left"
                  : "rounded-xl border border-slate-800 bg-slate-950 px-4 py-3 text-left transition hover:border-slate-600"
              }
            >
              <p className="text-sm font-medium text-white">{metric.label}</p>
              <p className="mt-1 text-xs text-slate-400">
                {metric.description}
              </p>
            </button>
          );
        })}
      </div>
    </div>
  );
}
