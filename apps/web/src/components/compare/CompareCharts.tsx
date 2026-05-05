"use client";

import { MultiLineChartCard } from "@/components/ui/MultiLineChartCard";
import type { MarketCompareResponse } from "@/lib/api";
import { getCompareMetricDefinition } from "@/lib/compareMetrics";
import { formatDate } from "@/lib/format";

type CompareChartsProps = {
  comparison: MarketCompareResponse;
};

function marketLabel(comparison: MarketCompareResponse, geoId: string): string {
  const market = comparison.markets.find((item) => item.geo_id === geoId);
  return market?.display_name || market?.name || geoId;
}

export function CompareCharts({ comparison }: CompareChartsProps) {
  const rows = comparison.timeseries.map((item) => {
    const row: Record<string, string | number | null> = {
      period: formatDate(item.period_month),
      period_month: item.period_month,
    };

    for (const geoId of Object.keys(item.markets)) {
      for (const metric of comparison.metrics) {
        row[`${geoId}:${metric}`] = item.markets[geoId]?.[metric] ?? null;
      }
    }

    return row;
  });

  return (
    <div className="grid gap-5 xl:grid-cols-2">
      {comparison.metrics.map((metric) => {
        const definition = getCompareMetricDefinition(metric);

        return (
          <MultiLineChartCard
            key={metric}
            title={definition.label}
            description={definition.description}
            data={rows}
            xKey="period"
            valueSuffix={definition.valueSuffix}
            series={comparison.markets.map((market) => ({
              key: `${market.geo_id}:${metric}`,
              label: marketLabel(comparison, market.geo_id),
            }))}
          />
        );
      })}
    </div>
  );
}
