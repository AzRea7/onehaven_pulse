import { CycleBadge } from "@/components/ui/CycleBadge";
import { InvestorSignalBadge } from "@/components/ui/InvestorSignalBadge";
import type { MarketCompareResponse } from "@/lib/api";
import { getCompareMetricDefinition } from "@/lib/compareMetrics";
import { formatMapValue } from "@/lib/mapStyle";

type CompareLatestTableProps = {
  comparison: MarketCompareResponse;
};

function marketName(comparison: MarketCompareResponse, geoId: string): string {
  const market = comparison.markets.find((item) => item.geo_id === geoId);
  return market?.display_name || market?.name || geoId;
}

export function CompareLatestTable({ comparison }: CompareLatestTableProps) {
  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-900">
      <div className="border-b border-slate-800 p-5">
        <p className="text-base font-semibold text-white">Latest comparison</p>
        <p className="mt-1 text-sm text-slate-400">
          Latest scoreable metrics by selected market.
        </p>
      </div>

      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-slate-800">
          <thead className="bg-slate-950">
            <tr>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Market
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Period
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Cycle
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Signal
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Confidence
              </th>
              {comparison.metrics.map((metric) => (
                <th
                  key={metric}
                  className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500"
                >
                  {getCompareMetricDefinition(metric).label}
                </th>
              ))}
            </tr>
          </thead>

          <tbody className="divide-y divide-slate-800">
            {comparison.latest.map((item) => (
              <tr key={item.geo_id}>
                <td className="px-4 py-3 text-sm font-medium text-white">
                  {marketName(comparison, item.geo_id)}
                  <p className="mt-1 text-xs font-normal text-slate-500">
                    {item.geo_id}
                  </p>
                </td>
                <td className="px-4 py-3 text-sm text-slate-300">
                  {item.latest_period ?? item.latest_data_period ?? "—"}
                </td>
                <td className="px-4 py-3 text-sm">
                  <CycleBadge phase={item.cycle_phase} />
                </td>
                <td className="px-4 py-3 text-sm">
                  <InvestorSignalBadge signal={item.investor_signal} />
                </td>
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {item.confidence_score === null
                    ? "—"
                    : `${Math.round(item.confidence_score * 100)}%`}
                </td>

                {comparison.metrics.map((metric) => {
                  const definition = getCompareMetricDefinition(metric);
                  return (
                    <td
                      key={`${item.geo_id}:${metric}`}
                      className="px-4 py-3 text-right text-sm text-slate-300"
                    >
                      {formatMapValue(item.values[metric], definition.valueSuffix)}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
