import Link from "next/link";

import { CycleBadge } from "@/components/ui/CycleBadge";
import { InvestorSignalBadge } from "@/components/ui/InvestorSignalBadge";
import type { ScreenerMarketResult } from "@/lib/api";
import { formatNumber, formatPercent } from "@/lib/format";

type ScreenerResultsTableProps = {
  items: ScreenerMarketResult[];
};

export function ScreenerResultsTable({ items }: ScreenerResultsTableProps) {
  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-900">
      <div className="border-b border-slate-800 p-5">
        <p className="text-base font-semibold text-white">Results</p>
        <p className="mt-1 text-sm text-slate-400">
          Markets matching the selected filters.
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
                Cycle
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Signal
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Confidence
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Price YoY
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Rent YoY
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Payment / Income
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Composite
              </th>
            </tr>
          </thead>

          <tbody className="divide-y divide-slate-800">
            {items.map((item) => (
              <tr key={item.market.geo_id}>
                <td className="px-4 py-3 text-sm">
                  <Link
                    href={`/markets/${item.market.geo_id}`}
                    className="font-medium text-white hover:underline"
                  >
                    {item.market.display_name || item.market.name}
                  </Link>
                  <p className="mt-1 text-xs text-slate-500">
                    {item.market.geo_id}
                  </p>
                </td>
                <td className="px-4 py-3 text-sm">
                  <CycleBadge phase={item.cycle_phase} />
                </td>
                <td className="px-4 py-3 text-sm">
                  <InvestorSignalBadge signal={item.investor_signal} />
                </td>
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {item.confidence_score === null ||
                  item.confidence_score === undefined
                    ? "—"
                    : formatPercent(item.confidence_score * 100, { digits: 0 })}
                </td>
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {formatPercent(item.values.home_price_yoy)}
                </td>
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {formatPercent(item.values.rent_yoy)}
                </td>
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {formatPercent(item.values.payment_to_income_ratio)}
                </td>
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {formatNumber(item.values.composite_cycle_score, {
                    maximumFractionDigits: 0,
                  })}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
