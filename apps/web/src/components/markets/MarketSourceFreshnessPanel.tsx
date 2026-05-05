import type { SourceFreshnessItem } from "@/lib/api";
import { formatDate, formatDateTime } from "@/lib/format";

type MarketSourceFreshnessPanelProps = {
  items: SourceFreshnessItem[];
};

export function MarketSourceFreshnessPanel({
  items,
}: MarketSourceFreshnessPanelProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <p className="text-base font-semibold text-white">Source freshness</p>
      <p className="mt-1 text-sm text-slate-400">
        Source recency used by this market response.
      </p>

      {items.length === 0 ? (
        <div className="mt-5 rounded-xl border border-slate-800 bg-slate-950 p-4">
          <p className="text-sm text-slate-400">
            No source freshness records were returned for this market.
          </p>
        </div>
      ) : (
        <div className="mt-5 overflow-hidden rounded-xl border border-slate-800">
          <table className="min-w-full divide-y divide-slate-800">
            <thead className="bg-slate-950">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                  Source
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                  Dataset
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                  Latest period
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                  Loaded
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                  Status
                </th>
              </tr>
            </thead>

            <tbody className="divide-y divide-slate-800">
              {items.map((item) => (
                <tr key={`${item.source}:${item.dataset}`}>
                  <td className="px-4 py-3 text-sm font-medium text-white">
                    {item.source}
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-300">
                    {item.dataset}
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-300">
                    {formatDate(item.latest_source_period)}
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-300">
                    {formatDateTime(item.last_loaded_at)}
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-300">
                    {item.last_status ?? "unknown"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
