import type { SourceFreshnessItem } from "@/lib/api";
import { formatDate, formatDateTime, formatNumber } from "@/lib/format";

type SourceFreshnessTableProps = {
  items: SourceFreshnessItem[];
};

function statusBadgeClass(item: SourceFreshnessItem): string {
  const status = item.last_status ?? "unknown";

  if (status !== "success") {
    return "border-red-800 bg-red-950 text-red-200";
  }

  if (item.is_stale === true) {
    return "border-amber-800 bg-amber-950 text-amber-200";
  }

  if (item.is_stale === false) {
    return "border-emerald-800 bg-emerald-950 text-emerald-200";
  }

  return "border-slate-700 bg-slate-950 text-slate-300";
}

function staleLabel(item: SourceFreshnessItem): string {
  if (item.is_stale === true) return "stale";
  if (item.is_stale === false) return "fresh";
  return "unknown";
}

export function SourceFreshnessTable({ items }: SourceFreshnessTableProps) {
  return (
    <div className="overflow-hidden rounded-2xl border border-slate-800 bg-slate-900">
      <div className="border-b border-slate-800 p-5">
        <p className="text-base font-semibold text-white">Source records</p>
        <p className="mt-1 text-sm text-slate-400">
          Ingestion freshness and status by source/dataset.
        </p>
      </div>

      <div className="overflow-x-auto">
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
                Last loaded
              </th>
              <th className="px-4 py-3 text-right text-xs font-medium uppercase tracking-wider text-slate-500">
                Records
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Status
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Staleness
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
                <td className="px-4 py-3 text-right text-sm text-slate-300">
                  {formatNumber(item.record_count, {
                    maximumFractionDigits: 0,
                  })}
                </td>
                <td className="px-4 py-3 text-sm">
                  <span
                    className={`rounded-full border px-2 py-1 text-xs ${statusBadgeClass(
                      item,
                    )}`}
                  >
                    {item.last_status ?? "unknown"}
                  </span>
                </td>
                <td className="px-4 py-3 text-sm">
                  <div>
                    <span
                      className={`rounded-full border px-2 py-1 text-xs ${statusBadgeClass(
                        item,
                      )}`}
                    >
                      {staleLabel(item)}
                    </span>
                    {item.stale_reason ? (
                      <p className="mt-2 max-w-md text-xs leading-5 text-slate-500">
                        {item.stale_reason}
                      </p>
                    ) : null}
                    {item.error_message ? (
                      <p className="mt-2 max-w-md text-xs leading-5 text-red-300">
                        {item.error_message}
                      </p>
                    ) : null}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
