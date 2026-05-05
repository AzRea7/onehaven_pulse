import type { SourceFreshnessItem } from "@/lib/api";
import { formatDate, formatDateTime } from "@/lib/format";

type SourceFreshnessSummaryProps = {
  items: SourceFreshnessItem[];
};

function statusClass(item: SourceFreshnessItem): string {
  if ((item.last_status ?? "unknown") !== "success") {
    return "border-red-800 bg-red-950 text-red-200";
  }

  if (item.is_stale) {
    return "border-amber-800 bg-amber-950 text-amber-200";
  }

  return "border-emerald-800 bg-emerald-950 text-emerald-200";
}

export function SourceFreshnessSummary({ items }: SourceFreshnessSummaryProps) {
  const sorted = [...items].sort((a, b) => {
    const left = `${a.source}:${a.dataset}`;
    const right = `${b.source}:${b.dataset}`;
    return left.localeCompare(right);
  });

  const staleCount = items.filter((item) => item.is_stale).length;
  const failedCount = items.filter((item) => item.last_status !== "success").length;

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
        <div>
          <p className="text-base font-semibold text-white">Source freshness</p>
          <p className="mt-1 text-sm text-slate-400">
            Latest source periods and ingestion health.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <span className="rounded-full border border-slate-700 bg-slate-950 px-3 py-1 text-xs text-slate-300">
            {items.length} sources
          </span>
          <span className="rounded-full border border-amber-800 bg-amber-950 px-3 py-1 text-xs text-amber-200">
            {staleCount} stale
          </span>
          <span className="rounded-full border border-red-800 bg-red-950 px-3 py-1 text-xs text-red-200">
            {failedCount} failed
          </span>
        </div>
      </div>

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
                Last loaded
              </th>
              <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                Status
              </th>
            </tr>
          </thead>

          <tbody className="divide-y divide-slate-800">
            {sorted.slice(0, 8).map((item) => (
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
                <td className="px-4 py-3 text-sm">
                  <span
                    className={`rounded-full border px-2 py-1 text-xs ${statusClass(item)}`}
                  >
                    {item.last_status ?? "unknown"}
                    {item.is_stale ? " / stale" : ""}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {sorted.length > 8 ? (
        <p className="mt-3 text-sm text-slate-500">
          Showing 8 of {sorted.length} source records. Full table is available
          on the source freshness admin page.
        </p>
      ) : null}
    </div>
  );
}
