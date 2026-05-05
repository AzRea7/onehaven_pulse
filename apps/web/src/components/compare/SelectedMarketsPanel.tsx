import type { MarketIdentity } from "@/lib/api";

type SelectedMarketsPanelProps = {
  markets: MarketIdentity[];
  onRemove: (geoId: string) => void;
};

export function SelectedMarketsPanel({
  markets,
  onRemove,
}: SelectedMarketsPanelProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div className="flex items-center justify-between gap-4">
        <div>
          <p className="text-base font-semibold text-white">Selected markets</p>
          <p className="mt-1 text-sm text-slate-400">
            Select 2–5 markets to compare.
          </p>
        </div>

        <span className="rounded-full border border-slate-700 bg-slate-950 px-3 py-1 text-xs text-slate-300">
          {markets.length}/5
        </span>
      </div>

      <div className="mt-4 space-y-2">
        {markets.length === 0 ? (
          <p className="rounded-xl border border-dashed border-slate-700 bg-slate-950 p-4 text-sm text-slate-500">
            No markets selected.
          </p>
        ) : null}

        {markets.map((market) => (
          <div
            key={market.geo_id}
            className="flex items-center justify-between gap-4 rounded-xl border border-slate-800 bg-slate-950 px-4 py-3"
          >
            <div>
              <p className="text-sm font-medium text-white">
                {market.display_name || market.name}
              </p>
              <p className="mt-1 text-xs text-slate-500">{market.geo_id}</p>
            </div>

            <button
              type="button"
              onClick={() => onRemove(market.geo_id)}
              className="rounded-lg border border-slate-700 px-2 py-1 text-xs text-slate-300 transition hover:border-slate-500 hover:text-white"
            >
              Remove
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
