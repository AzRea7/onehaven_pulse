import type { MarketDetailResponse } from "@/lib/api";
import { formatNumber, formatPercent, titleCase } from "@/lib/format";

type ScoreBreakdownPanelProps = {
  detail: MarketDetailResponse;
};

const SCORE_KEYS = [
  "composite_cycle_score",
  "price_momentum",
  "rent_momentum",
  "inventory_tightness",
  "affordability",
  "labor_market",
  "data_completeness",
] as const;

export function ScoreBreakdownPanel({ detail }: ScoreBreakdownPanelProps) {
  const breakdown = detail.score_breakdown;

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div>
        <p className="text-base font-semibold text-white">Score breakdown</p>
        <p className="mt-1 text-sm text-slate-400">
          Component-level explanation of the market-cycle score.
        </p>
      </div>

      <div className="mt-5 space-y-3">
        {SCORE_KEYS.map((key) => {
          const value = breakdown?.[key];

          const display =
            key === "data_completeness"
              ? formatPercent(value, { digits: 0 })
              : formatNumber(value, { maximumFractionDigits: 2 });

          return (
            <div
              key={key}
              className="flex items-center justify-between gap-4 rounded-xl border border-slate-800 bg-slate-950 px-4 py-3"
            >
              <span className="text-sm text-slate-300">{titleCase(key)}</span>
              <span className="text-sm font-semibold text-white">{display}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}
