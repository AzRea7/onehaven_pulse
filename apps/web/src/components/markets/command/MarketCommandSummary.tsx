import Link from "next/link";

import type { InvestorMarketSignal, MarketCoverage, MarketContext, MarketDetail } from "@/lib/api";
import { formatDate, formatScore, titleCase } from "@/lib/uiFormat";

type MarketCommandSummaryProps = {
  detail: MarketDetail;
  context: MarketContext;
  coverage: MarketCoverage;
  signal: InvestorMarketSignal;
};

function CoveragePill({ label, value }: { label: string; value: boolean }) {
  return (
    <span
      className={[
        "rounded-full px-3 py-1 text-xs font-semibold",
        value
          ? "bg-emerald-100 text-emerald-800"
          : "bg-amber-100 text-amber-800",
      ].join(" ")}
    >
      {label}: {value ? "yes" : "missing"}
    </span>
  );
}

export function MarketCommandSummary({
  detail,
  coverage,
  signal,
}: MarketCommandSummaryProps) {
  const marketName =
    detail.market?.display_name ??
    detail.market?.name ??
    signal.geo_id;

  return (
    <section className="rounded-3xl border border-slate-800 bg-gradient-to-br from-slate-900 to-slate-950 p-6 shadow-sm">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.24em] text-cyan-300">
            Market Command Center
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-5xl">
            {marketName}
          </h1>
          <p className="mt-3 max-w-3xl text-slate-400">
            Fast investor readout for market selection, risk triage, and
            research prioritization.
          </p>

          <div className="mt-5 flex flex-wrap gap-2">
            {Object.entries(signal.coverage).map(([key, value]) => (
              <CoveragePill key={key} label={titleCase(key)} value={value} />
            ))}
          </div>
        </div>

        <div className="rounded-2xl border border-slate-800 bg-slate-950 p-5 lg:min-w-80">
          <div className="text-sm text-slate-400">Investor stance</div>
          <div className="mt-2 text-3xl font-bold text-white">
            {signal.stance_label}
          </div>
          <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
            <div className="rounded-xl bg-slate-900 p-3">
              <div className="text-slate-500">Score</div>
              <div className="mt-1 text-xl font-semibold text-white">
                {formatScore(signal.stance_score)}
              </div>
            </div>
            <div className="rounded-xl bg-slate-900 p-3">
              <div className="text-slate-500">Confidence</div>
              <div className="mt-1 text-xl font-semibold text-white">
                {signal.confidence_score === null
                  ? "—"
                  : signal.confidence_score.toFixed(2)}
              </div>
            </div>
          </div>
          <div className="mt-3 text-sm text-slate-400">
            Latest scoreable period:{" "}
            <span className="font-semibold text-slate-200">
              {formatDate(signal.latest_scoreable_period)}
            </span>
          </div>
        </div>
      </div>

      <div className="mt-6 rounded-2xl border border-slate-800 bg-slate-950/70 p-5">
        <h2 className="text-lg font-semibold text-white">Bottom line</h2>
        <p className="mt-2 text-sm leading-6 text-slate-300">
          {signal.stance_reason}
        </p>

        <div className="mt-5 flex flex-wrap gap-3">
          <Link
            href="/screener"
            className="rounded-xl bg-cyan-300 px-4 py-2 text-sm font-semibold text-slate-950 hover:bg-cyan-200"
          >
            Back to screener
          </Link>
          <Link
            href="/compare"
            className="rounded-xl border border-slate-700 px-4 py-2 text-sm font-semibold text-slate-200 hover:bg-slate-800"
          >
            Compare markets
          </Link>
          <Link
            href="/admin/source-freshness"
            className="rounded-xl border border-slate-700 px-4 py-2 text-sm font-semibold text-slate-200 hover:bg-slate-800"
          >
            Check data health
          </Link>
        </div>
      </div>
    </section>
  );
}
