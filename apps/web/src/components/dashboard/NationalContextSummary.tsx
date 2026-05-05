import { CoverageBadge } from "@/components/ui/CoverageBadge";
import { CycleBadge } from "@/components/ui/CycleBadge";
import { InvestorSignalBadge } from "@/components/ui/InvestorSignalBadge";
import type { MarketContextResponse } from "@/lib/api";
import { formatDate, formatNumber, formatPercent } from "@/lib/format";

type NationalContextSummaryProps = {
  context: MarketContextResponse;
};

export function NationalContextSummary({ context }: NationalContextSummaryProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            National context
          </p>
          <h2 className="mt-2 text-2xl font-semibold text-white">
            {context.market}
          </h2>
          <p className="mt-2 text-sm text-slate-400">
            Latest score period: {formatDate(context.latest_period)}
          </p>
          {context.latest_data_period ? (
            <p className="mt-1 text-sm text-slate-500">
              Latest data period: {formatDate(context.latest_data_period)}
            </p>
          ) : null}
        </div>

        <div className="flex flex-wrap gap-2">
          <CycleBadge phase={context.cycle_phase} />
          <InvestorSignalBadge signal={context.investor_signal} />
        </div>
      </div>

      <div className="mt-6 grid gap-4 md:grid-cols-4">
        <div>
          <p className="text-sm text-slate-400">Composite score</p>
          <p className="mt-1 text-2xl font-semibold text-white">
            {formatNumber(context.evidence.composite_cycle_score, {
              maximumFractionDigits: 0,
            })}
          </p>
        </div>

        <div>
          <p className="text-sm text-slate-400">Price growth YoY</p>
          <p className="mt-1 text-2xl font-semibold text-white">
            {formatPercent(context.evidence.price_growth_yoy)}
          </p>
        </div>

        <div>
          <p className="text-sm text-slate-400">Rent growth YoY</p>
          <p className="mt-1 text-2xl font-semibold text-white">
            {formatPercent(context.evidence.rent_growth_yoy)}
          </p>
        </div>

        <div>
          <p className="text-sm text-slate-400">Unemployment</p>
          <p className="mt-1 text-2xl font-semibold text-white">
            {formatPercent(context.evidence.unemployment_rate)}
          </p>
        </div>
      </div>

      {context.coverage ? (
        <div className="mt-6 flex flex-wrap gap-2">
          <CoverageBadge label="price" available={context.coverage.price} />
          <CoverageBadge label="rent" available={context.coverage.rent} />
          <CoverageBadge
            label="inventory"
            available={context.coverage.inventory}
          />
          <CoverageBadge
            label="affordability"
            available={context.coverage.affordability}
          />
          <CoverageBadge label="labor" available={context.coverage.labor} />
          <CoverageBadge label="permits" available={context.coverage.permits} />
        </div>
      ) : null}

      {context.risks.length > 0 ? (
        <div className="mt-6 rounded-xl border border-amber-900/70 bg-amber-950/30 p-4">
          <p className="text-sm font-semibold text-amber-100">
            Data and interpretation risks
          </p>
          <ul className="mt-2 space-y-2 text-sm text-amber-200">
            {context.risks.map((risk) => (
              <li key={risk.code}>
                <span className="font-medium">{risk.severity}:</span>{" "}
                {risk.message}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}
