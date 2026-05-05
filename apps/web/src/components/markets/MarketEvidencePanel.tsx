import type { MarketContextResponse } from "@/lib/api";
import { formatNumber, formatPercent } from "@/lib/format";

type MarketEvidencePanelProps = {
  context: MarketContextResponse;
};

export function MarketEvidencePanel({ context }: MarketEvidencePanelProps) {
  const evidence = context.evidence;

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <p className="text-base font-semibold text-white">Evidence</p>
      <p className="mt-1 text-sm text-slate-400">
        Structured facts used by the market context endpoint.
      </p>

      <div className="mt-5 grid gap-3 md:grid-cols-2">
        <EvidenceRow
          label="Price growth YoY"
          value={formatPercent(evidence.price_growth_yoy)}
          detail={evidence.price_growth_metric ?? undefined}
        />
        <EvidenceRow
          label="Rent growth YoY"
          value={formatPercent(evidence.rent_growth_yoy)}
          detail={evidence.rent_growth_metric ?? undefined}
        />
        <EvidenceRow
          label="Inventory trend"
          value={evidence.inventory_trend ?? "unknown"}
        />
        <EvidenceRow
          label="Active listings YoY"
          value={formatPercent(evidence.active_listings_yoy)}
        />
        <EvidenceRow
          label="Months supply"
          value={formatNumber(evidence.months_supply, {
            maximumFractionDigits: 2,
          })}
        />
        <EvidenceRow
          label="Days on market"
          value={formatNumber(evidence.median_days_on_market, {
            maximumFractionDigits: 0,
          })}
        />
        <EvidenceRow
          label="Affordability"
          value={evidence.affordability ?? "unknown"}
        />
        <EvidenceRow
          label="Payment-to-income"
          value={formatPercent(evidence.payment_to_income_ratio)}
        />
        <EvidenceRow
          label="Price-to-income"
          value={formatNumber(evidence.price_to_income_ratio, {
            maximumFractionDigits: 2,
          })}
        />
        <EvidenceRow
          label="Unemployment"
          value={formatPercent(evidence.unemployment_rate)}
        />
        <EvidenceRow
          label="Building permits"
          value={formatNumber(evidence.building_permits, {
            maximumFractionDigits: 0,
          })}
        />
        <EvidenceRow
          label="Composite score"
          value={formatNumber(evidence.composite_cycle_score, {
            maximumFractionDigits: 0,
          })}
        />
      </div>
    </div>
  );
}

function EvidenceRow({
  label,
  value,
  detail,
}: {
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950 p-4">
      <p className="text-sm text-slate-400">{label}</p>
      <p className="mt-1 text-lg font-semibold text-white">{value}</p>
      {detail ? <p className="mt-1 text-xs text-slate-500">{detail}</p> : null}
    </div>
  );
}
