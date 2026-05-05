import { CoverageBadge } from "@/components/ui/CoverageBadge";
import { CycleBadge } from "@/components/ui/CycleBadge";
import { InvestorSignalBadge } from "@/components/ui/InvestorSignalBadge";
import type {
  MarketContextResponse,
  MarketCoverageResponse,
  MarketDetailResponse,
} from "@/lib/api";
import { formatDate, formatPercent } from "@/lib/format";

type MarketHeaderProps = {
  detail: MarketDetailResponse;
  context: MarketContextResponse;
  coverage: MarketCoverageResponse;
};

export function MarketHeader({ detail, context, coverage }: MarketHeaderProps) {
  const marketName =
    detail.market.display_name || detail.market.name || detail.market.geo_id;

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-6">
      <div className="flex flex-col gap-5 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <p className="text-sm font-medium uppercase tracking-[0.2em] text-slate-500">
            Market detail
          </p>
          <h1 className="mt-3 text-3xl font-bold tracking-tight text-white md:text-4xl">
            {marketName}
          </h1>

          <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-400">
            <span>{detail.market.geo_id}</span>
            <span>{detail.market.geo_type}</span>
            {detail.market.cbsa_code ? <span>CBSA {detail.market.cbsa_code}</span> : null}
            {detail.market.state_code ? <span>{detail.market.state_code}</span> : null}
          </div>

          <p className="mt-4 max-w-3xl text-sm leading-6 text-slate-400">
            Latest scoreable period: {formatDate(coverage.latest_scoreable_period)}.
            Latest data period: {formatDate(coverage.latest_data_period)}.
            Data status: {coverage.data_status}.
          </p>
        </div>

        <div className="flex flex-wrap gap-2">
          <CycleBadge phase={detail.cycle_phase} />
          <InvestorSignalBadge signal={detail.investor_signal} />
        </div>
      </div>

      <div className="mt-6 grid gap-4 md:grid-cols-3">
        <div className="rounded-xl border border-slate-800 bg-slate-950 p-4">
          <p className="text-sm text-slate-400">Confidence score</p>
          <p className="mt-2 text-2xl font-semibold text-white">
            {formatPercent((detail.confidence_score ?? 0) * 100, { digits: 0 })}
          </p>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-950 p-4">
          <p className="text-sm text-slate-400">Composite score</p>
          <p className="mt-2 text-2xl font-semibold text-white">
            {context.evidence.composite_cycle_score ?? "—"}
          </p>
        </div>

        <div className="rounded-xl border border-slate-800 bg-slate-950 p-4">
          <p className="text-sm text-slate-400">AI/MCP status</p>
          <p className="mt-2 text-2xl font-semibold text-white">
            {context.mcp?.schema_version ? `v${context.mcp.schema_version}` : "ready"}
          </p>
        </div>
      </div>

      <div className="mt-6 flex flex-wrap gap-2">
        <CoverageBadge label="price" available={coverage.coverage.price} />
        <CoverageBadge label="rent" available={coverage.coverage.rent} />
        <CoverageBadge label="inventory" available={coverage.coverage.inventory} />
        <CoverageBadge label="affordability" available={coverage.coverage.affordability} />
        <CoverageBadge label="labor" available={coverage.coverage.labor} />
        <CoverageBadge label="permits" available={coverage.coverage.permits} />
      </div>
    </div>
  );
}
