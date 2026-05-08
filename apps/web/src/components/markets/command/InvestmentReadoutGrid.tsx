import type { InvestorMarketSignal } from "@/lib/api";
import { dimensionStatusTone, formatInvestorDimensionName } from "@/lib/investorSignal";
import { titleCase } from "@/lib/uiFormat";

type InvestmentReadoutGridProps = {
  signal: InvestorMarketSignal;
};

export function InvestmentReadoutGrid({ signal }: InvestmentReadoutGridProps) {
  return (
    <section className="grid gap-5 xl:grid-cols-3">
      <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 className="text-lg font-semibold text-white">Decision dimensions</h2>
        <div className="mt-4 grid gap-3">
          {Object.entries(signal.dimension_statuses).map(([name, status]) => (
            <div
              key={name}
              className="flex items-center justify-between rounded-xl bg-slate-950 px-4 py-3"
            >
              <span className="text-sm text-slate-300">
                {formatInvestorDimensionName(name)}
              </span>
              <span
                className={`rounded-full px-2.5 py-1 text-xs font-semibold ${dimensionStatusTone(
                  status,
                )}`}
              >
                {titleCase(status)}
              </span>
            </div>
          ))}
        </div>
      </div>

      <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 className="text-lg font-semibold text-white">Why this market is interesting</h2>
        <div className="mt-4 space-y-3">
          {signal.drivers.length === 0 ? (
            <p className="text-sm text-slate-400">No positive drivers found.</p>
          ) : (
            signal.drivers.slice(0, 5).map((driver) => (
              <div key={driver.name} className="rounded-xl bg-slate-950 p-4">
                <div className="text-sm font-semibold text-white">
                  {formatInvestorDimensionName(driver.name)}
                </div>
                <p className="mt-1 text-sm leading-6 text-slate-400">
                  {driver.message}
                </p>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="rounded-2xl border border-slate-800 bg-slate-900/70 p-5">
        <h2 className="text-lg font-semibold text-white">What can break the thesis</h2>
        <div className="mt-4 space-y-3">
          {signal.risks.length === 0 ? (
            <p className="text-sm text-slate-400">No major risks found.</p>
          ) : (
            signal.risks.slice(0, 5).map((risk) => (
              <div key={risk.name} className="rounded-xl bg-slate-950 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-white">
                    {formatInvestorDimensionName(risk.name)}
                  </div>
                  <span className="rounded-full bg-amber-100 px-2 py-1 text-xs font-semibold text-amber-800">
                    {risk.severity}
                  </span>
                </div>
                <p className="mt-1 text-sm leading-6 text-slate-400">
                  {risk.message}
                </p>
              </div>
            ))
          )}
        </div>
      </div>
    </section>
  );
}
