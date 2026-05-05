import type { MarketContextResponse } from "@/lib/api";

type MarketRisksPanelProps = {
  context: MarketContextResponse;
};

function severityClass(severity: string): string {
  const normalized = severity.toLowerCase();

  if (normalized === "high") {
    return "border-red-800 bg-red-950 text-red-200";
  }

  if (normalized === "medium") {
    return "border-amber-800 bg-amber-950 text-amber-200";
  }

  return "border-slate-700 bg-slate-950 text-slate-300";
}

export function MarketRisksPanel({ context }: MarketRisksPanelProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <p className="text-base font-semibold text-white">Risks and missing data</p>
      <p className="mt-1 text-sm text-slate-400">
        Data gaps and context warnings returned by the API.
      </p>

      {context.risks.length === 0 ? (
        <div className="mt-5 rounded-xl border border-emerald-900/70 bg-emerald-950/30 p-4">
          <p className="text-sm font-medium text-emerald-200">
            No context risks returned.
          </p>
        </div>
      ) : (
        <div className="mt-5 space-y-3">
          {context.risks.map((risk) => (
            <div
              key={risk.code}
              className={`rounded-xl border p-4 ${severityClass(risk.severity)}`}
            >
              <div className="flex flex-wrap items-center justify-between gap-3">
                <p className="text-sm font-semibold">{risk.code}</p>
                <span className="rounded-full border border-current px-2 py-0.5 text-xs">
                  {risk.severity}
                </span>
              </div>
              <p className="mt-2 text-sm leading-6">{risk.message}</p>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
