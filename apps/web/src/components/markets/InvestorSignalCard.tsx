import type { InvestorMarketSignal } from "@/lib/api";
import {
  dimensionStatusTone,
  formatInvestorDimensionName,
  formatSignalValue,
  investorStanceTone,
  severityTone,
} from "@/lib/investorSignal";

type InvestorSignalCardProps = {
  signal: InvestorMarketSignal;
};

function EvidenceLine({
  metricName,
  value,
  period,
}: {
  metricName: string;
  value: unknown;
  period: string | null;
}) {
  return (
    <div className="mt-1 text-xs text-slate-500">
      <span className="font-medium text-slate-600">{metricName}</span>
      {" = "}
      <span>{formatSignalValue(value)}</span>
      {period ? <span> · {period}</span> : null}
    </div>
  );
}

export function InvestorSignalCard({ signal }: InvestorSignalCardProps) {
  const stanceTone = investorStanceTone(signal.stance);

  const topDrivers = signal.drivers.slice(0, 4);
  const topRisks = signal.risks.slice(0, 4);
  const dimensionEntries = Object.entries(signal.dimension_statuses);

  return (
    <section className={`rounded-2xl border p-5 shadow-sm ${stanceTone}`}>
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="text-xs font-semibold uppercase tracking-wide opacity-75">
            Investor Signal
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-3">
            <h2 className="text-2xl font-semibold">{signal.stance_label}</h2>
            {signal.stance_score !== null ? (
              <span className="rounded-full bg-white/70 px-3 py-1 text-sm font-medium">
                Score {(signal.stance_score * 100).toFixed(0)}
              </span>
            ) : null}
            <span className="rounded-full bg-white/70 px-3 py-1 text-sm font-medium">
              {signal.rule_version}
            </span>
          </div>
          <p className="mt-3 max-w-3xl text-sm leading-6">
            {signal.stance_reason}
          </p>
        </div>

        <div className="min-w-48 rounded-xl bg-white/70 p-4 text-sm">
          <div className="flex justify-between gap-4">
            <span className="text-slate-600">Confidence</span>
            <span className="font-semibold">
              {signal.confidence_score === null
                ? "missing"
                : signal.confidence_score.toFixed(2)}
            </span>
          </div>
          <div className="mt-2 flex justify-between gap-4">
            <span className="text-slate-600">Latest period</span>
            <span className="font-semibold">
              {signal.latest_scoreable_period ?? signal.latest_data_period ?? "missing"}
            </span>
          </div>
          <div className="mt-2 flex justify-between gap-4">
            <span className="text-slate-600">Material gaps</span>
            <span className="font-semibold">
              {signal.material_missing_score_inputs ? "yes" : "no"}
            </span>
          </div>
        </div>
      </div>

      <div className="mt-5 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
        {dimensionEntries.map(([name, status]) => (
          <div key={name} className="rounded-xl bg-white/70 p-3">
            <div className="text-xs font-medium text-slate-500">
              {formatInvestorDimensionName(name)}
            </div>
            <div
              className={`mt-2 inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${dimensionStatusTone(
                status,
              )}`}
            >
              {status.replace("_", " ")}
            </div>
          </div>
        ))}
      </div>

      <div className="mt-5 grid gap-4 lg:grid-cols-2">
        <div className="rounded-xl bg-white/75 p-4">
          <h3 className="text-sm font-semibold text-slate-900">Top drivers</h3>
          {topDrivers.length === 0 ? (
            <p className="mt-2 text-sm text-slate-600">
              No positive drivers were identified from the current signal.
            </p>
          ) : (
            <div className="mt-3 space-y-3">
              {topDrivers.map((driver) => (
                <div key={driver.name} className="border-t border-slate-200 pt-3 first:border-t-0 first:pt-0">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-sm font-medium text-slate-900">
                      {formatInvestorDimensionName(driver.name)}
                    </div>
                    <span className={`rounded-full px-2 py-1 text-xs font-semibold ${dimensionStatusTone(driver.status)}`}>
                      {driver.status}
                    </span>
                  </div>
                  <p className="mt-1 text-sm text-slate-600">{driver.message}</p>
                  {driver.evidence.slice(0, 1).map((item) => (
                    <EvidenceLine
                      key={`${driver.name}-${item.metric_name}`}
                      metricName={item.metric_name}
                      value={item.value}
                      period={item.period}
                    />
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="rounded-xl bg-white/75 p-4">
          <h3 className="text-sm font-semibold text-slate-900">Top risks</h3>
          {topRisks.length === 0 ? (
            <p className="mt-2 text-sm text-slate-600">
              No major risks were identified from the current signal.
            </p>
          ) : (
            <div className="mt-3 space-y-3">
              {topRisks.map((risk) => (
                <div key={risk.name} className="border-t border-slate-200 pt-3 first:border-t-0 first:pt-0">
                  <div className="flex items-center justify-between gap-3">
                    <div className="text-sm font-medium text-slate-900">
                      {formatInvestorDimensionName(risk.name)}
                    </div>
                    <span className={`rounded-full px-2 py-1 text-xs font-semibold ${severityTone(risk.severity)}`}>
                      {risk.severity}
                    </span>
                  </div>
                  <p className="mt-1 text-sm text-slate-600">{risk.message}</p>
                  {risk.evidence.slice(0, 1).map((item) => (
                    <EvidenceLine
                      key={`${risk.name}-${item.metric_name}`}
                      metricName={item.metric_name}
                      value={item.value}
                      period={item.period}
                    />
                  ))}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {signal.missing_score_inputs.length > 0 ? (
        <div className="mt-5 rounded-xl bg-white/75 p-4">
          <h3 className="text-sm font-semibold text-slate-900">Missing score inputs</h3>
          <div className="mt-3 flex flex-wrap gap-2">
            {signal.missing_score_inputs.map((input) => (
              <span
                key={input}
                className="rounded-full bg-amber-100 px-3 py-1 text-xs font-medium text-amber-800"
              >
                {input}
              </span>
            ))}
          </div>
        </div>
      ) : null}

      {signal.rule_trace.length > 0 ? (
        <div className="mt-5 rounded-xl bg-white/75 p-4">
          <h3 className="text-sm font-semibold text-slate-900">Rule trace</h3>
          <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-slate-600">
            {signal.rule_trace.map((rule) => (
              <li key={rule}>{rule}</li>
            ))}
          </ul>
        </div>
      ) : null}
    </section>
  );
}
