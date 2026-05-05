type MetricCardProps = {
  label: string;
  value: string | number | null | undefined;
  helperText?: string;
  tone?: "default" | "good" | "warning" | "bad" | "neutral";
};

const toneClasses = {
  default: "border-slate-800 bg-slate-900",
  good: "border-emerald-900/70 bg-emerald-950/30",
  warning: "border-amber-900/70 bg-amber-950/30",
  bad: "border-red-900/70 bg-red-950/30",
  neutral: "border-slate-800 bg-slate-900/60",
};

export function MetricCard({
  label,
  value,
  helperText,
  tone = "default",
}: MetricCardProps) {
  const displayValue =
    value === null || value === undefined || value === "" ? "—" : value;

  return (
    <div className={`rounded-2xl border p-5 ${toneClasses[tone]}`}>
      <p className="text-sm font-medium text-slate-400">{label}</p>
      <p className="mt-3 text-2xl font-semibold tracking-tight text-white">
        {displayValue}
      </p>
      {helperText ? (
        <p className="mt-2 text-sm leading-6 text-slate-400">{helperText}</p>
      ) : null}
    </div>
  );
}
