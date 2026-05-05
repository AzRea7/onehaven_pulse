type InvestorSignalBadgeProps = {
  signal: string | null | undefined;
};

function classForSignal(signal: string): string {
  const normalized = signal.toLowerCase();

  if (normalized.includes("buy")) {
    return "border-emerald-800 bg-emerald-950 text-emerald-200";
  }

  if (normalized.includes("hold") || normalized.includes("watch")) {
    return "border-amber-800 bg-amber-950 text-amber-200";
  }

  if (normalized.includes("sell") || normalized.includes("avoid")) {
    return "border-red-800 bg-red-950 text-red-200";
  }

  return "border-slate-700 bg-slate-900 text-slate-300";
}

export function InvestorSignalBadge({ signal }: InvestorSignalBadgeProps) {
  const label = signal || "Unknown";

  return (
    <span
      className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium ${classForSignal(label)}`}
    >
      {label}
    </span>
  );
}
