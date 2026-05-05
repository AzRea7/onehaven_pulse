type CycleBadgeProps = {
  phase: string | null | undefined;
};

function classForPhase(phase: string): string {
  const normalized = phase.toLowerCase();

  if (normalized.includes("expansion")) {
    return "border-emerald-800 bg-emerald-950 text-emerald-200";
  }

  if (normalized.includes("recovery")) {
    return "border-sky-800 bg-sky-950 text-sky-200";
  }

  if (normalized.includes("stabilizing")) {
    return "border-amber-800 bg-amber-950 text-amber-200";
  }

  if (normalized.includes("contraction") || normalized.includes("decline")) {
    return "border-red-800 bg-red-950 text-red-200";
  }

  return "border-slate-700 bg-slate-900 text-slate-300";
}

export function CycleBadge({ phase }: CycleBadgeProps) {
  const label = phase || "Unknown";

  return (
    <span
      className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-medium ${classForPhase(label)}`}
    >
      {label}
    </span>
  );
}
