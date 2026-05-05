type CoverageBadgeProps = {
  label: string;
  available: boolean;
};

export function CoverageBadge({ label, available }: CoverageBadgeProps) {
  return (
    <span
      className={
        available
          ? "inline-flex items-center rounded-full border border-emerald-800 bg-emerald-950 px-3 py-1 text-xs font-medium text-emerald-200"
          : "inline-flex items-center rounded-full border border-slate-700 bg-slate-900 px-3 py-1 text-xs font-medium text-slate-400"
      }
    >
      {label}: {available ? "available" : "missing"}
    </span>
  );
}
