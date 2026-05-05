type MapLegendProps = {
  metricLabel: string;
};

const legendItems = [
  { label: "High / favorable", color: "#16a34a" },
  { label: "Moderate", color: "#65a30d" },
  { label: "Watch", color: "#d97706" },
  { label: "Weak / unfavorable", color: "#dc2626" },
  { label: "Unknown", color: "#334155" },
];

export function MapLegend({ metricLabel }: MapLegendProps) {
  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-4">
      <p className="text-sm font-semibold text-white">{metricLabel}</p>
      <p className="mt-1 text-xs text-slate-400">
        Color is directional and metric-specific. Unknown data is neutral.
      </p>

      <div className="mt-4 space-y-2">
        {legendItems.map((item) => (
          <div key={item.label} className="flex items-center gap-2 text-sm">
            <span
              className="h-3 w-3 rounded-full"
              style={{ backgroundColor: item.color }}
            />
            <span className="text-slate-300">{item.label}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
