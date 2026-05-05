"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

type MultiLineChartCardProps = {
  title: string;
  description?: string;
  data: Array<Record<string, string | number | null>>;
  xKey: string;
  series: Array<{
    key: string;
    label: string;
  }>;
  valueSuffix?: string;
  emptyMessage?: string;
};

const STROKES = [
  "#f8fafc",
  "#38bdf8",
  "#a78bfa",
  "#34d399",
  "#fbbf24",
];

export function MultiLineChartCard({
  title,
  description,
  data,
  xKey,
  series,
  valueSuffix = "",
  emptyMessage = "No chartable data is available.",
}: MultiLineChartCardProps) {
  const hasChartableData = data.some((row) =>
    series.some((item) => typeof row[item.key] === "number"),
  );

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div className="mb-4">
        <p className="text-base font-semibold text-white">{title}</p>
        {description ? (
          <p className="mt-1 text-sm leading-6 text-slate-400">{description}</p>
        ) : null}
      </div>

      {!hasChartableData ? (
        <div className="flex h-72 items-center justify-center rounded-xl border border-dashed border-slate-700 bg-slate-950/60 p-6 text-center text-sm text-slate-400">
          {emptyMessage}
        </div>
      ) : (
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
              <XAxis
                dataKey={xKey}
                tick={{ fill: "#94a3b8", fontSize: 12 }}
                tickLine={false}
                axisLine={{ stroke: "#334155" }}
                minTickGap={24}
              />
              <YAxis
                tick={{ fill: "#94a3b8", fontSize: 12 }}
                tickLine={false}
                axisLine={{ stroke: "#334155" }}
                width={48}
              />
              <Tooltip
                contentStyle={{
                  background: "#020617",
                  border: "1px solid #334155",
                  borderRadius: "12px",
                  color: "#f8fafc",
                }}
                labelStyle={{ color: "#cbd5e1" }}
                formatter={(value, name) => [
                  typeof value === "number"
                    ? `${value.toFixed(2)}${valueSuffix}`
                    : value,
                  name,
                ]}
              />

              {series.map((item, index) => (
                <Line
                  key={item.key}
                  type="monotone"
                  dataKey={item.key}
                  name={item.label}
                  stroke={STROKES[index % STROKES.length]}
                  strokeWidth={2}
                  dot={false}
                  connectNulls={false}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      <div className="mt-4 flex flex-wrap gap-3">
        {series.map((item, index) => (
          <div key={item.key} className="flex items-center gap-2 text-xs text-slate-300">
            <span
              className="h-2.5 w-2.5 rounded-full"
              style={{ backgroundColor: STROKES[index % STROKES.length] }}
            />
            {item.label}
          </div>
        ))}
      </div>
    </div>
  );
}
