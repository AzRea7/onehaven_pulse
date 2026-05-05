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

type LineChartCardProps = {
  title: string;
  description?: string;
  data: Array<Record<string, string | number | null>>;
  xKey: string;
  yKey: string;
  valueSuffix?: string;
  emptyMessage?: string;
};

export function LineChartCard({
  title,
  description,
  data,
  xKey,
  yKey,
  valueSuffix = "",
  emptyMessage = "No chartable data is available.",
}: LineChartCardProps) {
  const chartData = data.filter((item) => {
    const value = item[yKey];
    return typeof value === "number" && Number.isFinite(value);
  });

  return (
    <div className="rounded-2xl border border-slate-800 bg-slate-900 p-5">
      <div className="mb-4">
        <p className="text-base font-semibold text-white">{title}</p>
        {description ? (
          <p className="mt-1 text-sm leading-6 text-slate-400">{description}</p>
        ) : null}
      </div>

      {chartData.length === 0 ? (
        <div className="flex h-64 items-center justify-center rounded-xl border border-dashed border-slate-700 bg-slate-950/60 p-6 text-center text-sm text-slate-400">
          {emptyMessage}
        </div>
      ) : (
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 0 }}>
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
                formatter={(value) => [
                  typeof value === "number" ? `${value.toFixed(2)}${valueSuffix}` : value,
                  title,
                ]}
              />
              <Line
                type="monotone"
                dataKey={yKey}
                stroke="#f8fafc"
                strokeWidth={2}
                dot={false}
                connectNulls={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
