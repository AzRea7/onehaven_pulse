import type { InvestorDimensionStatus, InvestorStance } from "./api";

export function formatInvestorDimensionName(name: string): string {
  return name
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function investorStanceTone(stance: InvestorStance): string {
  switch (stance) {
    case "attractive":
      return "border-emerald-200 bg-emerald-50 text-emerald-900";
    case "watchlist":
      return "border-blue-200 bg-blue-50 text-blue-900";
    case "mixed":
      return "border-amber-200 bg-amber-50 text-amber-900";
    case "avoid":
      return "border-rose-200 bg-rose-50 text-rose-900";
    case "insufficient_data":
      return "border-slate-200 bg-slate-50 text-slate-800";
    default:
      return "border-slate-200 bg-slate-50 text-slate-800";
  }
}

export function dimensionStatusTone(status: InvestorDimensionStatus): string {
  switch (status) {
    case "positive":
      return "bg-emerald-100 text-emerald-800";
    case "neutral":
      return "bg-slate-100 text-slate-700";
    case "negative":
      return "bg-rose-100 text-rose-800";
    case "missing":
      return "bg-amber-100 text-amber-800";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

export function severityTone(severity: string): string {
  switch (severity) {
    case "high":
      return "bg-rose-100 text-rose-800";
    case "medium":
      return "bg-amber-100 text-amber-800";
    case "low":
      return "bg-slate-100 text-slate-700";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

export function formatSignalValue(value: unknown): string {
  if (value === null || value === undefined || value === "") {
    return "missing";
  }

  if (typeof value === "number") {
    if (Math.abs(value) < 1) {
      return value.toFixed(3);
    }
    return value.toFixed(2);
  }

  return String(value);
}
