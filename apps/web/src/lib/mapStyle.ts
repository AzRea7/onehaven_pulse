import type { MarketMapFeature } from "@/lib/api";

export function formatMapValue(
  value: number | null | undefined,
  suffix = "",
): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "—";
  }

  if (Math.abs(value) >= 1000) {
    return new Intl.NumberFormat("en-US", {
      maximumFractionDigits: 0,
    }).format(value);
  }

  return `${value.toFixed(2)}${suffix}`;
}

export function metricFill(value: number | null | undefined, metric: string): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "#334155";
  }

  if (metric === "composite_cycle_score") {
    if (value >= 70) return "#16a34a";
    if (value >= 50) return "#65a30d";
    if (value >= 35) return "#d97706";
    return "#dc2626";
  }

  if (metric === "payment_to_income_ratio" || metric === "unemployment_rate") {
    if (value <= 4) return "#16a34a";
    if (value <= 7) return "#65a30d";
    if (value <= 10) return "#d97706";
    return "#dc2626";
  }

  if (metric === "building_permits") {
    if (value >= 1000) return "#16a34a";
    if (value >= 250) return "#65a30d";
    if (value > 0) return "#d97706";
    return "#334155";
  }

  if (value >= 5) return "#16a34a";
  if (value >= 2) return "#65a30d";
  if (value >= 0) return "#d97706";
  return "#dc2626";
}

export function cyclePhaseClass(phase: string | null | undefined): string {
  const normalized = (phase ?? "").toLowerCase();

  if (normalized.includes("recovery")) return "text-sky-200";
  if (normalized.includes("expansion")) return "text-emerald-200";
  if (normalized.includes("stabilizing")) return "text-amber-200";
  if (normalized.includes("contraction")) return "text-red-200";

  return "text-slate-300";
}

export function getFeatureDisplayName(feature: MarketMapFeature): string {
  return (
    feature.properties.display_name ||
    feature.properties.name ||
    feature.properties.geo_id
  );
}
