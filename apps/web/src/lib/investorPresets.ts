export type InvestorPresetId =
  | "all"
  | "attractive"
  | "watchlist"
  | "mixed"
  | "avoid"
  | "insufficient_data"
  | "affordable_watchlist"
  | "rent_momentum"
  | "high_confidence"
  | "missing_data_review";

export type InvestorPreset = {
  id: InvestorPresetId;
  label: string;
  description: string;
};

export const INVESTOR_PRESETS: InvestorPreset[] = [
  {
    id: "all",
    label: "All Markets",
    description: "Show all available screener markets.",
  },
  {
    id: "attractive",
    label: "Attractive",
    description: "High-conviction markets under investor_signal_v2 rules.",
  },
  {
    id: "watchlist",
    label: "Watchlist",
    description: "Promising but imperfect markets that deserve monitoring or selective research.",
  },
  {
    id: "mixed",
    label: "Mixed",
    description: "Markets with contradictory or strategy-dependent signals.",
  },
  {
    id: "avoid",
    label: "Avoid",
    description: "Markets the investor signal currently deprioritizes.",
  },
  {
    id: "insufficient_data",
    label: "Insufficient Data",
    description: "Markets where required inputs are missing.",
  },
  {
    id: "affordable_watchlist",
    label: "Affordable Watchlist",
    description: "Markets with favorable payment-to-income profile and non-avoid stance.",
  },
  {
    id: "rent_momentum",
    label: "Rent Momentum",
    description: "Markets with positive rent growth and non-avoid stance.",
  },
  {
    id: "high_confidence",
    label: "High Confidence",
    description: "Markets with stronger confidence and fewer missing metrics.",
  },
  {
    id: "missing_data_review",
    label: "Missing Data Review",
    description: "Markets where missing metrics materially affect interpretation.",
  },
];

export function getInvestorPreset(id: InvestorPresetId): InvestorPreset {
  return INVESTOR_PRESETS.find((preset) => preset.id === id) ?? INVESTOR_PRESETS[0];
}
