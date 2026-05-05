export type MapMetricId =
  | "composite_cycle_score"
  | "home_price_yoy"
  | "rent_yoy"
  | "building_permits"
  | "payment_to_income_ratio"
  | "unemployment_rate";

export type MapMetricDefinition = {
  id: MapMetricId;
  label: string;
  description: string;
  valueSuffix?: string;
  nullLabel: string;
};

export const MAP_METRICS: MapMetricDefinition[] = [
  {
    id: "composite_cycle_score",
    label: "Composite cycle score",
    description: "Overall market-cycle score.",
    nullLabel: "No score",
  },
  {
    id: "home_price_yoy",
    label: "Home price YoY",
    description: "Year-over-year home-price growth.",
    valueSuffix: "%",
    nullLabel: "No price data",
  },
  {
    id: "rent_yoy",
    label: "Rent YoY",
    description: "Year-over-year rent growth.",
    valueSuffix: "%",
    nullLabel: "No rent data",
  },
  {
    id: "building_permits",
    label: "Building permits",
    description: "Monthly building permit count.",
    nullLabel: "No permit data",
  },
  {
    id: "payment_to_income_ratio",
    label: "Payment-to-income",
    description: "Estimated payment burden.",
    valueSuffix: "%",
    nullLabel: "No affordability data",
  },
  {
    id: "unemployment_rate",
    label: "Unemployment",
    description: "Labor-market unemployment rate.",
    valueSuffix: "%",
    nullLabel: "No labor data",
  },
];

export function getMapMetricDefinition(metric: string): MapMetricDefinition {
  return (
    MAP_METRICS.find((item) => item.id === metric) ?? MAP_METRICS[0]
  );
}
