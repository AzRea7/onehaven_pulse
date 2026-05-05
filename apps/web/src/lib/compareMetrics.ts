export type CompareMetricId =
  | "home_price_yoy"
  | "rent_yoy"
  | "payment_to_income_ratio"
  | "price_to_income_ratio"
  | "unemployment_rate"
  | "building_permits"
  | "composite_cycle_score";

export type CompareMetricDefinition = {
  id: CompareMetricId;
  label: string;
  description: string;
  valueSuffix?: string;
};

export const COMPARE_METRICS: CompareMetricDefinition[] = [
  {
    id: "home_price_yoy",
    label: "Home price YoY",
    description: "Year-over-year home-price growth.",
    valueSuffix: "%",
  },
  {
    id: "rent_yoy",
    label: "Rent YoY",
    description: "Year-over-year rent growth.",
    valueSuffix: "%",
  },
  {
    id: "payment_to_income_ratio",
    label: "Payment-to-income",
    description: "Estimated payment burden.",
    valueSuffix: "%",
  },
  {
    id: "price_to_income_ratio",
    label: "Price-to-income",
    description: "Home value relative to household income.",
  },
  {
    id: "unemployment_rate",
    label: "Unemployment",
    description: "Labor-market unemployment rate.",
    valueSuffix: "%",
  },
  {
    id: "building_permits",
    label: "Building permits",
    description: "Monthly building permit count.",
  },
  {
    id: "composite_cycle_score",
    label: "Composite score",
    description: "Overall market-cycle score.",
  },
];

export const DEFAULT_COMPARE_METRICS: CompareMetricId[] = [
  "home_price_yoy",
  "rent_yoy",
  "payment_to_income_ratio",
  "unemployment_rate",
  "composite_cycle_score",
];

export function getCompareMetricDefinition(
  metric: string,
): CompareMetricDefinition {
  return (
    COMPARE_METRICS.find((item) => item.id === metric) ?? {
      id: metric as CompareMetricId,
      label: metric,
      description: metric,
    }
  );
}
