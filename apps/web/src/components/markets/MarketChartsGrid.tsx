"use client";

import { LineChartCard } from "@/components/ui/LineChartCard";
import type { MarketTimeSeriesResponse } from "@/lib/api";
import { formatDate } from "@/lib/format";

type MarketChartsGridProps = {
  timeseries: MarketTimeSeriesResponse;
};

export function MarketChartsGrid({ timeseries }: MarketChartsGridProps) {
  const rows = timeseries.items.map((item) => ({
    period: formatDate(item.period_month),
    period_month: item.period_month,
    home_price_yoy: item.values.home_price_yoy ?? item.values.zhvi_yoy ?? null,
    rent_yoy: item.values.rent_yoy ?? item.values.zori_yoy ?? null,
    payment_to_income_ratio: item.values.payment_to_income_ratio ?? null,
    price_to_income_ratio: item.values.price_to_income_ratio ?? null,
    unemployment_rate: item.values.unemployment_rate ?? null,
    building_permits: item.values.building_permits ?? null,
    composite_cycle_score: item.values.composite_cycle_score ?? null,
  }));

  return (
    <div className="grid gap-5 xl:grid-cols-2">
      <LineChartCard
        title="Price growth"
        description="Home price growth, using the available canonical price metric."
        data={rows}
        xKey="period"
        yKey="home_price_yoy"
        valueSuffix="%"
      />

      <LineChartCard
        title="Rent growth"
        description="Rent growth, using the available canonical rent metric."
        data={rows}
        xKey="period"
        yKey="rent_yoy"
        valueSuffix="%"
      />

      <LineChartCard
        title="Payment-to-income"
        description="Estimated monthly payment burden as a share of income."
        data={rows}
        xKey="period"
        yKey="payment_to_income_ratio"
        valueSuffix="%"
      />

      <LineChartCard
        title="Unemployment"
        description="Latest available labor-market unemployment rate."
        data={rows}
        xKey="period"
        yKey="unemployment_rate"
        valueSuffix="%"
      />

      <LineChartCard
        title="Building permits"
        description="Monthly building permit count where available."
        data={rows}
        xKey="period"
        yKey="building_permits"
      />

      <LineChartCard
        title="Composite cycle score"
        description="Market-cycle score over time."
        data={rows}
        xKey="period"
        yKey="composite_cycle_score"
      />
    </div>
  );
}
