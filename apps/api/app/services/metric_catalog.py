from pydantic import BaseModel


class MetricDefinition(BaseModel):
    name: str
    label: str
    unit: str
    category: str
    source_type: str
    description: str
    is_derived: bool = False
    is_score_input: bool = False
    is_map_supported: bool = True
    is_compare_supported: bool = True
    is_timeseries_supported: bool = True


DIRECT_METRIC_COLUMNS: dict[str, str] = {
    "zhvi": "zhvi",
    "zhvi_yoy": "zhvi_yoy",
    "zhvi_mom": "zhvi_mom",
    "home_price_index": "home_price_index",
    "home_price_index_yoy": "home_price_index_yoy",
    "home_price_index_mom": "home_price_index_mom",
    "median_sale_price": "median_sale_price",
    "median_sale_price_yoy": "median_sale_price_yoy",
    "median_sale_price_mom": "median_sale_price_mom",
    "zori": "zori",
    "zori_yoy": "zori_yoy",
    "zori_mom": "zori_mom",
    "median_rent": "median_rent",
    "median_rent_yoy": "median_rent_yoy",
    "rent_to_price_ratio": "rent_to_price_ratio",
    "active_listings": "active_listings",
    "active_listings_yoy": "active_listings_yoy",
    "new_listings": "new_listings",
    "new_listings_yoy": "new_listings_yoy",
    "homes_sold": "homes_sold",
    "homes_sold_yoy": "homes_sold_yoy",
    "months_supply": "months_supply",
    "median_days_on_market": "median_days_on_market",
    "sale_to_list_ratio": "sale_to_list_ratio",
    "price_drops_pct": "price_drops_pct",
    "mortgage_rate_30y": "mortgage_rate_30y",
    "fed_funds_rate": "fed_funds_rate",
    "cpi": "cpi",
    "unemployment_rate": "unemployment_rate",
    "estimated_monthly_payment": "estimated_monthly_payment",
    "payment_to_income_ratio": "payment_to_income_ratio",
    "price_to_income_ratio": "price_to_income_ratio",
    "building_permits": "building_permits",
    "permits_per_1000_people": "permits_per_1000_people",
    "population": "population",
    "population_yoy": "population_yoy",
    "median_household_income": "median_household_income",
    "households": "households",
}

DERIVED_METRICS = {
    "home_price_yoy",
    "rent_yoy",
    "composite_cycle_score",
}

SUPPORTED_METRICS = set(DIRECT_METRIC_COLUMNS.keys()) | DERIVED_METRICS

SCORE_INPUT_METRICS = {
    "home_price_yoy",
    "rent_yoy",
    "zhvi_yoy",
    "median_sale_price_yoy",
    "home_price_index_yoy",
    "zori_yoy",
    "median_rent_yoy",
    "active_listings_yoy",
    "months_supply",
    "median_days_on_market",
    "payment_to_income_ratio",
    "price_to_income_ratio",
    "unemployment_rate",
}

METRIC_CATALOG: dict[str, MetricDefinition] = {
    "home_price_yoy": MetricDefinition(
        name="home_price_yoy",
        label="Home Price Growth YoY",
        unit="percent",
        category="price",
        source_type="derived_alias",
        description="Preferred year-over-year home price growth using ZHVI, median sale price, or HPI.",
        is_derived=True,
        is_score_input=True,
    ),
    "rent_yoy": MetricDefinition(
        name="rent_yoy",
        label="Rent Growth YoY",
        unit="percent",
        category="rent",
        source_type="derived_alias",
        description="Preferred year-over-year rent growth using ZORI or median rent.",
        is_derived=True,
        is_score_input=True,
    ),
    "composite_cycle_score": MetricDefinition(
        name="composite_cycle_score",
        label="Composite Cycle Score",
        unit="score",
        category="cycle",
        source_type="api_derived",
        description="Derived market-cycle score used by the Market Engine.",
        is_derived=True,
        is_score_input=False,
    ),
    "zhvi": MetricDefinition(
        name="zhvi",
        label="Zillow Home Value Index",
        unit="usd",
        category="price",
        source_type="stored_metric",
        description="Zillow Home Value Index.",
    ),
    "zhvi_yoy": MetricDefinition(
        name="zhvi_yoy",
        label="ZHVI YoY",
        unit="percent",
        category="price",
        source_type="stored_metric",
        description="Year-over-year growth in ZHVI.",
        is_score_input=True,
    ),
    "zori": MetricDefinition(
        name="zori",
        label="Zillow Observed Rent Index",
        unit="usd",
        category="rent",
        source_type="stored_metric",
        description="Zillow Observed Rent Index.",
    ),
    "zori_yoy": MetricDefinition(
        name="zori_yoy",
        label="ZORI YoY",
        unit="percent",
        category="rent",
        source_type="stored_metric",
        description="Year-over-year growth in ZORI.",
        is_score_input=True,
    ),
    "building_permits": MetricDefinition(
        name="building_permits",
        label="Building Permits",
        unit="count",
        category="supply",
        source_type="stored_metric",
        description="Monthly residential building permits.",
    ),
    "active_listings_yoy": MetricDefinition(
        name="active_listings_yoy",
        label="Active Listings YoY",
        unit="percent",
        category="inventory",
        source_type="stored_metric",
        description="Year-over-year active listing growth.",
        is_score_input=True,
    ),
    "months_supply": MetricDefinition(
        name="months_supply",
        label="Months Supply",
        unit="months",
        category="inventory",
        source_type="stored_metric",
        description="Months of available housing supply.",
        is_score_input=True,
    ),
    "median_days_on_market": MetricDefinition(
        name="median_days_on_market",
        label="Median Days on Market",
        unit="days",
        category="inventory",
        source_type="stored_metric",
        description="Median number of days listings remain on market.",
        is_score_input=True,
    ),
    "payment_to_income_ratio": MetricDefinition(
        name="payment_to_income_ratio",
        label="Payment-to-Income Ratio",
        unit="ratio",
        category="affordability",
        source_type="stored_metric",
        description="Estimated monthly housing payment divided by household income.",
        is_score_input=True,
    ),
    "price_to_income_ratio": MetricDefinition(
        name="price_to_income_ratio",
        label="Price-to-Income Ratio",
        unit="ratio",
        category="affordability",
        source_type="stored_metric",
        description="Home price divided by household income.",
        is_score_input=True,
    ),
    "unemployment_rate": MetricDefinition(
        name="unemployment_rate",
        label="Unemployment Rate",
        unit="percent",
        category="labor",
        source_type="stored_metric",
        description="Unemployment rate.",
        is_score_input=True,
    ),
}

# Fill in generic definitions for supported direct metrics not explicitly defined above.
for metric_name in sorted(SUPPORTED_METRICS):
    if metric_name not in METRIC_CATALOG:
        METRIC_CATALOG[metric_name] = MetricDefinition(
            name=metric_name,
            label=metric_name.replace("_", " ").title(),
            unit="unknown",
            category="other",
            source_type="stored_metric" if metric_name in DIRECT_METRIC_COLUMNS else "api_derived",
            description=f"{metric_name} metric.",
            is_derived=metric_name in DERIVED_METRICS,
            is_score_input=metric_name in SCORE_INPUT_METRICS,
        )


def get_supported_metrics() -> list[str]:
    return sorted(SUPPORTED_METRICS)


def get_metric_catalog() -> list[MetricDefinition]:
    return [
        METRIC_CATALOG[name]
        for name in sorted(METRIC_CATALOG.keys())
    ]
