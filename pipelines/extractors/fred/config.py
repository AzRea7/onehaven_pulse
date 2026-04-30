from dataclasses import dataclass

from pipelines.common.settings import settings


@dataclass(frozen=True)
class FredSeries:
    series_id: str
    metric_name: str
    description: str
    frequency_hint: str
    category: str
    source_role: str


FRED_SERIES = [
    FredSeries(
        series_id="MORTGAGE30US",
        metric_name="mortgage_30yr_fixed_rate",
        description="30-Year Fixed Rate Mortgage Average in the United States",
        frequency_hint="weekly",
        category="mortgage_rates",
        source_role="mortgage_rate_outcome",
    ),
    FredSeries(
        series_id="CPIAUCSL",
        metric_name="consumer_price_index",
        description="Consumer Price Index for All Urban Consumers: All Items in U.S. City Average",
        frequency_hint="monthly",
        category="inflation",
        source_role="inflation_context",
    ),
    FredSeries(
        series_id="UNRATE",
        metric_name="unemployment_rate",
        description="Civilian Unemployment Rate",
        frequency_hint="monthly",
        category="labor_macro",
        source_role="macro_labor_context",
    ),
    FredSeries(
        series_id="FEDFUNDS",
        metric_name="effective_federal_funds_rate",
        description="Effective Federal Funds Rate",
        frequency_hint="monthly",
        category="policy_rates",
        source_role="short_rate_policy_context",
    ),
    FredSeries(
        series_id="USREC",
        metric_name="us_recession_indicator",
        description="NBER based Recession Indicators for the United States from the Peak through the Trough",
        frequency_hint="monthly",
        category="cycle_regime",
        source_role="recession_regime",
    ),
    FredSeries(
        series_id="DGS2",
        metric_name="treasury_2yr_constant_maturity",
        description="Market Yield on U.S. Treasury Securities at 2-Year Constant Maturity",
        frequency_hint="daily",
        category="treasury_curve",
        source_role="yield_curve_short_intermediate",
    ),
    FredSeries(
        series_id="DGS5",
        metric_name="treasury_5yr_constant_maturity",
        description="Market Yield on U.S. Treasury Securities at 5-Year Constant Maturity",
        frequency_hint="daily",
        category="treasury_curve",
        source_role="yield_curve_intermediate",
    ),
    FredSeries(
        series_id="DGS10",
        metric_name="treasury_10yr_constant_maturity",
        description="Market Yield on U.S. Treasury Securities at 10-Year Constant Maturity",
        frequency_hint="daily",
        category="treasury_curve",
        source_role="mortgage_rate_driver",
    ),
    FredSeries(
        series_id="DGS30",
        metric_name="treasury_30yr_constant_maturity",
        description="Market Yield on U.S. Treasury Securities at 30-Year Constant Maturity",
        frequency_hint="daily",
        category="treasury_curve",
        source_role="long_rate_context",
    ),
    FredSeries(
        series_id="T10Y2Y",
        metric_name="treasury_10yr_2yr_spread",
        description="10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity",
        frequency_hint="daily",
        category="yield_curve_spreads",
        source_role="yield_curve_inversion_signal",
    ),
    FredSeries(
        series_id="T10Y3M",
        metric_name="treasury_10yr_3mo_spread",
        description="10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity",
        frequency_hint="daily",
        category="yield_curve_spreads",
        source_role="recession_warning_signal",
    ),
]


FRED_SERIES_BY_ID = {series.series_id: series for series in FRED_SERIES}


FRED_API_KEY = settings.fred_api_key
FRED_BASE_URL = settings.fred_base_url
