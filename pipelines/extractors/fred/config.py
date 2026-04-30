from dataclasses import dataclass


@dataclass(frozen=True)
class FredSeries:
    series_id: str
    metric_name: str
    description: str
    frequency_hint: str


FRED_SERIES = [
    FredSeries(
        series_id="MORTGAGE30US",
        metric_name="mortgage_rate_30y",
        description="30-Year Fixed Rate Mortgage Average in the United States",
        frequency_hint="weekly",
    ),
    FredSeries(
        series_id="CPIAUCSL",
        metric_name="cpi",
        description="Consumer Price Index for All Urban Consumers",
        frequency_hint="monthly",
    ),
    FredSeries(
        series_id="UNRATE",
        metric_name="unemployment_rate",
        description="Civilian Unemployment Rate",
        frequency_hint="monthly",
    ),
    FredSeries(
        series_id="FEDFUNDS",
        metric_name="fed_funds_rate",
        description="Federal Funds Effective Rate",
        frequency_hint="monthly",
    ),
    FredSeries(
        series_id="USREC",
        metric_name="recession_indicator",
        description="NBER based Recession Indicators for the United States",
        frequency_hint="monthly",
    ),
]
