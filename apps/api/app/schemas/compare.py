from pydantic import BaseModel, Field

from app.schemas.markets import MarketListItem


class CompareLatestItem(BaseModel):
    geo_id: str
    latest_period: str | None = None
    latest_data_period: str | None = None
    data_status: str
    cycle_phase: str
    investor_signal: str
    confidence_score: float
    values: dict[str, float | None]
    missing_metrics: list[str]


class CompareTimeSeriesPoint(BaseModel):
    period_month: str
    markets: dict[str, dict[str, float | None]]


class MarketCompareResponse(BaseModel):
    markets: list[MarketListItem] = Field(min_length=2, max_length=5)
    metrics: list[str]
    start_date: str | None = None
    end_date: str | None = None
    date_window_source: str = "explicit_or_empty"
    latest: list[CompareLatestItem]
    timeseries: list[CompareTimeSeriesPoint]
    invalid_geo_ids: list[str]
