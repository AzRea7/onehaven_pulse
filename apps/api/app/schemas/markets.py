from typing import Any, Literal

from pydantic import BaseModel, Field


GeoType = Literal["national", "state", "metro", "county", "zcta"]

CyclePhase = Literal[
    "Expansion",
    "Peak",
    "Correction",
    "Recovery",
    "Stabilizing",
    "Insufficient Data",
]

InvestorSignal = Literal[
    "Buy Watch",
    "Selective Buy",
    "Hold",
    "Caution",
    "Avoid Watch",
    "Insufficient Data",
]


class MarketListItem(BaseModel):
    geo_id: str
    geo_type: GeoType
    name: str
    display_name: str | None = None
    state_code: str | None = None
    state_name: str | None = None
    county_fips: str | None = None
    cbsa_code: str | None = None
    zcta: str | None = None
    country_code: str
    latitude: float | None = None
    longitude: float | None = None


class MarketListResponse(BaseModel):
    items: list[MarketListItem]
    limit: int = Field(ge=1, le=100)
    offset: int = Field(ge=0)
    total: int = Field(ge=0)


class MetricValue(BaseModel):
    metric: str | None = None
    value: float | None = None


class InventoryCondition(BaseModel):
    active_listings_yoy: float | None = None
    months_supply: float | None = None
    median_days_on_market: float | None = None
    condition: str


class ScoreBreakdown(BaseModel):
    composite_cycle_score: float | None = None
    price_momentum: float | None = None
    rent_momentum: float | None = None
    inventory_tightness: float | None = None
    affordability: float | None = None
    labor_market: float | None = None
    data_completeness: float


class SourceFreshnessItem(BaseModel):
    source: str
    dataset: str
    latest_source_period: str | None = None
    last_loaded_at: str | None = None
    last_status: str | None = None
    is_stale: bool | None = None
    stale_reason: str | None = None
    record_count: int | None = None


class MarketDetailResponse(BaseModel):
    market: MarketListItem

    # Period used for cycle scoring and displayed market intelligence.
    latest_period: str | None = None

    # Most recent period where any metric exists for the market.
    latest_data_period: str | None = None

    # Explains whether the detail response used the newest scoreable row,
    # found only non-scoreable data, or has no metrics at all.
    data_status: str

    cycle_phase: CyclePhase
    confidence_score: float
    investor_signal: InvestorSignal
    price_growth: MetricValue
    rent_growth: MetricValue
    inventory_condition: InventoryCondition
    score_breakdown: ScoreBreakdown
    source_freshness: list[SourceFreshnessItem]
    quality_flags: dict[str, Any]
    source_flags: dict[str, Any]


class MarketTimeSeriesPoint(BaseModel):
    period_month: str
    values: dict[str, float | None]
    missing_metrics: list[str]


class MarketTimeSeriesResponse(BaseModel):
    market: MarketListItem
    metrics: list[str]
    start_date: str | None = None
    end_date: str | None = None
    date_window_source: str = "explicit_or_empty"
    items: list[MarketTimeSeriesPoint]
