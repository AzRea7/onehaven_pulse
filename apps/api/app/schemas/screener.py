from pydantic import BaseModel, Field


class ScreenerMarketIdentity(BaseModel):
    geo_id: str
    geo_type: str
    name: str
    display_name: str | None = None
    state_code: str | None = None
    state_name: str | None = None
    county_fips: str | None = None
    cbsa_code: str | None = None
    zcta: str | None = None
    country_code: str | None = None
    latitude: float | None = None
    longitude: float | None = None


class ScreenerMarketResult(BaseModel):
    market: ScreenerMarketIdentity
    latest_period: str | None = None
    latest_data_period: str | None = None
    data_status: str | None = None
    cycle_phase: str | None = None
    investor_signal: str | None = None
    confidence_score: float | None = None
    values: dict[str, float | None] = Field(default_factory=dict)
    missing_metrics: list[str] = Field(default_factory=list)


class MarketScreenerResponse(BaseModel):
    items: list[ScreenerMarketResult]
    total: int
    limit: int
    offset: int
    filters: dict[str, str | float | int | None]
