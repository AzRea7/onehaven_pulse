from typing import Any

from pydantic import BaseModel, Field


class ContextRisk(BaseModel):
    code: str
    severity: str
    message: str


class MarketContextEvidence(BaseModel):
    price_growth_yoy: float | None = None
    price_growth_metric: str | None = None

    rent_growth_yoy: float | None = None
    rent_growth_metric: str | None = None

    inventory_trend: str
    active_listings_yoy: float | None = None
    months_supply: float | None = None
    median_days_on_market: float | None = None

    affordability: str
    payment_to_income_ratio: float | None = None
    price_to_income_ratio: float | None = None

    unemployment_rate: float | None = None
    building_permits: float | None = None

    composite_cycle_score: float | None = None


class MarketContextMcpMetadata(BaseModel):
    tool_name: str = "get_market_context"
    resource_type: str = "market"
    resource_id: str
    schema_version: str = "1.0"


class MarketContextResponse(BaseModel):
    geo_id: str
    market: str
    geo_type: str

    latest_period: str | None = None
    latest_data_period: str | None = None
    data_status: str

    cycle_phase: str
    investor_signal: str
    confidence_score: float

    evidence: MarketContextEvidence
    score_breakdown: dict[str, float | None]
    coverage: dict[str, bool]

    risks: list[ContextRisk]
    data_quality: dict[str, Any]

    source_freshness: list[dict[str, Any]] = Field(default_factory=list)
    mcp: MarketContextMcpMetadata
