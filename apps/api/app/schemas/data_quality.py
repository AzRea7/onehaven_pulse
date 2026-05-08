from __future__ import annotations

from pydantic import BaseModel, Field


class MarketDataQualityResponse(BaseModel):
    geo_id: str
    quality_version: str
    as_of_date: str
    latest_period: str | None = None

    coverage_score: float
    freshness_score: float
    validity_score: float
    overall_quality_score: float

    has_price: bool
    has_rent: bool
    has_inventory: bool
    has_affordability: bool
    has_labor: bool
    has_permits: bool

    is_fresh: bool
    has_bad_values: bool

    missing_categories: list[str] = Field(default_factory=list)
    stale_categories: list[str] = Field(default_factory=list)
    quality_issues: list[str] = Field(default_factory=list)


class MarketDataQualityListResponse(BaseModel):
    items: list[MarketDataQualityResponse]
    total: int
    limit: int
    offset: int
