from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator


Audience = Literal["investor", "operator", "analyst", "general"]
DetailLevel = Literal["brief", "standard", "detailed"]


class CompareSummaryRequest(BaseModel):
    geo_ids: list[str] = Field(..., min_length=2, max_length=5)
    metrics: list[str] = Field(
        default_factory=lambda: [
            "zhvi_yoy",
            "zori_yoy",
            "payment_to_income_ratio",
            "unemployment_rate",
        ],
        min_length=1,
        max_length=12,
    )
    start_date: str | None = Field(default="2024-01-01")
    end_date: str | None = None
    audience: Audience = "investor"
    detail_level: DetailLevel = "standard"

    @field_validator("geo_ids")
    @classmethod
    def dedupe_geo_ids(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()

        for geo_id in value:
            geo_id = geo_id.strip()
            if not geo_id:
                raise ValueError("geo_ids cannot contain empty values")
            if geo_id not in seen:
                cleaned.append(geo_id)
                seen.add(geo_id)

        if len(cleaned) < 2:
            raise ValueError("compare summary requires at least 2 unique geo_ids")

        if len(cleaned) > 5:
            raise ValueError("compare summary supports at most 5 unique geo_ids")

        return cleaned

    @field_validator("metrics")
    @classmethod
    def clean_metrics(cls, value: list[str]) -> list[str]:
        cleaned = [metric.strip() for metric in value if metric.strip()]
        if not cleaned:
            raise ValueError("metrics cannot be empty")
        return cleaned


class EvidenceCitation(BaseModel):
    label: str
    tool_name: str
    field_path: str
    value: Any = None


class CompareSummaryResponse(BaseModel):
    geo_ids: list[str]
    audience: Audience
    detail_level: DetailLevel
    summary: str
    key_takeaways: list[str]
    confidence_explanation: str
    missing_data_explanation: str
    deterministic_scores_note: str
    not_investment_advice: str
    citations: list[EvidenceCitation]
    structured_payloads: dict[str, Any]
