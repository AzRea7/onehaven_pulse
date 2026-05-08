from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


InvestorStance = Literal[
    "attractive",
    "watchlist",
    "mixed",
    "avoid",
    "insufficient_data",
]

DimensionStatus = Literal[
    "positive",
    "neutral",
    "negative",
    "missing",
]

SignalSeverity = Literal[
    "low",
    "medium",
    "high",
]


class InvestorSignalEvidence(BaseModel):
    metric_name: str
    value: Any = None
    period: str | None = None
    interpretation: str


class InvestorSignalDriver(BaseModel):
    name: str
    status: DimensionStatus
    message: str
    evidence: list[InvestorSignalEvidence] = Field(default_factory=list)


class InvestorSignalRisk(BaseModel):
    name: str
    severity: SignalSeverity
    message: str
    evidence: list[InvestorSignalEvidence] = Field(default_factory=list)


class InvestorMarketSignal(BaseModel):
    geo_id: str
    stance: InvestorStance
    stance_label: str
    stance_score: float | None = None
    stance_reason: str
    rule_version: str = "investor_signal_v2"
    confidence_score: float | None = None
    latest_data_period: str | None = None
    latest_scoreable_period: str | None = None
    required_coverage_present: bool
    material_missing_score_inputs: bool
    coverage: dict[str, bool] = Field(default_factory=dict)
    available_metrics: list[str] = Field(default_factory=list)
    missing_score_inputs: list[str] = Field(default_factory=list)
    dimension_statuses: dict[str, DimensionStatus] = Field(default_factory=dict)
    drivers: list[InvestorSignalDriver] = Field(default_factory=list)
    risks: list[InvestorSignalRisk] = Field(default_factory=list)
    rule_trace: list[str] = Field(default_factory=list)
    deterministic: bool = True
