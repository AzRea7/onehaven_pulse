from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any


VALID_PERIOD_GRAINS = {"daily", "weekly", "monthly", "quarterly", "annual"}


@dataclass(frozen=True)
class MarketMetricRecord:
    geo_id: str
    period_month: date
    metric_name: str
    metric_value: Decimal
    metric_unit: str
    source: str
    dataset: str
    source_file_id: str | None = None
    pipeline_run_id: str | None = None
    source_value: Decimal | None = None
    source_period: date | None = None
    period_grain: str = "monthly"
    transformation_notes: str | None = None
    source_flags: dict[str, Any] = field(default_factory=dict)
    quality_flags: dict[str, Any] = field(default_factory=dict)

    def validate(self) -> None:
        if not self.geo_id:
            raise ValueError("geo_id is required")

        if not self.period_month:
            raise ValueError("period_month is required")

        if self.period_month.day != 1:
            raise ValueError("period_month must be normalized to first day of month")

        if not self.metric_name:
            raise ValueError("metric_name is required")

        if self.metric_value is None:
            raise ValueError("metric_value is required")

        if not self.metric_unit:
            raise ValueError("metric_unit is required")

        if not self.source:
            raise ValueError("source is required")

        if not self.dataset:
            raise ValueError("dataset is required")

        if self.period_grain not in VALID_PERIOD_GRAINS:
            raise ValueError(f"period_grain must be one of {sorted(VALID_PERIOD_GRAINS)}")
