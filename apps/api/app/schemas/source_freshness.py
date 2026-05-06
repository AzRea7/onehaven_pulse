from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field


class SourceFreshnessItem(BaseModel):
    source: str
    dataset: str
    latest_source_period: date | None = None
    last_loaded_at: datetime | None = None
    record_count: int | None = None
    status: str = Field(validation_alias="last_status")
    error_message: str | None = None
    stale_reason: str | None = None
    is_stale: bool
    expected_frequency: str
    freshness_threshold_days: int
    last_successful_run_id: str | None = None
    updated_at: datetime | None = None

    model_config = ConfigDict(populate_by_name=True)


class SourceFreshnessSummary(BaseModel):
    total: int
    stale: int
    failed: int
    pending: int
    healthy: int


class SourceFreshnessResponse(BaseModel):
    summary: SourceFreshnessSummary
    items: list[SourceFreshnessItem]
