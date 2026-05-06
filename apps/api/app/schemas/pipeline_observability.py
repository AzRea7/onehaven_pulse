from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class PipelineRunItem(BaseModel):
    id: str
    pipeline_name: str
    source: str | None = None
    dataset: str | None = None
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    records_extracted: int | None = None
    records_loaded: int | None = None
    records_failed: int | None = None
    unmatched_count: int | None = None
    error_message: str | None = None
    metadata_json: dict[str, Any] | None = None

    model_config = ConfigDict(from_attributes=True)


class SourceFileItem(BaseModel):
    id: str
    pipeline_run_id: str
    source: str
    dataset: str
    source_url: str | None = None
    raw_file_path: str | None = None
    storage_backend: str | None = None
    file_format: str | None = None
    checksum_sha256: str | None = None
    file_size_bytes: int | None = None
    record_count: int | None = None
    source_period_start: str | None = None
    source_period_end: str | None = None
    load_date: str | None = None
    status: str
    error_message: str | None = None
    metadata_json: dict[str, Any] | None = None
    created_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class PipelineRunDetail(BaseModel):
    run: PipelineRunItem
    source_files: list[SourceFileItem]


class PipelineRunSummary(BaseModel):
    total: int
    running: int
    success: int
    failed: int
    other: int
    records_extracted: int
    records_loaded: int
    records_failed: int
    unmatched_count: int
    latest_started_at: datetime | None = None


class PipelineRunsResponse(BaseModel):
    summary: PipelineRunSummary
    items: list[PipelineRunItem]
