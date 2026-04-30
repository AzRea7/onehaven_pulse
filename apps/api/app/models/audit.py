from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Boolean, Date, DateTime, ForeignKey, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.models.geography import Base


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "audit"}

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_name: Mapped[str] = mapped_column(String(150), nullable=False)
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    dataset: Mapped[str | None] = mapped_column(String(150), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_seconds: Mapped[Decimal | None] = mapped_column(Numeric(12, 3), nullable=True)
    records_extracted: Mapped[int | None] = mapped_column(nullable=True)
    records_loaded: Mapped[int | None] = mapped_column(nullable=True)
    records_failed: Mapped[int | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class SourceFile(Base):
    __tablename__ = "source_files"
    __table_args__ = {"schema": "audit"}

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_run_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("audit.pipeline_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    dataset: Mapped[str] = mapped_column(String(150), nullable=False)
    source_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_file_path: Mapped[str] = mapped_column(Text, nullable=False)
    storage_backend: Mapped[str] = mapped_column(String(50), nullable=False, default="local")
    file_format: Mapped[str] = mapped_column(String(50), nullable=False)
    checksum_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    file_size_bytes: Mapped[int | None] = mapped_column(nullable=True)
    record_count: Mapped[int | None] = mapped_column(nullable=True)
    source_period_start: Mapped[date | None] = mapped_column(Date, nullable=True)
    source_period_end: Mapped[date | None] = mapped_column(Date, nullable=True)
    load_date: Mapped[date] = mapped_column(Date, nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class SourceFreshness(Base):
    __tablename__ = "source_freshness"
    __table_args__ = {"schema": "audit"}

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False)
    dataset: Mapped[str] = mapped_column(String(150), nullable=False)
    expected_frequency: Mapped[str] = mapped_column(String(50), nullable=False)
    freshness_threshold_days: Mapped[int] = mapped_column(Integer, nullable=False)
    latest_source_period: Mapped[date | None] = mapped_column(Date, nullable=True)
    last_loaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_successful_run_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("audit.pipeline_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    last_status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    is_stale: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    stale_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    record_count: Mapped[int | None] = mapped_column(nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )


class DataQualityCheck(Base):
    __tablename__ = "data_quality_checks"
    __table_args__ = {"schema": "audit"}

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    pipeline_run_id: Mapped[str | None] = mapped_column(
        String(64),
        ForeignKey("audit.pipeline_runs.id", ondelete="SET NULL"),
        nullable=True,
    )
    source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    dataset: Mapped[str | None] = mapped_column(String(150), nullable=True)
    table_schema: Mapped[str] = mapped_column(String(100), nullable=False)
    table_name: Mapped[str] = mapped_column(String(150), nullable=False)
    check_name: Mapped[str] = mapped_column(String(150), nullable=False)
    check_type: Mapped[str] = mapped_column(String(100), nullable=False)
    severity: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False)
    checked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    failed_row_count: Mapped[int | None] = mapped_column(nullable=True)
    total_row_count: Mapped[int | None] = mapped_column(nullable=True)
    details_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
