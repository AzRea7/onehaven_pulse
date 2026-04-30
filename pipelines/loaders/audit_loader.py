from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from psycopg2.extras import Json
from sqlalchemy import create_engine, text

from pipelines.common.settings import settings


def new_pipeline_run_id() -> str:
    return f"run_{uuid4().hex}"


def new_source_file_id() -> str:
    return f"src_{uuid4().hex}"


def as_json(value: dict[str, Any] | None):
    return Json(value or {})


def get_engine():
    return create_engine(settings.database_url, pool_pre_ping=True)


def start_pipeline_run(
    pipeline_name: str,
    source: str | None,
    dataset: str | None,
    metadata: dict[str, Any] | None = None,
) -> str:
    run_id = new_pipeline_run_id()
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit.pipeline_runs (
                    id,
                    pipeline_name,
                    source,
                    dataset,
                    status,
                    started_at,
                    metadata_json
                )
                VALUES (
                    :id,
                    :pipeline_name,
                    :source,
                    :dataset,
                    'running',
                    :started_at,
                    :metadata_json
                )
                """
            ),
            {
                "id": run_id,
                "pipeline_name": pipeline_name,
                "source": source,
                "dataset": dataset,
                "started_at": datetime.now(UTC),
                "metadata_json": as_json(metadata),
            },
        )

    return run_id


def finish_pipeline_run(
    run_id: str,
    status: str,
    records_extracted: int | None = None,
    records_loaded: int | None = None,
    records_failed: int | None = None,
    error_message: str | None = None,
) -> None:
    engine = get_engine()
    finished_at = datetime.now(UTC)

    with engine.begin() as connection:
        started_at = connection.execute(
            text(
                """
                SELECT started_at
                FROM audit.pipeline_runs
                WHERE id = :run_id
                """
            ),
            {"run_id": run_id},
        ).scalar_one()

        duration_seconds = (finished_at - started_at).total_seconds()

        connection.execute(
            text(
                """
                UPDATE audit.pipeline_runs
                SET
                    status = :status,
                    finished_at = :finished_at,
                    duration_seconds = :duration_seconds,
                    records_extracted = :records_extracted,
                    records_loaded = :records_loaded,
                    records_failed = :records_failed,
                    error_message = :error_message
                WHERE id = :run_id
                """
            ),
            {
                "run_id": run_id,
                "status": status,
                "finished_at": finished_at,
                "duration_seconds": duration_seconds,
                "records_extracted": records_extracted,
                "records_loaded": records_loaded,
                "records_failed": records_failed,
                "error_message": error_message,
            },
        )


def record_source_file(
    pipeline_run_id: str,
    source: str,
    dataset: str,
    source_url: str | None,
    raw_file_path: str,
    file_format: str,
    checksum_sha256: str | None,
    file_size_bytes: int | None,
    record_count: int | None,
    source_period_start: date | None,
    source_period_end: date | None,
    load_date: date,
    status: str,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> str:
    source_file_id = new_source_file_id()
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit.source_files (
                    id,
                    pipeline_run_id,
                    source,
                    dataset,
                    source_url,
                    raw_file_path,
                    storage_backend,
                    file_format,
                    checksum_sha256,
                    file_size_bytes,
                    record_count,
                    source_period_start,
                    source_period_end,
                    load_date,
                    status,
                    error_message,
                    metadata_json
                )
                VALUES (
                    :id,
                    :pipeline_run_id,
                    :source,
                    :dataset,
                    :source_url,
                    :raw_file_path,
                    :storage_backend,
                    :file_format,
                    :checksum_sha256,
                    :file_size_bytes,
                    :record_count,
                    :source_period_start,
                    :source_period_end,
                    :load_date,
                    :status,
                    :error_message,
                    :metadata_json
                )
                """
            ),
            {
                "id": source_file_id,
                "pipeline_run_id": pipeline_run_id,
                "source": source,
                "dataset": dataset,
                "source_url": source_url,
                "raw_file_path": str(Path(raw_file_path)),
                "storage_backend": settings.storage_backend,
                "file_format": file_format,
                "checksum_sha256": checksum_sha256,
                "file_size_bytes": file_size_bytes,
                "record_count": record_count,
                "source_period_start": source_period_start,
                "source_period_end": source_period_end,
                "load_date": load_date,
                "status": status,
                "error_message": error_message,
                "metadata_json": as_json(metadata),
            },
        )

    return source_file_id


def update_source_freshness(
    source: str,
    dataset: str,
    latest_source_period: date | None,
    last_successful_run_id: str | None,
    last_status: str,
    record_count: int | None,
    error_message: str | None = None,
) -> None:
    engine = get_engine()

    is_stale = last_status != "success"
    stale_reason = None if last_status == "success" else error_message or "Latest run did not succeed"

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO audit.source_freshness (
                    source,
                    dataset,
                    expected_frequency,
                    freshness_threshold_days,
                    latest_source_period,
                    last_loaded_at,
                    last_successful_run_id,
                    last_status,
                    is_stale,
                    stale_reason,
                    record_count,
                    error_message,
                    updated_at
                )
                VALUES (
                    :source,
                    :dataset,
                    'weekly',
                    14,
                    :latest_source_period,
                    NOW(),
                    :last_successful_run_id,
                    :last_status,
                    :is_stale,
                    :stale_reason,
                    :record_count,
                    :error_message,
                    NOW()
                )
                ON CONFLICT (source, dataset)
                DO UPDATE SET
                    latest_source_period = EXCLUDED.latest_source_period,
                    last_loaded_at = EXCLUDED.last_loaded_at,
                    last_successful_run_id = EXCLUDED.last_successful_run_id,
                    last_status = EXCLUDED.last_status,
                    is_stale = EXCLUDED.is_stale,
                    stale_reason = EXCLUDED.stale_reason,
                    record_count = EXCLUDED.record_count,
                    error_message = EXCLUDED.error_message,
                    updated_at = NOW()
                """
            ),
            {
                "source": source,
                "dataset": dataset,
                "latest_source_period": latest_source_period,
                "last_successful_run_id": last_successful_run_id,
                "last_status": last_status,
                "is_stale": is_stale,
                "stale_reason": stale_reason,
                "record_count": record_count,
                "error_message": error_message,
            },
        )
