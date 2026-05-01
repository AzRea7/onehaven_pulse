import json
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text

from pipelines.common.db import engine


def start_transform_run(
    transform_name: str,
    source: str,
    dataset: str,
    target_table: str,
    metadata: dict | None = None,
) -> str:
    run_id = f"transform_{uuid4().hex}"

    sql = text(
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
            CAST(:metadata_json AS JSON)
        )
        """
    )

    metadata_payload = {
        **(metadata or {}),
        "run_type": "transform",
        "target_table": target_table,
    }

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "id": run_id,
                "pipeline_name": transform_name,
                "source": source,
                "dataset": dataset,
                "started_at": datetime.now(UTC),
                "metadata_json": json.dumps(metadata_payload),
            },
        )

    return run_id


def finish_transform_run(
    run_id: str,
    status: str,
    records_extracted: int | None = None,
    records_loaded: int | None = None,
    records_failed: int | None = None,
    error_message: str | None = None,
) -> None:
    sql = text(
        """
        UPDATE audit.pipeline_runs
        SET
            status = :status,
            finished_at = :finished_at,
            records_extracted = :records_extracted,
            records_loaded = :records_loaded,
            records_failed = :records_failed,
            error_message = :error_message
        WHERE id = :id
        """
    )

    with engine.begin() as connection:
        connection.execute(
            sql,
            {
                "id": run_id,
                "status": status,
                "finished_at": datetime.now(UTC),
                "records_extracted": records_extracted,
                "records_loaded": records_loaded,
                "records_failed": records_failed,
                "error_message": error_message,
            },
        )
