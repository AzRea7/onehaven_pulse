from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.pipeline_observability import (
    PipelineRunDetail,
    PipelineRunItem,
    PipelineRunsResponse,
    PipelineRunSummary,
    SourceFileItem,
)

LIST_PIPELINE_RUNS_SQL = text(
    """
    SELECT
        id,
        pipeline_name,
        source,
        dataset,
        status,
        started_at,
        finished_at,
        duration_seconds,
        records_extracted,
        records_loaded,
        records_failed,
        unmatched_count,
        error_message,
        metadata_json
    FROM audit.pipeline_runs
    WHERE (:source IS NULL OR source = :source)
      AND (:dataset IS NULL OR dataset = :dataset)
      AND (:status IS NULL OR status = :status)
      AND (:pipeline_name IS NULL OR pipeline_name = :pipeline_name)
      AND (
            :search IS NULL
            OR pipeline_name ILIKE '%' || :search || '%'
            OR COALESCE(source, '') ILIKE '%' || :search || '%'
            OR COALESCE(dataset, '') ILIKE '%' || :search || '%'
            OR COALESCE(error_message, '') ILIKE '%' || :search || '%'
      )
    ORDER BY started_at DESC
    LIMIT :limit
    """
)

GET_PIPELINE_RUN_SQL = text(
    """
    SELECT
        id,
        pipeline_name,
        source,
        dataset,
        status,
        started_at,
        finished_at,
        duration_seconds,
        records_extracted,
        records_loaded,
        records_failed,
        unmatched_count,
        error_message,
        metadata_json
    FROM audit.pipeline_runs
    WHERE id = :run_id
    """
)

GET_SOURCE_FILES_FOR_RUN_SQL = text(
    """
    SELECT
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
        source_period_start::text AS source_period_start,
        source_period_end::text AS source_period_end,
        load_date::text AS load_date,
        status,
        error_message,
        metadata_json,
        created_at
    FROM audit.source_files
    WHERE pipeline_run_id = :run_id
    ORDER BY created_at DESC
    """
)

SUMMARY_SQL = text(
    """
    SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE status = 'running')::int AS running,
        COUNT(*) FILTER (WHERE status = 'success')::int AS success,
        COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
        COUNT(*) FILTER (WHERE status NOT IN ('running', 'success', 'failed'))::int AS other,
        COALESCE(SUM(records_extracted), 0)::int AS records_extracted,
        COALESCE(SUM(records_loaded), 0)::int AS records_loaded,
        COALESCE(SUM(records_failed), 0)::int AS records_failed,
        COALESCE(SUM(unmatched_count), 0)::int AS unmatched_count,
        MAX(started_at) AS latest_started_at
    FROM audit.pipeline_runs
    WHERE (:source IS NULL OR source = :source)
      AND (:dataset IS NULL OR dataset = :dataset)
      AND (:status IS NULL OR status = :status)
      AND (:pipeline_name IS NULL OR pipeline_name = :pipeline_name)
    """
)


def list_pipeline_runs(
    db: Session,
    *,
    source: str | None = None,
    dataset: str | None = None,
    status: str | None = None,
    pipeline_name: str | None = None,
    search: str | None = None,
    limit: int = 100,
) -> PipelineRunsResponse:
    rows = db.execute(
        LIST_PIPELINE_RUNS_SQL,
        {
            "source": source,
            "dataset": dataset,
            "status": status,
            "pipeline_name": pipeline_name,
            "search": search,
            "limit": limit,
        },
    ).mappings().all()

    items = [PipelineRunItem.model_validate(dict(row)) for row in rows]

    summary_row = db.execute(
        SUMMARY_SQL,
        {
            "source": source,
            "dataset": dataset,
            "status": status,
            "pipeline_name": pipeline_name,
        },
    ).mappings().one()

    summary = PipelineRunSummary.model_validate(dict(summary_row))

    return PipelineRunsResponse(summary=summary, items=items)


def get_pipeline_run_detail(db: Session, *, run_id: str) -> PipelineRunDetail | None:
    run_row = db.execute(GET_PIPELINE_RUN_SQL, {"run_id": run_id}).mappings().first()

    if run_row is None:
        return None

    source_file_rows = db.execute(
        GET_SOURCE_FILES_FOR_RUN_SQL,
        {"run_id": run_id},
    ).mappings().all()

    return PipelineRunDetail(
        run=PipelineRunItem.model_validate(dict(run_row)),
        source_files=[SourceFileItem.model_validate(dict(row)) for row in source_file_rows],
    )


def get_pipeline_run_summary(
    db: Session,
    *,
    source: str | None = None,
    dataset: str | None = None,
    status: str | None = None,
    pipeline_name: str | None = None,
) -> PipelineRunSummary:
    row = db.execute(
        SUMMARY_SQL,
        {
            "source": source,
            "dataset": dataset,
            "status": status,
            "pipeline_name": pipeline_name,
        },
    ).mappings().one()

    return PipelineRunSummary.model_validate(dict(row))
