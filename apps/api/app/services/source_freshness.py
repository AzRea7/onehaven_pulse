from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.source_freshness import (
    SourceFreshnessItem,
    SourceFreshnessResponse,
    SourceFreshnessSummary,
)

LIST_SOURCE_FRESHNESS_SQL = text(
    """
    SELECT
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
    FROM audit.source_freshness
    WHERE (:source IS NULL OR source = :source)
      AND (:dataset IS NULL OR dataset = :dataset)
      AND (:status IS NULL OR last_status = :status)
      AND (
            :stale_only IS NULL
            OR (:stale_only = TRUE AND is_stale = TRUE)
            OR (:stale_only = FALSE AND is_stale = FALSE)
      )
      AND (
            :search IS NULL
            OR source ILIKE '%' || :search || '%'
            OR dataset ILIKE '%' || :search || '%'
            OR COALESCE(stale_reason, '') ILIKE '%' || :search || '%'
            OR COALESCE(error_message, '') ILIKE '%' || :search || '%'
      )
    ORDER BY
        is_stale DESC,
        CASE last_status
            WHEN 'failed' THEN 1
            WHEN 'pending' THEN 2
            WHEN 'success' THEN 3
            ELSE 4
        END,
        source,
        dataset
    LIMIT :limit
    """
)

REFRESH_SOURCE_FRESHNESS_SQL = text(
    """
    WITH latest_source_file AS (
        SELECT DISTINCT ON (source, dataset)
            source,
            dataset,
            COALESCE(source_period_end, source_period_start) AS latest_source_period,
            created_at AS last_loaded_at,
            pipeline_run_id,
            status,
            record_count,
            error_message
        FROM audit.source_files
        ORDER BY source, dataset, created_at DESC
    ),
    latest_pipeline_run AS (
        SELECT DISTINCT ON (source, dataset)
            source,
            dataset,
            id AS pipeline_run_id,
            status,
            finished_at,
            records_loaded,
            error_message
        FROM audit.pipeline_runs
        WHERE source IS NOT NULL
          AND dataset IS NOT NULL
        ORDER BY source, dataset, started_at DESC
    ),
    merged AS (
        SELECT
            sf.source,
            sf.dataset,
            COALESCE(lsf.latest_source_period, sf.latest_source_period) AS latest_source_period,
            COALESCE(lsf.last_loaded_at, lpr.finished_at, sf.last_loaded_at) AS last_loaded_at,
            CASE
                WHEN COALESCE(lsf.status, lpr.status, sf.last_status) = 'success'
                    THEN COALESCE(lsf.pipeline_run_id, lpr.pipeline_run_id, sf.last_successful_run_id)
                ELSE sf.last_successful_run_id
            END AS last_successful_run_id,
            COALESCE(lsf.status, lpr.status, sf.last_status) AS last_status,
            COALESCE(lsf.record_count, lpr.records_loaded, sf.record_count) AS record_count,
            COALESCE(lsf.error_message, lpr.error_message, sf.error_message) AS error_message
        FROM audit.source_freshness sf
        LEFT JOIN latest_source_file lsf
            ON lsf.source = sf.source
           AND lsf.dataset = sf.dataset
        LEFT JOIN latest_pipeline_run lpr
            ON lpr.source = sf.source
           AND lpr.dataset = sf.dataset
    )
    UPDATE audit.source_freshness sf
    SET
        latest_source_period = merged.latest_source_period,
        last_loaded_at = merged.last_loaded_at,
        last_successful_run_id = merged.last_successful_run_id,
        last_status = merged.last_status,
        record_count = merged.record_count,
        error_message = merged.error_message,
        is_stale = CASE
            WHEN merged.last_status = 'failed' THEN TRUE
            WHEN merged.last_status = 'pending' THEN TRUE
            WHEN merged.last_loaded_at IS NULL THEN TRUE
            WHEN merged.latest_source_period IS NULL THEN TRUE
            WHEN merged.latest_source_period < (CURRENT_DATE - (sf.freshness_threshold_days || ' days')::interval)::date THEN TRUE
            ELSE FALSE
        END,
        stale_reason = CASE
            WHEN merged.last_status = 'failed' THEN COALESCE(merged.error_message, 'Latest job failed')
            WHEN merged.last_status = 'pending' THEN 'Dataset configured but not loaded yet'
            WHEN merged.last_loaded_at IS NULL THEN 'Dataset has never been loaded'
            WHEN merged.latest_source_period IS NULL THEN 'Latest source period is unknown'
            WHEN merged.latest_source_period < (CURRENT_DATE - (sf.freshness_threshold_days || ' days')::interval)::date
                THEN 'Latest source period is older than freshness threshold'
            ELSE NULL
        END,
        updated_at = NOW()
    FROM merged
    WHERE sf.source = merged.source
      AND sf.dataset = merged.dataset
    """
)


def refresh_source_freshness(db: Session) -> None:
    db.execute(REFRESH_SOURCE_FRESHNESS_SQL)
    db.commit()


def list_source_freshness(
    db: Session,
    *,
    source: str | None = None,
    dataset: str | None = None,
    status: str | None = None,
    stale_only: bool | None = None,
    search: str | None = None,
    limit: int = 200,
    refresh: bool = True,
) -> SourceFreshnessResponse:
    if refresh:
        refresh_source_freshness(db)

    rows = db.execute(
        LIST_SOURCE_FRESHNESS_SQL,
        {
            "source": source,
            "dataset": dataset,
            "status": status,
            "stale_only": stale_only,
            "search": search,
            "limit": limit,
        },
    ).mappings().all()

    items = [SourceFreshnessItem.model_validate(dict(row)) for row in rows]

    total = len(items)
    stale = sum(1 for item in items if item.is_stale)
    failed = sum(1 for item in items if item.status == "failed")
    pending = sum(1 for item in items if item.status == "pending")
    healthy = sum(1 for item in items if not item.is_stale and item.status == "success")

    return SourceFreshnessResponse(
        summary=SourceFreshnessSummary(
            total=total,
            stale=stale,
            failed=failed,
            pending=pending,
            healthy=healthy,
        ),
        items=items,
    )


def now_utc() -> datetime:
    return datetime.now(UTC)
