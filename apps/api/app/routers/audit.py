from fastapi import APIRouter
from sqlalchemy import text

from app.db.session import engine

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/source-freshness")
def source_freshness():
    with engine.connect() as connection:
        result = connection.execute(
            text(
                """
                SELECT
                    source,
                    dataset,
                    expected_frequency,
                    freshness_threshold_days,
                    latest_source_period,
                    last_loaded_at,
                    last_status,
                    is_stale,
                    stale_reason,
                    record_count,
                    error_message
                FROM audit.source_freshness
                ORDER BY source, dataset
                """
            )
        )

        rows = result.mappings().all()

    return {
        "items": [
            {
                "source": row["source"],
                "dataset": row["dataset"],
                "expected_frequency": row["expected_frequency"],
                "freshness_threshold_days": row["freshness_threshold_days"],
                "latest_source_period": (
                    row["latest_source_period"].isoformat()
                    if row["latest_source_period"]
                    else None
                ),
                "last_loaded_at": (
                    row["last_loaded_at"].isoformat()
                    if row["last_loaded_at"]
                    else None
                ),
                "last_status": row["last_status"],
                "is_stale": row["is_stale"],
                "stale_reason": row["stale_reason"],
                "record_count": row["record_count"],
                "error_message": row["error_message"],
            }
            for row in rows
        ]
    }
