from fastapi import APIRouter
from sqlalchemy import text

from app.db.session import engine

router = APIRouter(prefix="/markets", tags=["markets"])


@router.get("/metrics/summary")
def market_metrics_summary():
    with engine.connect() as connection:
        table_count_result = connection.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM analytics.market_monthly_metrics
                """
            )
        )
        metric_count = table_count_result.scalar()

        latest_period_result = connection.execute(
            text(
                """
                SELECT MAX(period_month) AS latest_period
                FROM analytics.market_monthly_metrics
                """
            )
        )
        latest_period = latest_period_result.scalar()

        source_count_result = connection.execute(
            text(
                """
                SELECT COUNT(*) AS count
                FROM analytics.market_metric_sources
                """
            )
        )
        source_count = source_count_result.scalar()

    return {
        "market_monthly_metrics_count": metric_count,
        "market_metric_sources_count": source_count,
        "latest_period": latest_period.isoformat() if latest_period else None,
    }
