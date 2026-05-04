from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.errors import ApiError
from app.schemas.coverage import MarketCoverageResponse
from app.services.markets import to_iso_date
from app.services.metric_catalog import SCORE_INPUT_METRICS


COVERAGE_GROUPS = {
    "price": [
        "zhvi_yoy",
        "median_sale_price_yoy",
        "home_price_index_yoy",
    ],
    "rent": [
        "zori_yoy",
        "median_rent_yoy",
    ],
    "inventory": [
        "active_listings_yoy",
        "months_supply",
        "median_days_on_market",
    ],
    "affordability": [
        "payment_to_income_ratio",
        "price_to_income_ratio",
    ],
    "labor": [
        "unemployment_rate",
    ],
    "permits": [
        "building_permits",
        "permits_per_1000_people",
    ],
}


def get_latest_data_period(db: Session, *, geo_id: str) -> date | None:
    return db.execute(
        text(
            """
            SELECT MAX(period_month)
            FROM analytics.market_monthly_metrics
            WHERE geo_id = :geo_id
            """
        ),
        {"geo_id": geo_id},
    ).scalar_one()


def get_latest_scoreable_period(db: Session, *, geo_id: str) -> date | None:
    return db.execute(
        text(
            """
            SELECT MAX(period_month)
            FROM analytics.market_monthly_metrics
            WHERE geo_id = :geo_id
              AND (
                    zhvi_yoy IS NOT NULL
                 OR median_sale_price_yoy IS NOT NULL
                 OR home_price_index_yoy IS NOT NULL
                 OR zori_yoy IS NOT NULL
                 OR median_rent_yoy IS NOT NULL
                 OR active_listings_yoy IS NOT NULL
                 OR months_supply IS NOT NULL
                 OR median_days_on_market IS NOT NULL
                 OR payment_to_income_ratio IS NOT NULL
                 OR price_to_income_ratio IS NOT NULL
                 OR unemployment_rate IS NOT NULL
              )
            """
        ),
        {"geo_id": geo_id},
    ).scalar_one()


def get_coverage_row(
    db: Session,
    *,
    geo_id: str,
    period_month: date,
):
    return db.execute(
        text(
            """
            SELECT
                m.zhvi_yoy,
                m.median_sale_price_yoy,
                m.home_price_index_yoy,
                m.zori_yoy,
                m.median_rent_yoy,
                m.active_listings_yoy,
                m.months_supply,
                m.median_days_on_market,
                m.payment_to_income_ratio,
                m.price_to_income_ratio,
                COALESCE(m.unemployment_rate, labor_asof.unemployment_rate) AS unemployment_rate,
                m.building_permits,
                m.permits_per_1000_people
            FROM analytics.market_monthly_metrics m
            LEFT JOIN LATERAL (
                SELECT m2.unemployment_rate
                FROM analytics.market_monthly_metrics m2
                WHERE m2.geo_id = m.geo_id
                  AND m2.period_month <= m.period_month
                  AND m2.unemployment_rate IS NOT NULL
                  AND m2.period_month >= m.period_month - INTERVAL '3 months'
                ORDER BY m2.period_month DESC
                LIMIT 1
            ) labor_asof ON true
            WHERE m.geo_id = :geo_id
              AND m.period_month = :period_month
            """
        ),
        {
            "geo_id": geo_id,
            "period_month": period_month,
        },
    ).mappings().one_or_none()


def get_market_coverage(
    db: Session,
    *,
    geo_id: str,
    period_month: date | None = None,
) -> MarketCoverageResponse:
    market_exists = db.execute(
        text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM geo.dim_geo
                WHERE geo_id = :geo_id
                  AND is_active = true
            )
            """
        ),
        {"geo_id": geo_id},
    ).scalar_one()

    if not market_exists:
        raise ApiError(
            status_code=404,
            code="market_not_found",
            message=f"Market '{geo_id}' was not found.",
            details={"geo_id": geo_id},
        )

    latest_data_period = get_latest_data_period(db, geo_id=geo_id)
    latest_scoreable_period = get_latest_scoreable_period(db, geo_id=geo_id)

    if latest_data_period is None:
        return MarketCoverageResponse(
            geo_id=geo_id,
            latest_data_period=None,
            latest_scoreable_period=None,
            coverage={key: False for key in COVERAGE_GROUPS},
            available_metrics=[],
            missing_score_inputs=sorted(SCORE_INPUT_METRICS),
            data_status="no_metrics",
        )

    selected_period = period_month or latest_data_period
    row = get_coverage_row(db, geo_id=geo_id, period_month=selected_period)

    if row is None:
        return MarketCoverageResponse(
            geo_id=geo_id,
            latest_data_period=to_iso_date(latest_data_period),
            latest_scoreable_period=to_iso_date(latest_scoreable_period),
            coverage={key: False for key in COVERAGE_GROUPS},
            available_metrics=[],
            missing_score_inputs=sorted(SCORE_INPUT_METRICS),
            data_status="missing_period_data",
        )

    coverage = {
        group: any(row[metric] is not None for metric in metrics)
        for group, metrics in COVERAGE_GROUPS.items()
    }

    available_metrics = sorted([
        metric
        for metrics in COVERAGE_GROUPS.values()
        for metric in metrics
        if row[metric] is not None
    ])

    missing_score_inputs = sorted([
        metric
        for metric in SCORE_INPUT_METRICS
        if metric in row and row[metric] is None
    ])

    if latest_scoreable_period is None:
        data_status = "no_scoreable_period"
    elif selected_period == latest_scoreable_period and latest_scoreable_period == latest_data_period:
        data_status = "latest_period_scoreable"
    elif selected_period == latest_scoreable_period:
        data_status = "using_prior_scoreable_period"
    elif selected_period == latest_data_period:
        data_status = "latest_data_period"
    else:
        data_status = "selected_period"

    return MarketCoverageResponse(
        geo_id=geo_id,
        latest_data_period=to_iso_date(latest_data_period),
        latest_scoreable_period=to_iso_date(latest_scoreable_period),
        coverage=coverage,
        available_metrics=available_metrics,
        missing_score_inputs=missing_score_inputs,
        data_status=data_status,
    )
