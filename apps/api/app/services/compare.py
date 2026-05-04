from datetime import date
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.errors import ApiError
from app.schemas.compare import (
    CompareLatestItem,
    CompareTimeSeriesPoint,
    MarketCompareResponse,
)
from app.schemas.markets import MarketListItem
from app.services.date_windows import resolve_date_window
from app.services.markets import (
    get_market_detail,
    get_timeseries_metric_value,
    market_identity_from_row,
    parse_timeseries_metrics,
    to_iso_date,
)


DEFAULT_COMPARE_METRICS = (
    "home_price_yoy",
    "rent_yoy",
    "building_permits",
    "composite_cycle_score",
)


def parse_geo_ids(raw_geo_ids: str) -> list[str]:
    geo_ids = [
        geo_id.strip()
        for geo_id in raw_geo_ids.split(",")
        if geo_id.strip()
    ]

    geo_ids = list(dict.fromkeys(geo_ids))

    if len(geo_ids) < 2:
        raise ApiError(
            status_code=422,
            code="compare_market_count_invalid",
            message="Compare requires at least 2 markets.",
            details={
                "minimum": 2,
                "maximum": 5,
                "received": len(geo_ids),
            },
        )

    if len(geo_ids) > 5:
        raise ApiError(
            status_code=422,
            code="compare_market_count_invalid",
            message="Compare supports at most 5 markets.",
            details={
                "minimum": 2,
                "maximum": 5,
                "received": len(geo_ids),
            },
        )

    return geo_ids


def get_compare_markets(
    db: Session,
    *,
    geo_ids: list[str],
) -> tuple[list[MarketListItem], list[str]]:
    rows = db.execute(
        text(
            """
            SELECT
                geo_id,
                geo_type,
                name,
                display_name,
                state_code,
                state_name,
                county_fips,
                cbsa_code,
                zcta,
                country_code,
                latitude,
                longitude
            FROM geo.dim_geo
            WHERE geo_id = ANY(:geo_ids)
              AND is_active = true
            """
        ),
        {"geo_ids": geo_ids},
    ).mappings().all()

    found_by_id = {
        row["geo_id"]: market_identity_from_row(row)
        for row in rows
    }

    ordered_markets = [
        found_by_id[geo_id]
        for geo_id in geo_ids
        if geo_id in found_by_id
    ]

    invalid_geo_ids = [
        geo_id
        for geo_id in geo_ids
        if geo_id not in found_by_id
    ]

    return ordered_markets, invalid_geo_ids


def build_compare_latest_item(
    db: Session,
    *,
    geo_id: str,
    metrics: list[str],
) -> CompareLatestItem:
    detail = get_market_detail(db, geo_id=geo_id)

    values: dict[str, float | None] = {
        "home_price_yoy": detail.price_growth.value,
        "rent_yoy": detail.rent_growth.value,
        "composite_cycle_score": detail.score_breakdown.composite_cycle_score,
    }

    # For direct metrics beyond the detail response, read the latest data row.
    direct_metric_rows = db.execute(
        text(
            """
            SELECT
                geo_id,
                period_month,

                home_price_index,
                home_price_index_yoy,
                home_price_index_mom,

                zhvi,
                zhvi_yoy,
                zhvi_mom,

                median_sale_price,
                median_sale_price_yoy,
                median_sale_price_mom,

                zori,
                zori_yoy,
                zori_mom,

                median_rent,
                median_rent_yoy,
                rent_to_price_ratio,

                active_listings,
                active_listings_yoy,
                new_listings,
                new_listings_yoy,
                homes_sold,
                homes_sold_yoy,
                months_supply,
                median_days_on_market,
                sale_to_list_ratio,
                price_drops_pct,

                mortgage_rate_30y,
                fed_funds_rate,
                cpi,
                unemployment_rate,

                estimated_monthly_payment,
                payment_to_income_ratio,
                price_to_income_ratio,

                building_permits,
                permits_per_1000_people,
                population,
                population_yoy,
                median_household_income,
                households
            FROM analytics.market_monthly_metrics
            WHERE geo_id = :geo_id
            ORDER BY period_month DESC
            LIMIT 1
            """
        ),
        {"geo_id": geo_id},
    ).mappings().one_or_none()

    for metric in metrics:
        if metric in values:
            continue

        values[metric] = (
            get_timeseries_metric_value(direct_metric_rows, metric)
            if direct_metric_rows is not None
            else None
        )

    selected_values = {
        metric: values.get(metric)
        for metric in metrics
    }

    missing_metrics = [
        metric
        for metric, value in selected_values.items()
        if value is None
    ]

    return CompareLatestItem(
        geo_id=geo_id,
        latest_period=detail.latest_period,
        latest_data_period=detail.latest_data_period,
        data_status=detail.data_status,
        cycle_phase=detail.cycle_phase,
        investor_signal=detail.investor_signal,
        confidence_score=detail.confidence_score,
        values=selected_values,
        missing_metrics=missing_metrics,
    )


def get_compare_timeseries_rows(
    db: Session,
    *,
    geo_ids: list[str],
    metrics: list[str],
    start_date: date | None,
    end_date: date | None,
) -> list[CompareTimeSeriesPoint]:
    filters = ["geo_id = ANY(:geo_ids)"]
    params: dict[str, Any] = {
        "geo_ids": geo_ids,
    }

    if start_date is not None:
        filters.append("period_month >= :start_date")
        params["start_date"] = start_date

    if end_date is not None:
        filters.append("period_month <= :end_date")
        params["end_date"] = end_date

    where_clause = " AND ".join(filters)

    rows = db.execute(
        text(
            f"""
            SELECT
                geo_id,
                period_month,

                home_price_index,
                home_price_index_yoy,
                home_price_index_mom,

                zhvi,
                zhvi_yoy,
                zhvi_mom,

                median_sale_price,
                median_sale_price_yoy,
                median_sale_price_mom,

                zori,
                zori_yoy,
                zori_mom,

                median_rent,
                median_rent_yoy,
                rent_to_price_ratio,

                active_listings,
                active_listings_yoy,
                new_listings,
                new_listings_yoy,
                homes_sold,
                homes_sold_yoy,
                months_supply,
                median_days_on_market,
                sale_to_list_ratio,
                price_drops_pct,

                mortgage_rate_30y,
                fed_funds_rate,
                cpi,
                unemployment_rate,

                estimated_monthly_payment,
                payment_to_income_ratio,
                price_to_income_ratio,

                building_permits,
                permits_per_1000_people,
                population,
                population_yoy,
                median_household_income,
                households
            FROM analytics.market_monthly_metrics
            WHERE {where_clause}
            ORDER BY period_month ASC, geo_id ASC
            """
        ),
        params,
    ).mappings().all()

    by_period: dict[str, dict[str, dict[str, float | None]]] = {}

    for row in rows:
        period = to_iso_date(row["period_month"])

        if period not in by_period:
            by_period[period] = {
                geo_id: {
                    metric: None
                    for metric in metrics
                }
                for geo_id in geo_ids
            }

        by_period[period][row["geo_id"]] = {
            metric: get_timeseries_metric_value(row, metric)
            for metric in metrics
        }

    return [
        CompareTimeSeriesPoint(
            period_month=period,
            markets=by_period[period],
        )
        for period in sorted(by_period.keys())
    ]



def get_latest_compare_period(
    db: Session,
    *,
    geo_ids: list[str],
) -> date | None:
    return db.execute(
        text(
            """
            SELECT MAX(period_month) AS latest_period
            FROM analytics.market_monthly_metrics
            WHERE geo_id = ANY(:geo_ids)
            """
        ),
        {"geo_ids": geo_ids},
    ).scalar_one()


def compare_markets(
    db: Session,
    *,
    raw_geo_ids: str,
    raw_metrics: str | None,
    start_date: date | None,
    end_date: date | None,
) -> MarketCompareResponse:
    geo_ids = parse_geo_ids(raw_geo_ids)

    metrics = (
        parse_timeseries_metrics(raw_metrics)
        if raw_metrics is not None and raw_metrics.strip()
        else list(DEFAULT_COMPARE_METRICS)
    )

    markets, invalid_geo_ids = get_compare_markets(db, geo_ids=geo_ids)

    if invalid_geo_ids:
        raise ApiError(
            status_code=422,
            code="invalid_geo_ids",
            message="Some geo_ids were not found.",
            details={
                "invalid_geo_ids": invalid_geo_ids,
                "valid_geo_ids": [market.geo_id for market in markets],
            },
        )

    latest = [
        build_compare_latest_item(
            db,
            geo_id=market.geo_id,
            metrics=metrics,
        )
        for market in markets
    ]

    ordered_geo_ids = [market.geo_id for market in markets]

    latest_compare_period = get_latest_compare_period(
        db,
        geo_ids=ordered_geo_ids,
    )

    resolved_start_date, resolved_end_date, date_window_source = resolve_date_window(
        start_date=start_date,
        end_date=end_date,
        latest_period=latest_compare_period,
    )

    timeseries = get_compare_timeseries_rows(
        db,
        geo_ids=ordered_geo_ids,
        metrics=metrics,
        start_date=resolved_start_date,
        end_date=resolved_end_date,
    )

    return MarketCompareResponse(
        markets=markets,
        metrics=metrics,
        start_date=to_iso_date(resolved_start_date),
        end_date=to_iso_date(resolved_end_date),
        date_window_source=date_window_source,
        latest=latest,
        timeseries=timeseries,
        invalid_geo_ids=[],
    )
