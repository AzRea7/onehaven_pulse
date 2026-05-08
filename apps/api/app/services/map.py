from datetime import date
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.errors import ApiError
from app.schemas.map import GeoJsonFeature, GeoJsonFeatureCollection
from app.services.markets import (
    build_score_breakdown,
    first_metric_value,
    to_float,
    to_iso_date,
)
from app.services.metric_catalog import DIRECT_METRIC_COLUMNS, SUPPORTED_METRICS


VALID_MAP_GEO_TYPES = {"state", "metro", "county", "zcta"}


def parse_map_metric(metric: str) -> str:
    cleaned = metric.strip()

    if cleaned not in SUPPORTED_METRICS:
        raise ApiError(
            status_code=422,
            code="unsupported_metric",
            message="Unsupported map metric requested.",
            details={
                "unsupported_metric": cleaned,
                "supported_metrics": sorted(SUPPORTED_METRICS),
            },
        )

    return cleaned


def get_map_metric_value(row: Any, metric: str) -> float | None:
    if metric == "home_price_yoy":
        return first_metric_value(
            row,
            [
                "zhvi_yoy",
                "median_sale_price_yoy",
                "home_price_index_yoy",
            ],
        ).value

    if metric == "rent_yoy":
        return first_metric_value(
            row,
            [
                "zori_yoy",
                "median_rent_yoy",
            ],
        ).value

    if metric == "composite_cycle_score":
        score_breakdown, _, _ = build_score_breakdown(row)
        return score_breakdown.composite_cycle_score

    column_name = DIRECT_METRIC_COLUMNS[metric]
    return to_float(row[column_name])


def classify_map_row(row: Any) -> tuple[str, str, str]:
    score_breakdown, cycle_phase, investor_signal = build_score_breakdown(row)

    if score_breakdown.data_completeness == 0:
        return "no_scoreable_period", "Insufficient Data", "Insufficient Data"

    return "scoreable_period", cycle_phase, investor_signal


def get_default_map_period(
    db: Session,
    *,
    geo_type: str,
    metric: str,
) -> date | None:
    if metric in {"home_price_yoy", "rent_yoy"}:
        if metric == "home_price_yoy":
            metric_presence_filter = """
                (
                       zhvi_yoy IS NOT NULL
                    OR median_sale_price_yoy IS NOT NULL
                    OR home_price_index_yoy IS NOT NULL
                )
            """
        else:
            metric_presence_filter = """
                (
                       zori_yoy IS NOT NULL
                    OR median_rent_yoy IS NOT NULL
                )
            """
    elif metric == "composite_cycle_score":
        # Important product behavior:
        # Do not return an empty map just because no market is currently scoreable.
        # Default to the latest period where the geo_type has any market metric row,
        # then individual features can expose null score values and no_scoreable_period.
        metric_presence_filter = "TRUE"
    else:
        column_name = DIRECT_METRIC_COLUMNS[metric]
        metric_presence_filter = f"{column_name} IS NOT NULL"

    return db.execute(
        text(
            f"""
            SELECT MAX(m.period_month) AS period_month
            FROM analytics.market_monthly_metrics m
            JOIN geo.dim_geo g
                ON g.geo_id = m.geo_id
            JOIN geo.geo_geometry gg
                ON gg.geo_id = g.geo_id
            WHERE g.geo_type = :geo_type
              AND g.is_active = true
              AND {metric_presence_filter}
            """
        ),
        {"geo_type": geo_type},
    ).scalar_one()


def get_market_map(
    db: Session,
    *,
    geo_type: str,
    metric: str,
    period_month: date | None,
    state: str | None = None,
) -> GeoJsonFeatureCollection:
    if geo_type not in VALID_MAP_GEO_TYPES:
        raise ApiError(
            status_code=422,
            code="unsupported_geo_type",
            message="Unsupported map geo_type requested.",
            details={
                "unsupported_geo_type": geo_type,
                "supported_geo_types": sorted(VALID_MAP_GEO_TYPES),
            },
        )

    selected_metric = parse_map_metric(metric)

    selected_period = period_month or get_default_map_period(
        db,
        geo_type=geo_type,
        metric=selected_metric,
    )

    if selected_period is None:
        return GeoJsonFeatureCollection(features=[])

    rows = db.execute(
        text(
            """
            SELECT
                g.geo_id,
                g.geo_type,
                g.name,
                g.display_name,
                g.state_code,
                g.state_name,
                g.county_fips,
                g.cbsa_code,
                g.zcta,
                g.country_code,

                ST_AsGeoJSON(
                    COALESCE(gg.simplified_geometry, gg.geometry)
                )::json AS geometry,

                m.period_month,

                m.home_price_index,
                m.home_price_index_yoy,
                m.home_price_index_mom,

                m.zhvi,
                m.zhvi_yoy,
                m.zhvi_mom,

                m.median_sale_price,
                m.median_sale_price_yoy,
                m.median_sale_price_mom,

                m.zori,
                m.zori_yoy,
                m.zori_mom,

                m.median_rent,
                m.median_rent_yoy,
                m.rent_to_price_ratio,

                m.active_listings,
                m.active_listings_yoy,
                m.new_listings,
                m.new_listings_yoy,
                m.homes_sold,
                m.homes_sold_yoy,
                m.months_supply,
                m.median_days_on_market,
                m.sale_to_list_ratio,
                m.price_drops_pct,

                m.mortgage_rate_30y,
                m.fed_funds_rate,
                m.cpi,
                m.unemployment_rate,

                m.estimated_monthly_payment,
                m.payment_to_income_ratio,
                m.price_to_income_ratio,

                m.building_permits,
                m.permits_per_1000_people,
                m.population,
                m.population_yoy,
                m.median_household_income,
                m.households
            FROM geo.dim_geo g
            JOIN geo.geo_geometry gg
                ON gg.geo_id = g.geo_id
            LEFT JOIN analytics.market_monthly_metrics m
                ON m.geo_id = g.geo_id
                AND m.period_month = :period_month
            WHERE g.geo_type = :geo_type
              AND g.is_active = true
              AND COALESCE(gg.simplified_geometry, gg.geometry) IS NOT NULL
              AND (
                    :state_code IS NULL
                    OR upper(COALESCE(g.state_code, '')) = :state_code
                    OR upper(COALESCE(g.name, '')) ~ :state_regex
                    OR upper(COALESCE(g.display_name, '')) ~ :state_regex
                  )
            ORDER BY g.display_name NULLS LAST, g.name, g.geo_id
            """
        ),
        {
            "geo_type": geo_type,
            "period_month": selected_period,
            "state_code": state.strip().upper() if state else None,
            "state_regex": (
                f"(^|[^A-Z]){state.strip().upper()}($|[^A-Z])"
                if state
                else None
            ),
        },
    ).mappings().all()

    features: list[GeoJsonFeature] = []

    for row in rows:
        value = get_map_metric_value(row, selected_metric) if row["period_month"] else None

        if row["period_month"]:
            data_status, cycle_phase, investor_signal = classify_map_row(row)
        else:
            data_status = "missing_period_data"
            cycle_phase = "Insufficient Data"
            investor_signal = "Insufficient Data"

        features.append(
            GeoJsonFeature(
                geometry=row["geometry"],
                properties={
                    "geo_id": row["geo_id"],
                    "geo_type": row["geo_type"],
                    "name": row["name"],
                    "display_name": row["display_name"],
                    "state_code": row["state_code"],
                    "state_name": row["state_name"],
                    "county_fips": row["county_fips"],
                    "cbsa_code": row["cbsa_code"],
                    "zcta": row["zcta"],
                    "country_code": row["country_code"],
                    "period_month": to_iso_date(selected_period),
                    "metric": selected_metric,
                    "value": value,
                    "cycle_phase": cycle_phase,
                    "investor_signal": investor_signal,
                    "data_status": data_status,
                },
            )
        )

    return GeoJsonFeatureCollection(features=features)
