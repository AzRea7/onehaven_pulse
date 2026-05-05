from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def screen_markets(
    db: Session,
    *,
    geo_type: str | None = "metro",
    state: str | None = None,
    cycle_phase: str | None = None,
    investor_signal: str | None = None,
    min_confidence: float | None = None,
    min_price_growth: float | None = None,
    max_price_growth: float | None = None,
    min_rent_growth: float | None = None,
    max_inventory_growth: float | None = None,
    max_payment_to_income: float | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    filters: list[str] = ["g.is_active = true"]
    params: dict[str, Any] = {
        "limit": limit,
        "offset": offset,
    }

    if geo_type:
        filters.append("g.geo_type = :geo_type")
        params["geo_type"] = geo_type

    if state:
        filters.append("lower(g.state_code) = :state")
        params["state"] = state.lower()

    if cycle_phase:
        filters.append("lower(COALESCE(labeled.cycle_phase, '')) = :cycle_phase")
        params["cycle_phase"] = cycle_phase.lower()

    if investor_signal:
        filters.append("lower(COALESCE(labeled.investor_signal, '')) = :investor_signal")
        params["investor_signal"] = investor_signal.lower()

    if min_confidence is not None:
        filters.append("COALESCE(labeled.confidence_score, 0) >= :min_confidence")
        params["min_confidence"] = min_confidence

    if min_price_growth is not None:
        filters.append("m.zhvi_yoy >= :min_price_growth")
        params["min_price_growth"] = min_price_growth

    if max_price_growth is not None:
        filters.append("m.zhvi_yoy <= :max_price_growth")
        params["max_price_growth"] = max_price_growth

    if min_rent_growth is not None:
        filters.append("m.zori_yoy >= :min_rent_growth")
        params["min_rent_growth"] = min_rent_growth

    # Redfin inventory is not available broadly yet. Keep param accepted,
    # but do not reference inventory columns that may not exist in this table.
    if max_inventory_growth is not None:
        filters.append("1 = 1")
        params["max_inventory_growth"] = max_inventory_growth

    if max_payment_to_income is not None:
        filters.append("m.payment_to_income_ratio <= :max_payment_to_income")
        params["max_payment_to_income"] = max_payment_to_income

    where_sql = " AND ".join(filters)

    sql = text(
        f"""
        WITH latest_metric_period AS (
            SELECT
                geo_id,
                MAX(period_month) AS period_month
            FROM analytics.market_monthly_metrics
            GROUP BY geo_id
        ),
        latest_rows AS (
            SELECT m.*
            FROM analytics.market_monthly_metrics m
            JOIN latest_metric_period latest
              ON latest.geo_id = m.geo_id
             AND latest.period_month = m.period_month
        ),
        scored AS (
            SELECT
                m.geo_id,

                (
                    (
                        CASE WHEN m.zhvi_yoy IS NOT NULL THEN 1 ELSE 0 END
                      + CASE WHEN m.zori_yoy IS NOT NULL THEN 1 ELSE 0 END
                      + CASE WHEN (m.payment_to_income_ratio IS NOT NULL OR m.price_to_income_ratio IS NOT NULL) THEN 1 ELSE 0 END
                      + CASE WHEN m.unemployment_rate IS NOT NULL THEN 1 ELSE 0 END
                    )::numeric / 5.0
                ) AS confidence_score,

                LEAST(
                    100,
                    GREATEST(
                        0,
                        (
                            35
                          + COALESCE(m.zhvi_yoy, 0) * 3
                          + COALESCE(m.zori_yoy, 0) * 2
                          - COALESCE(m.unemployment_rate, 0) * 1.5
                          - GREATEST(COALESCE(m.payment_to_income_ratio, 0) - 20, 0) * 0.75
                        )
                    )
                )::numeric AS screener_score

            FROM latest_rows m
        ),
        labeled AS (
            SELECT
                scored.*,
                CASE
                    WHEN scored.confidence_score < 0.4 THEN 'Insufficient Data'
                    WHEN scored.screener_score >= 70 THEN 'Expansion'
                    WHEN scored.screener_score >= 50 THEN 'Recovery'
                    WHEN scored.screener_score >= 35 THEN 'Stabilizing'
                    ELSE 'Contraction'
                END AS cycle_phase,
                CASE
                    WHEN scored.confidence_score < 0.4 THEN 'Insufficient Data'
                    WHEN scored.screener_score >= 70 THEN 'Buy Watch'
                    WHEN scored.screener_score >= 50 THEN 'Hold'
                    WHEN scored.screener_score >= 35 THEN 'Caution'
                    ELSE 'Avoid'
                END AS investor_signal
            FROM scored
        ),
        filtered AS (
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
                g.latitude,
                g.longitude,

                m.period_month AS latest_period,
                m.period_month AS latest_data_period,

                labeled.cycle_phase,
                labeled.investor_signal,
                labeled.confidence_score,
                labeled.screener_score,

                m.zhvi_yoy AS home_price_yoy,
                m.zori_yoy AS rent_yoy,
                NULL::numeric AS active_listings_yoy,
                NULL::numeric AS months_supply,
                NULL::numeric AS median_days_on_market,
                m.payment_to_income_ratio,
                m.price_to_income_ratio,
                m.unemployment_rate,
                m.building_permits,
                labeled.screener_score AS composite_cycle_score

            FROM geo.dim_geo g
            LEFT JOIN latest_rows m
              ON m.geo_id = g.geo_id
            LEFT JOIN labeled
              ON labeled.geo_id = g.geo_id
            WHERE {where_sql}
        )
        SELECT
            *,
            COUNT(*) OVER() AS total
        FROM filtered
        ORDER BY
            confidence_score DESC NULLS LAST,
            screener_score DESC NULLS LAST,
            display_name ASC
        LIMIT :limit
        OFFSET :offset
        """
    )

    rows = db.execute(sql, params).mappings().all()
    total = int(rows[0]["total"]) if rows else 0

    items = []

    for row in rows:
        values = {
            "home_price_yoy": float(row["home_price_yoy"]) if row["home_price_yoy"] is not None else None,
            "rent_yoy": float(row["rent_yoy"]) if row["rent_yoy"] is not None else None,
            "active_listings_yoy": None,
            "months_supply": None,
            "median_days_on_market": None,
            "payment_to_income_ratio": float(row["payment_to_income_ratio"]) if row["payment_to_income_ratio"] is not None else None,
            "price_to_income_ratio": float(row["price_to_income_ratio"]) if row["price_to_income_ratio"] is not None else None,
            "unemployment_rate": float(row["unemployment_rate"]) if row["unemployment_rate"] is not None else None,
            "building_permits": float(row["building_permits"]) if row["building_permits"] is not None else None,
            "composite_cycle_score": float(row["composite_cycle_score"]) if row["composite_cycle_score"] is not None else None,
        }

        missing_metrics = [
            metric for metric, value in values.items() if value is None
        ]

        items.append(
            {
                "market": {
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
                    "latitude": float(row["latitude"]) if row["latitude"] is not None else None,
                    "longitude": float(row["longitude"]) if row["longitude"] is not None else None,
                },
                "latest_period": row["latest_period"].isoformat() if row["latest_period"] is not None else None,
                "latest_data_period": row["latest_data_period"].isoformat() if row["latest_data_period"] is not None else None,
                "data_status": "latest_data_period",
                "cycle_phase": row["cycle_phase"],
                "investor_signal": row["investor_signal"],
                "confidence_score": float(row["confidence_score"])
                if row["confidence_score"] is not None
                else None,
                "values": values,
                "missing_metrics": missing_metrics,
            }
        )

    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "filters": {
            "geo_type": geo_type,
            "state": state,
            "cycle_phase": cycle_phase,
            "investor_signal": investor_signal,
            "min_confidence": min_confidence,
            "min_price_growth": min_price_growth,
            "max_price_growth": max_price_growth,
            "min_rent_growth": min_rent_growth,
            "max_inventory_growth": max_inventory_growth,
            "max_payment_to_income": max_payment_to_income,
        },
    }
