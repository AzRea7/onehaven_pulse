from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _float_or_none(value: Any) -> float | None:
    return float(value) if value is not None else None


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if value is not None else None


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
        normalized_state = state.lower().strip()
        filters.append(
            """(
                lower(COALESCE(g.state_code, '')) = :state
                OR lower(COALESCE(g.state_name, '')) = :state
                OR lower(COALESCE(g.name, '')) LIKE :state_suffix
                OR lower(COALESCE(g.display_name, '')) LIKE :state_suffix
            )"""
        )
        params["state"] = normalized_state
        params["state_suffix"] = f"%, {normalized_state}"

    if cycle_phase:
        filters.append("lower(COALESCE(labeled.cycle_phase, '')) = :cycle_phase")
        params["cycle_phase"] = cycle_phase.lower()

    if investor_signal:
        normalized_signal = investor_signal.lower().replace("-", "_").replace(" ", "_")
        filters.append(
            """(
                lower(COALESCE(labeled.investor_signal, '')) = :investor_signal
                OR lower(COALESCE(labeled.investor_stance, '')) = :investor_signal
                OR lower(replace(COALESCE(labeled.investor_signal, ''), ' ', '_')) = :investor_signal
            )"""
        )
        params["investor_signal"] = normalized_signal

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
                    )::numeric / 4.0
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
                          - GREATEST(COALESCE(m.payment_to_income_ratio, 0) - 0.30, 0) * 50
                        )
                    )
                )::numeric AS screener_score,

                CASE
                    WHEN m.zhvi_yoy IS NULL THEN 'missing'
                    WHEN m.zhvi_yoy BETWEEN 2 AND 12 THEN 'positive'
                    WHEN m.zhvi_yoy BETWEEN -2 AND 18 THEN 'neutral'
                    ELSE 'negative'
                END AS price_momentum_status,

                CASE
                    WHEN m.zori_yoy IS NULL THEN 'missing'
                    WHEN m.zori_yoy >= 3 THEN 'positive'
                    WHEN m.zori_yoy >= 0 THEN 'neutral'
                    ELSE 'negative'
                END AS rent_momentum_status,

                CASE
                    WHEN m.payment_to_income_ratio IS NULL THEN 'missing'
                    WHEN m.payment_to_income_ratio <= 0.28 THEN 'positive'
                    WHEN m.payment_to_income_ratio <= 0.36 THEN 'neutral'
                    ELSE 'negative'
                END AS affordability_status,

                CASE
                    WHEN m.unemployment_rate IS NULL THEN 'missing'
                    WHEN m.unemployment_rate <= 4.5 THEN 'positive'
                    WHEN m.unemployment_rate <= 6.5 THEN 'neutral'
                    ELSE 'negative'
                END AS labor_status,

                CASE
                    WHEN m.building_permits IS NULL THEN true
                    ELSE false
                END AS material_missing_score_inputs

            FROM latest_rows m
        ),
        status_counts AS (
            SELECT
                scored.*,

                (
                    CASE WHEN price_momentum_status = 'negative' THEN 1 ELSE 0 END
                  + CASE WHEN rent_momentum_status = 'negative' THEN 1 ELSE 0 END
                  + CASE WHEN affordability_status = 'negative' THEN 1 ELSE 0 END
                  + CASE WHEN labor_status = 'negative' THEN 1 ELSE 0 END
                ) AS core_negative_count,

                (
                    CASE WHEN price_momentum_status = 'positive' THEN 1 ELSE 0 END
                  + CASE WHEN rent_momentum_status = 'positive' THEN 1 ELSE 0 END
                  + CASE WHEN affordability_status = 'positive' THEN 1 ELSE 0 END
                  + CASE WHEN labor_status = 'positive' THEN 1 ELSE 0 END
                ) AS core_positive_count,

                (
                    CASE WHEN price_momentum_status = 'missing' THEN 1 ELSE 0 END
                  + CASE WHEN rent_momentum_status = 'missing' THEN 1 ELSE 0 END
                  + CASE WHEN affordability_status = 'missing' THEN 1 ELSE 0 END
                  + CASE WHEN labor_status = 'missing' THEN 1 ELSE 0 END
                ) AS core_missing_count

            FROM scored
        ),
        labeled AS (
            SELECT
                status_counts.*,

                CASE
                    WHEN confidence_score < 0.4 OR core_missing_count >= 2 THEN 'Insufficient Data'
                    WHEN screener_score >= 70 THEN 'Expansion'
                    WHEN screener_score >= 50 THEN 'Recovery'
                    WHEN screener_score >= 35 THEN 'Stabilizing'
                    ELSE 'Contraction'
                END AS cycle_phase,

                CASE
                    WHEN confidence_score < 0.4 OR core_missing_count >= 2 THEN 'Insufficient Data'
                    WHEN screener_score >= 70 THEN 'Buy Watch'
                    WHEN screener_score >= 50 THEN 'Hold'
                    WHEN screener_score >= 35 THEN 'Caution'
                    ELSE 'Avoid'
                END AS investor_signal,

                CASE
                    WHEN confidence_score < 0.4 OR core_missing_count >= 2 THEN 'insufficient_data'
                    WHEN confidence_score < 0.6 THEN 'avoid'
                    WHEN core_negative_count >= 3 THEN 'avoid'
                    WHEN affordability_status = 'negative' AND labor_status = 'negative' THEN 'avoid'
                    WHEN
                        confidence_score >= 0.85
                        AND rent_momentum_status = 'positive'
                        AND price_momentum_status IN ('positive', 'neutral')
                        AND affordability_status IN ('positive', 'neutral')
                        AND labor_status IN ('positive', 'neutral')
                        AND core_negative_count = 0
                        AND core_missing_count = 0
                        AND material_missing_score_inputs = false
                    THEN 'attractive'
                    WHEN
                        core_positive_count >= 2
                        AND core_negative_count <= 2
                        AND confidence_score >= 0.7
                    THEN 'watchlist'
                    ELSE 'mixed'
                END AS investor_stance,

                CASE
                    WHEN confidence_score < 0.4 OR core_missing_count >= 2 THEN 'Insufficient Data'
                    WHEN confidence_score < 0.6 THEN 'Avoid'
                    WHEN core_negative_count >= 3 THEN 'Avoid'
                    WHEN affordability_status = 'negative' AND labor_status = 'negative' THEN 'Avoid'
                    WHEN
                        confidence_score >= 0.85
                        AND rent_momentum_status = 'positive'
                        AND price_momentum_status IN ('positive', 'neutral')
                        AND affordability_status IN ('positive', 'neutral')
                        AND labor_status IN ('positive', 'neutral')
                        AND core_negative_count = 0
                        AND core_missing_count = 0
                        AND material_missing_score_inputs = false
                    THEN 'Attractive'
                    WHEN
                        core_positive_count >= 2
                        AND core_negative_count <= 2
                        AND confidence_score >= 0.7
                    THEN 'Watchlist'
                    ELSE 'Mixed'
                END AS investor_stance_label,

                ROUND(
                    LEAST(
                        1,
                        GREATEST(
                            0,
                            (
                                0.50
                              + CASE WHEN price_momentum_status = 'positive' THEN 0.08 WHEN price_momentum_status = 'negative' THEN -0.12 ELSE 0 END
                              + CASE WHEN rent_momentum_status = 'positive' THEN 0.12 WHEN rent_momentum_status = 'negative' THEN -0.12 ELSE 0 END
                              + CASE WHEN affordability_status = 'positive' THEN 0.12 WHEN affordability_status = 'negative' THEN -0.15 ELSE 0 END
                              + CASE WHEN labor_status = 'positive' THEN 0.08 WHEN labor_status = 'negative' THEN -0.12 ELSE 0 END
                              + CASE WHEN confidence_score >= 0.85 THEN 0.08 WHEN confidence_score < 0.6 THEN -0.15 ELSE 0 END
                              + CASE WHEN material_missing_score_inputs THEN -0.05 ELSE 0 END
                            )
                        )
                    ),
                    4
                ) AS investor_stance_score,

                'investor_signal_v2'::text AS investor_signal_rule_version

            FROM status_counts
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
                labeled.investor_stance,
                labeled.investor_stance_label,
                labeled.investor_stance_score,
                labeled.investor_signal_rule_version,
                labeled.material_missing_score_inputs,
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
            investor_stance_score DESC NULLS LAST,
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
            "home_price_yoy": _float_or_none(row["home_price_yoy"]),
            "rent_yoy": _float_or_none(row["rent_yoy"]),
            "active_listings_yoy": None,
            "months_supply": None,
            "median_days_on_market": None,
            "payment_to_income_ratio": _float_or_none(row["payment_to_income_ratio"]),
            "price_to_income_ratio": _float_or_none(row["price_to_income_ratio"]),
            "unemployment_rate": _float_or_none(row["unemployment_rate"]),
            "building_permits": _float_or_none(row["building_permits"]),
            "composite_cycle_score": _float_or_none(row["composite_cycle_score"]),
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
                    "latitude": _float_or_none(row["latitude"]),
                    "longitude": _float_or_none(row["longitude"]),
                },
                "latest_period": _iso_or_none(row["latest_period"]),
                "latest_data_period": _iso_or_none(row["latest_data_period"]),
                "data_status": "latest_data_period",
                "cycle_phase": row["cycle_phase"],
                "investor_signal": row["investor_signal"],
                "investor_stance": row["investor_stance"],
                "investor_stance_label": row["investor_stance_label"],
                "investor_stance_score": _float_or_none(row["investor_stance_score"]),
                "investor_signal_rule_version": row["investor_signal_rule_version"],
                "material_missing_score_inputs": bool(row["material_missing_score_inputs"])
                if row["material_missing_score_inputs"] is not None
                else None,
                "confidence_score": _float_or_none(row["confidence_score"]),
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
