from decimal import Decimal
from typing import Any

from fastapi import HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.errors import ApiError
from app.services.metric_catalog import DIRECT_METRIC_COLUMNS, DERIVED_METRICS, SUPPORTED_METRICS
from app.services.date_windows import resolve_date_window

from app.schemas.markets import (
    InventoryCondition,
    MarketDetailResponse,
    MarketListItem,
    MarketListResponse,
    MarketTimeSeriesPoint,
    MarketTimeSeriesResponse,
    MetricValue,
    ScoreBreakdown,
    SourceFreshnessItem,
)


VALID_GEO_TYPES = {"national", "state", "metro", "county", "zcta"}


def normalize_state_filter(state: str | None) -> str | None:
    if state is None:
        return None

    cleaned = state.strip().upper()

    if cleaned == "":
        return None

    return cleaned


def normalize_search_filter(search: str | None) -> str | None:
    if search is None:
        return None

    cleaned = search.strip()

    if cleaned == "":
        return None

    return cleaned


def to_float(value: Any) -> float | None:
    if value is None:
        return None

    if isinstance(value, Decimal):
        return float(value)

    return float(value)


def to_iso_date(value: Any) -> str | None:
    if value is None:
        return None

    return value.isoformat()


def to_iso_datetime(value: Any) -> str | None:
    if value is None:
        return None

    return value.isoformat()


def market_identity_from_row(row: Any) -> MarketListItem:
    return MarketListItem(
        geo_id=row["geo_id"],
        geo_type=row["geo_type"],
        name=row["name"],
        display_name=row["display_name"],
        state_code=row["state_code"],
        state_name=row["state_name"],
        county_fips=row["county_fips"],
        cbsa_code=row["cbsa_code"],
        zcta=row["zcta"],
        country_code=row["country_code"],
        latitude=to_float(row["latitude"]),
        longitude=to_float(row["longitude"]),
    )


def list_markets(
    db: Session,
    *,
    geo_type: str | None,
    state: str | None,
    search: str | None,
    limit: int,
    offset: int,
) -> MarketListResponse:
    filters: list[str] = ["is_active = true"]
    params: dict[str, Any] = {
        "limit": limit,
        "offset": offset,
    }

    if geo_type is not None:
        filters.append("geo_type = :geo_type")
        params["geo_type"] = geo_type

    normalized_state = normalize_state_filter(state)

    if normalized_state is not None:
        filters.append("state_code = :state_code")
        params["state_code"] = normalized_state

    normalized_search = normalize_search_filter(search)

    if normalized_search is not None:
        filters.append(
            """
            (
                name ILIKE :search
                OR display_name ILIKE :search
                OR geo_id ILIKE :search
                OR cbsa_code ILIKE :search
                OR county_fips ILIKE :search
                OR zcta ILIKE :search
            )
            """
        )
        params["search"] = f"%{normalized_search}%"

    where_clause = " AND ".join(filters)

    total_sql = text(
        f"""
        SELECT COUNT(*) AS total
        FROM geo.dim_geo
        WHERE {where_clause}
        """
    )

    items_sql = text(
        f"""
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
        WHERE {where_clause}
        ORDER BY
            CASE geo_type
                WHEN 'national' THEN 1
                WHEN 'state' THEN 2
                WHEN 'metro' THEN 3
                WHEN 'county' THEN 4
                WHEN 'zcta' THEN 5
                ELSE 99
            END,
            state_code NULLS LAST,
            display_name NULLS LAST,
            name,
            geo_id
        LIMIT :limit
        OFFSET :offset
        """
    )

    total = int(db.execute(total_sql, params).scalar_one())
    rows = db.execute(items_sql, params).mappings().all()

    items = [market_identity_from_row(row) for row in rows]

    return MarketListResponse(
        items=items,
        limit=limit,
        offset=offset,
        total=total,
    )


def first_metric_value(row: Any, candidates: list[str]) -> MetricValue:
    for metric_name in candidates:
        value = to_float(row[metric_name])
        if value is not None:
            return MetricValue(metric=metric_name, value=value)

    return MetricValue(metric=None, value=None)


def score_price_momentum(price_growth: float | None) -> float | None:
    if price_growth is None:
        return None

    if price_growth <= -5:
        return 2.0
    if price_growth <= 0:
        return 7.0
    if price_growth <= 3:
        return 16.0
    if price_growth <= 6:
        return 22.0
    if price_growth <= 10:
        return 18.0

    return 10.0


def score_rent_momentum(rent_growth: float | None) -> float | None:
    if rent_growth is None:
        return None

    if rent_growth <= -3:
        return 3.0
    if rent_growth <= 0:
        return 8.0
    if rent_growth <= 3:
        return 17.0
    if rent_growth <= 7:
        return 23.0

    return 18.0


def score_inventory_tightness(
    active_listings_yoy: float | None,
    months_supply: float | None,
) -> float | None:
    if active_listings_yoy is None and months_supply is None:
        return None

    score = 10.0

    if active_listings_yoy is not None:
        if active_listings_yoy <= -10:
            score += 7.0
        elif active_listings_yoy <= 0:
            score += 4.0
        elif active_listings_yoy <= 15:
            score += 1.0
        else:
            score -= 4.0

    if months_supply is not None:
        if months_supply <= 3:
            score += 4.0
        elif months_supply <= 5:
            score += 2.0
        elif months_supply >= 8:
            score -= 4.0

    return max(0.0, min(20.0, score))


def score_affordability(payment_to_income_ratio: float | None) -> float | None:
    if payment_to_income_ratio is None:
        return None

    if payment_to_income_ratio <= 0.25:
        return 15.0
    if payment_to_income_ratio <= 0.32:
        return 11.0
    if payment_to_income_ratio <= 0.40:
        return 7.0
    if payment_to_income_ratio <= 0.50:
        return 4.0

    return 1.0


def score_labor_market(unemployment_rate: float | None) -> float | None:
    if unemployment_rate is None:
        return None

    if unemployment_rate <= 3.5:
        return 15.0
    if unemployment_rate <= 5.0:
        return 12.0
    if unemployment_rate <= 7.0:
        return 7.0
    if unemployment_rate <= 9.0:
        return 4.0

    return 1.0


def classify_inventory_condition(
    active_listings_yoy: float | None,
    months_supply: float | None,
    median_days_on_market: float | None,
) -> str:
    if active_listings_yoy is None and months_supply is None and median_days_on_market is None:
        return "unknown"

    if active_listings_yoy is not None:
        if active_listings_yoy <= -5:
            return "tightening"
        if active_listings_yoy >= 15:
            return "loosening"

    if months_supply is not None:
        if months_supply <= 3:
            return "tight"
        if months_supply >= 7:
            return "loose"

    return "balanced"


def classify_cycle_phase(
    *,
    price_growth: float | None,
    rent_growth: float | None,
    active_listings_yoy: float | None,
    months_supply: float | None,
    composite_score: float | None,
    confidence_score: float,
) -> str:
    if confidence_score < 0.35 or composite_score is None:
        return "Insufficient Data"

    inventory_rising = active_listings_yoy is not None and active_listings_yoy >= 15
    inventory_falling = active_listings_yoy is not None and active_listings_yoy <= -5
    supply_loose = months_supply is not None and months_supply >= 7

    if price_growth is not None and price_growth < 0 and (inventory_rising or supply_loose):
        return "Correction"

    if (
        price_growth is not None
        and rent_growth is not None
        and price_growth <= 3
        and rent_growth > 0
        and inventory_falling
    ):
        return "Recovery"

    if (
        price_growth is not None
        and rent_growth is not None
        and 1 <= price_growth <= 7
        and rent_growth > 1
        and not supply_loose
    ):
        return "Expansion"

    if price_growth is not None and price_growth > 7 and inventory_rising:
        return "Peak"

    if composite_score >= 70:
        return "Expansion"

    if composite_score <= 40:
        return "Correction"

    return "Stabilizing"


def classify_investor_signal(
    *,
    composite_score: float | None,
    confidence_score: float,
    cycle_phase: str,
) -> str:
    if composite_score is None or confidence_score < 0.35:
        return "Insufficient Data"

    if cycle_phase == "Correction" and composite_score < 40:
        return "Avoid Watch"

    if composite_score >= 70 and confidence_score >= 0.6:
        return "Buy Watch"

    if composite_score >= 58 and confidence_score >= 0.5:
        return "Selective Buy"

    if composite_score >= 45:
        return "Hold"

    return "Caution"


def build_score_breakdown(row: Any) -> tuple[ScoreBreakdown, str, str]:
    price_growth_metric = first_metric_value(
        row,
        [
            "zhvi_yoy",
            "median_sale_price_yoy",
            "home_price_index_yoy",
        ],
    )
    rent_growth_metric = first_metric_value(
        row,
        [
            "zori_yoy",
            "median_rent_yoy",
        ],
    )

    active_listings_yoy = to_float(row["active_listings_yoy"])
    months_supply = to_float(row["months_supply"])
    payment_to_income_ratio = to_float(row["payment_to_income_ratio"])
    unemployment_rate = to_float(row["unemployment_rate"])

    price_score = score_price_momentum(price_growth_metric.value)
    rent_score = score_rent_momentum(rent_growth_metric.value)
    inventory_score = score_inventory_tightness(active_listings_yoy, months_supply)
    affordability_score = score_affordability(payment_to_income_ratio)
    labor_score = score_labor_market(unemployment_rate)

    scored_components = [
        price_score,
        rent_score,
        inventory_score,
        affordability_score,
        labor_score,
    ]

    available_components = [score for score in scored_components if score is not None]
    confidence_score = round(len(available_components) / len(scored_components), 2)

    composite_score = (
        round(sum(available_components), 2)
        if available_components
        else None
    )

    cycle_phase = classify_cycle_phase(
        price_growth=price_growth_metric.value,
        rent_growth=rent_growth_metric.value,
        active_listings_yoy=active_listings_yoy,
        months_supply=months_supply,
        composite_score=composite_score,
        confidence_score=confidence_score,
    )

    investor_signal = classify_investor_signal(
        composite_score=composite_score,
        confidence_score=confidence_score,
        cycle_phase=cycle_phase,
    )

    breakdown = ScoreBreakdown(
        composite_cycle_score=composite_score,
        price_momentum=price_score,
        rent_momentum=rent_score,
        inventory_tightness=inventory_score,
        affordability=affordability_score,
        labor_market=labor_score,
        data_completeness=confidence_score,
    )

    return breakdown, cycle_phase, investor_signal


def get_source_freshness(
    db: Session,
    *,
    geo_id: str,
    period_month: Any,
) -> list[SourceFreshnessItem]:
    rows = db.execute(
        text(
            """
            SELECT DISTINCT
                mms.source,
                mms.dataset,
                sf.latest_source_period,
                sf.last_loaded_at,
                sf.last_status,
                sf.is_stale,
                sf.stale_reason,
                sf.record_count
            FROM analytics.market_metric_sources mms
            LEFT JOIN audit.source_freshness sf
                ON sf.source = mms.source
                AND sf.dataset = mms.dataset
            WHERE mms.geo_id = :geo_id
              AND mms.period_month = :period_month
            ORDER BY mms.source, mms.dataset
            """
        ),
        {
            "geo_id": geo_id,
            "period_month": period_month,
        },
    ).mappings().all()

    return [
        SourceFreshnessItem(
            source=row["source"],
            dataset=row["dataset"],
            latest_source_period=to_iso_date(row["latest_source_period"]),
            last_loaded_at=to_iso_datetime(row["last_loaded_at"]),
            last_status=row["last_status"],
            is_stale=row["is_stale"],
            stale_reason=row["stale_reason"],
            record_count=row["record_count"],
        )
        for row in rows
    ]



def get_latest_data_period(db: Session, *, geo_id: str) -> Any:
    return db.execute(
        text(
            """
            SELECT MAX(period_month) AS latest_data_period
            FROM analytics.market_monthly_metrics
            WHERE geo_id = :geo_id
            """
        ),
        {"geo_id": geo_id},
    ).scalar_one()


def get_latest_scoreable_metric_row(db: Session, *, geo_id: str) -> Any:
    return db.execute(
        text(
            """
            SELECT
                m.geo_id,
                m.period_month,

                m.home_price_index_yoy,
                m.zhvi_yoy,
                m.median_sale_price_yoy,

                m.zori_yoy,
                m.median_rent_yoy,

                m.active_listings_yoy,
                m.months_supply,
                m.median_days_on_market,

                m.payment_to_income_ratio,
                m.price_to_income_ratio,

                COALESCE(m.unemployment_rate, labor_asof.unemployment_rate) AS unemployment_rate,

                m.source_flags,
                m.quality_flags
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
              AND (
                    m.home_price_index_yoy IS NOT NULL
                 OR m.zhvi_yoy IS NOT NULL
                 OR m.median_sale_price_yoy IS NOT NULL
                 OR m.zori_yoy IS NOT NULL
                 OR m.median_rent_yoy IS NOT NULL
                 OR m.active_listings_yoy IS NOT NULL
                 OR m.months_supply IS NOT NULL
                 OR m.median_days_on_market IS NOT NULL
                 OR m.payment_to_income_ratio IS NOT NULL
                 OR m.price_to_income_ratio IS NOT NULL
                 OR m.unemployment_rate IS NOT NULL
                 OR labor_asof.unemployment_rate IS NOT NULL
              )
            ORDER BY m.period_month DESC
            LIMIT 1
            """
        ),
        {"geo_id": geo_id},
    ).mappings().one_or_none()


def get_market_detail(db: Session, *, geo_id: str) -> MarketDetailResponse:
    market_row = db.execute(
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
            WHERE geo_id = :geo_id
              AND is_active = true
            """
        ),
        {"geo_id": geo_id},
    ).mappings().one_or_none()

    if market_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Market '{geo_id}' was not found.",
        )

    latest_data_period = get_latest_data_period(db, geo_id=geo_id)
    metric_row = get_latest_scoreable_metric_row(db, geo_id=geo_id)

    market = market_identity_from_row(market_row)

    if metric_row is None:
        return MarketDetailResponse(
            market=market,
            latest_period=None,
            latest_data_period=to_iso_date(latest_data_period),
            data_status=(
                "no_metrics"
                if latest_data_period is None
                else "no_scoreable_period"
            ),
            cycle_phase="Insufficient Data",
            confidence_score=0.0,
            investor_signal="Insufficient Data",
            price_growth=MetricValue(metric=None, value=None),
            rent_growth=MetricValue(metric=None, value=None),
            inventory_condition=InventoryCondition(
                active_listings_yoy=None,
                months_supply=None,
                median_days_on_market=None,
                condition="unknown",
            ),
            score_breakdown=ScoreBreakdown(
                composite_cycle_score=None,
                price_momentum=None,
                rent_momentum=None,
                inventory_tightness=None,
                affordability=None,
                labor_market=None,
                data_completeness=0.0,
            ),
            source_freshness=[],
            quality_flags={"missing_latest_metrics": True},
            source_flags={},
        )

    price_growth = first_metric_value(
        metric_row,
        [
            "zhvi_yoy",
            "median_sale_price_yoy",
            "home_price_index_yoy",
        ],
    )

    rent_growth = first_metric_value(
        metric_row,
        [
            "zori_yoy",
            "median_rent_yoy",
        ],
    )

    active_listings_yoy = to_float(metric_row["active_listings_yoy"])
    months_supply = to_float(metric_row["months_supply"])
    median_days_on_market = to_float(metric_row["median_days_on_market"])

    inventory_condition = InventoryCondition(
        active_listings_yoy=active_listings_yoy,
        months_supply=months_supply,
        median_days_on_market=median_days_on_market,
        condition=classify_inventory_condition(
            active_listings_yoy,
            months_supply,
            median_days_on_market,
        ),
    )

    score_breakdown, cycle_phase, investor_signal = build_score_breakdown(metric_row)

    source_freshness = get_source_freshness(
        db,
        geo_id=geo_id,
        period_month=metric_row["period_month"],
    )

    data_status = (
    "latest_period_scoreable"
    if latest_data_period == metric_row["period_month"]
    else "using_latest_scoreable_period"
)

    return MarketDetailResponse(
        market=market,
        latest_period=to_iso_date(metric_row["period_month"]),
        latest_data_period=to_iso_date(latest_data_period),
        data_status=data_status,
        cycle_phase=cycle_phase,
        confidence_score=score_breakdown.data_completeness,
        investor_signal=investor_signal,
        price_growth=price_growth,
        rent_growth=rent_growth,
        inventory_condition=inventory_condition,
        score_breakdown=score_breakdown,
        source_freshness=source_freshness,
        quality_flags=metric_row["quality_flags"] or {},
        source_flags=metric_row["source_flags"] or {},
    )


TIMESERIES_DIRECT_METRIC_COLUMNS = DIRECT_METRIC_COLUMNS
TIMESERIES_DERIVED_METRICS = DERIVED_METRICS
TIMESERIES_SUPPORTED_METRICS = SUPPORTED_METRICS


def parse_timeseries_metrics(metrics: str) -> list[str]:
    parsed = [
        metric.strip()
        for metric in metrics.split(",")
        if metric.strip()
    ]

    if not parsed:
        raise ApiError(
            status_code=422,
            code="missing_metrics",
            message="At least one metric is required.",
        )

    deduped = list(dict.fromkeys(parsed))

    unsupported = [
        metric
        for metric in deduped
        if metric not in TIMESERIES_SUPPORTED_METRICS
    ]

    if unsupported:
        raise ApiError(
            status_code=422,
            code="unsupported_metric",
            message="Unsupported timeseries metrics requested.",
            details={
                "unsupported_metrics": unsupported,
                "supported_metrics": sorted(TIMESERIES_SUPPORTED_METRICS),
            },
        )

    return deduped


def get_timeseries_metric_value(row: Any, metric: str) -> float | None:
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

    column_name = TIMESERIES_DIRECT_METRIC_COLUMNS[metric]
    return to_float(row[column_name])


def get_market_timeseries(
    db: Session,
    *,
    geo_id: str,
    metrics: str,
    start_date: Any | None,
    end_date: Any | None,
) -> MarketTimeSeriesResponse:
    selected_metrics = parse_timeseries_metrics(metrics)

    market_row = db.execute(
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
            WHERE geo_id = :geo_id
              AND is_active = true
            """
        ),
        {"geo_id": geo_id},
    ).mappings().one_or_none()

    if market_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"Market '{geo_id}' was not found.",
        )

    latest_period = db.execute(
        text(
            """
            SELECT MAX(period_month) AS latest_period
            FROM analytics.market_monthly_metrics
            WHERE geo_id = :geo_id
            """
        ),
        {"geo_id": geo_id},
    ).scalar_one()

    resolved_start_date, resolved_end_date, date_window_source = resolve_date_window(
        start_date=start_date,
        end_date=end_date,
        latest_period=latest_period,
    )

    filters = ["geo_id = :geo_id"]
    params: dict[str, Any] = {"geo_id": geo_id}

    if resolved_start_date is not None:
        filters.append("period_month >= :start_date")
        params["start_date"] = resolved_start_date

    if resolved_end_date is not None:
        filters.append("period_month <= :end_date")
        params["end_date"] = resolved_end_date

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
            ORDER BY period_month ASC
            """
        ),
        params,
    ).mappings().all()

    items: list[MarketTimeSeriesPoint] = []

    for row in rows:
        values = {
            metric: get_timeseries_metric_value(row, metric)
            for metric in selected_metrics
        }

        missing_metrics = [
            metric
            for metric, value in values.items()
            if value is None
        ]

        items.append(
            MarketTimeSeriesPoint(
                period_month=to_iso_date(row["period_month"]),
                values=values,
                missing_metrics=missing_metrics,
            )
        )

    return MarketTimeSeriesResponse(
        market=market_identity_from_row(market_row),
        metrics=selected_metrics,
        start_date=to_iso_date(resolved_start_date),
        end_date=to_iso_date(resolved_end_date),
        date_window_source=date_window_source,
        items=items,
    )
