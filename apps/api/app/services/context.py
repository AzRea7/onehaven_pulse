from datetime import date

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.context import (
    ContextRisk,
    MarketContextEvidence,
    MarketContextMcpMetadata,
    MarketContextResponse,
)
from app.services.coverage import get_market_coverage
from app.services.markets import (
    get_market_detail,
    to_float,
)


def classify_inventory_trend(
    *,
    active_listings_yoy: float | None,
    months_supply: float | None,
    median_days_on_market: float | None,
) -> str:
    if active_listings_yoy is None and months_supply is None and median_days_on_market is None:
        return "unknown"

    if active_listings_yoy is not None:
        if active_listings_yoy <= -5:
            return "falling"
        if active_listings_yoy >= 15:
            return "rising"

    if months_supply is not None:
        if months_supply <= 3:
            return "tight"
        if months_supply >= 7:
            return "loose"

    return "stable"


def classify_affordability(
    *,
    payment_to_income_ratio: float | None,
    price_to_income_ratio: float | None,
) -> str:
    if payment_to_income_ratio is None and price_to_income_ratio is None:
        return "unknown"

    if payment_to_income_ratio is not None:
        if payment_to_income_ratio <= 0.28:
            return "favorable"
        if payment_to_income_ratio <= 0.36:
            return "neutral"
        return "strained"

    if price_to_income_ratio is not None:
        if price_to_income_ratio <= 3.5:
            return "favorable"
        if price_to_income_ratio <= 5.0:
            return "neutral"
        return "strained"

    return "unknown"


def get_latest_context_metric_row(
    db: Session,
    *,
    geo_id: str,
    latest_period: str | None,
    latest_data_period: str | None,
):
    selected_period = latest_period or latest_data_period

    if selected_period is None:
        return None

    return db.execute(
        text(
            """
            SELECT
                m.geo_id,
                m.period_month,

                m.active_listings_yoy,
                m.months_supply,
                m.median_days_on_market,

                m.payment_to_income_ratio,
                m.price_to_income_ratio,

                COALESCE(m.unemployment_rate, labor_asof.unemployment_rate) AS unemployment_rate,
                labor_asof.period_month AS unemployment_rate_period,

                m.building_permits
            FROM analytics.market_monthly_metrics m
            LEFT JOIN LATERAL (
                SELECT
                    m2.period_month,
                    m2.unemployment_rate
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
            LIMIT 1
            """
        ),
        {
            "geo_id": geo_id,
            "period_month": selected_period,
        },
    ).mappings().one_or_none()


def build_context_risks(
    *,
    data_status: str,
    coverage: dict[str, bool],
    confidence_score: float,
    source_freshness: list[dict],
) -> list[ContextRisk]:
    risks: list[ContextRisk] = []

    if data_status == "no_metrics":
        risks.append(
            ContextRisk(
                code="no_metrics",
                severity="high",
                message="No market metrics are available for this market.",
            )
        )

    if data_status == "no_scoreable_period":
        risks.append(
            ContextRisk(
                code="missing_scoreable_metrics",
                severity="high",
                message="No scoreable market-cycle metrics are available for this market.",
            )
        )

    if data_status == "using_prior_scoreable_period":
        risks.append(
            ContextRisk(
                code="latest_data_not_scoreable",
                severity="medium",
                message="The latest data period exists, but the most recent scoreable period is older.",
            )
        )

    if not coverage.get("price", False):
        risks.append(
            ContextRisk(
                code="missing_price_metrics",
                severity="high",
                message="Price growth metrics are missing for this market.",
            )
        )

    if not coverage.get("rent", False):
        risks.append(
            ContextRisk(
                code="missing_rent_metrics",
                severity="high",
                message="Rent growth metrics are missing for this market.",
            )
        )

    if not coverage.get("inventory", False):
        risks.append(
            ContextRisk(
                code="missing_inventory_metrics",
                severity="medium",
                message="Inventory metrics are missing for this market.",
            )
        )

    if not coverage.get("affordability", False):
        risks.append(
            ContextRisk(
                code="missing_affordability_metrics",
                severity="medium",
                message="Affordability metrics are missing for this market.",
            )
        )

    if not coverage.get("labor", False):
        risks.append(
            ContextRisk(
                code="missing_labor_metrics",
                severity="medium",
                message="Labor market metrics are missing for this market.",
            )
        )

    if confidence_score < 0.5:
        risks.append(
            ContextRisk(
                code="low_confidence_score",
                severity="medium",
                message="Market context confidence is below 0.50.",
            )
        )

    stale_sources = [
        item
        for item in source_freshness
        if item.get("is_stale") is True
    ]

    if stale_sources:
        risks.append(
            ContextRisk(
                code="stale_sources",
                severity="medium",
                message="One or more source datasets are stale.",
            )
        )

    # Deduplicate while preserving order.
    seen: set[str] = set()
    deduped: list[ContextRisk] = []

    for risk in risks:
        if risk.code not in seen:
            seen.add(risk.code)
            deduped.append(risk)

    return deduped


def get_market_context(db: Session, *, geo_id: str) -> MarketContextResponse:
    detail = get_market_detail(db, geo_id=geo_id)

    context_period = detail.latest_period or detail.latest_data_period
    coverage_period = date.fromisoformat(context_period) if context_period else None

    coverage = get_market_coverage(
        db,
        geo_id=geo_id,
        period_month=coverage_period,
    )

    metric_row = get_latest_context_metric_row(
        db,
        geo_id=geo_id,
        latest_period=detail.latest_period,
        latest_data_period=detail.latest_data_period,
    )

    active_listings_yoy = (
        to_float(metric_row["active_listings_yoy"])
        if metric_row is not None
        else None
    )
    months_supply = (
        to_float(metric_row["months_supply"])
        if metric_row is not None
        else None
    )
    median_days_on_market = (
        to_float(metric_row["median_days_on_market"])
        if metric_row is not None
        else None
    )
    payment_to_income_ratio = (
        to_float(metric_row["payment_to_income_ratio"])
        if metric_row is not None
        else None
    )
    price_to_income_ratio = (
        to_float(metric_row["price_to_income_ratio"])
        if metric_row is not None
        else None
    )
    unemployment_rate = (
        to_float(metric_row["unemployment_rate"])
        if metric_row is not None
        else None
    )
    building_permits = (
        to_float(metric_row["building_permits"])
        if metric_row is not None
        else None
    )

    source_freshness = [
        item.model_dump()
        for item in detail.source_freshness
    ]

    risks = build_context_risks(
        data_status=detail.data_status,
        coverage=coverage.coverage,
        confidence_score=detail.confidence_score,
        source_freshness=source_freshness,
    )

    market_name = (
        detail.market.display_name
        or detail.market.name
        or detail.market.geo_id
    )

    evidence = MarketContextEvidence(
        price_growth_yoy=detail.price_growth.value,
        price_growth_metric=detail.price_growth.metric,
        rent_growth_yoy=detail.rent_growth.value,
        rent_growth_metric=detail.rent_growth.metric,
        inventory_trend=classify_inventory_trend(
            active_listings_yoy=active_listings_yoy,
            months_supply=months_supply,
            median_days_on_market=median_days_on_market,
        ),
        active_listings_yoy=active_listings_yoy,
        months_supply=months_supply,
        median_days_on_market=median_days_on_market,
        affordability=classify_affordability(
            payment_to_income_ratio=payment_to_income_ratio,
            price_to_income_ratio=price_to_income_ratio,
        ),
        payment_to_income_ratio=payment_to_income_ratio,
        price_to_income_ratio=price_to_income_ratio,
        unemployment_rate=unemployment_rate,
        building_permits=building_permits,
        composite_cycle_score=detail.score_breakdown.composite_cycle_score,
    )

    return MarketContextResponse(
        geo_id=detail.market.geo_id,
        market=market_name,
        geo_type=detail.market.geo_type,
        latest_period=detail.latest_period,
        latest_data_period=detail.latest_data_period,
        data_status=detail.data_status,
        cycle_phase=detail.cycle_phase,
        investor_signal=detail.investor_signal,
        confidence_score=detail.confidence_score,
        evidence=evidence,
        score_breakdown=detail.score_breakdown.model_dump(),
        coverage=coverage.coverage,
        risks=risks,
        data_quality={
            "available_metrics": coverage.available_metrics,
            "missing_score_inputs": coverage.missing_score_inputs,
            "quality_flags": detail.quality_flags,
            "source_flags": detail.source_flags,
        },
        source_freshness=source_freshness,
        mcp=MarketContextMcpMetadata(
            resource_id=detail.market.geo_id,
        ),
    )
