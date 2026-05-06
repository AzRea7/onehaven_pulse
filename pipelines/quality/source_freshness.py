from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import text

from pipelines.common.db import engine


@dataclass(frozen=True)
class FreshnessRule:
    source: str
    dataset: str
    expected_frequency: str
    freshness_threshold_days: int


DEFAULT_FRESHNESS_RULES: tuple[FreshnessRule, ...] = (
    FreshnessRule("fred", "macro_series", "weekly", 14),
    FreshnessRule("fhfa", "hpi", "quarterly", 120),
    FreshnessRule("zillow", "zhvi", "monthly", 45),
    FreshnessRule("zillow", "zori", "monthly", 45),
    FreshnessRule("redfin", "market_tracker", "monthly", 45),
    FreshnessRule("census", "geography", "annual", 450),
    FreshnessRule("census_acs", "profile", "annual", 450),
    FreshnessRule("bls_laus", "labor_market", "monthly", 45),
    FreshnessRule("census_bps", "building_permits", "monthly", 75),
    FreshnessRule("fema_nri", "county_risk", "annual", 450),
    FreshnessRule("hmda", "lar", "annual", 450),
    FreshnessRule("overture_maps", "places", "monthly", 45),
)


UPSERT_RULE_SQL = text(
    """
    INSERT INTO audit.source_freshness (
        source,
        dataset,
        expected_frequency,
        freshness_threshold_days,
        last_status,
        is_stale,
        stale_reason
    )
    VALUES (
        :source,
        :dataset,
        :expected_frequency,
        :freshness_threshold_days,
        'pending',
        TRUE,
        'Dataset configured but not loaded yet'
    )
    ON CONFLICT (source, dataset)
    DO UPDATE SET
        expected_frequency = EXCLUDED.expected_frequency,
        freshness_threshold_days = EXCLUDED.freshness_threshold_days,
        updated_at = NOW()
    """
)


UPSERT_FRESHNESS_SQL = text(
    """
    INSERT INTO audit.source_freshness (
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
    )
    VALUES (
        :source,
        :dataset,
        :expected_frequency,
        :freshness_threshold_days,
        :latest_source_period,
        :last_loaded_at,
        :last_successful_run_id,
        :last_status,
        CASE
            WHEN :last_status = 'failed' THEN TRUE
            WHEN :latest_source_period IS NULL THEN TRUE
            WHEN :latest_source_period < (CURRENT_DATE - (:freshness_threshold_days || ' days')::interval)::date THEN TRUE
            ELSE FALSE
        END,
        CASE
            WHEN :last_status = 'failed' THEN COALESCE(:error_message, 'Latest job failed')
            WHEN :latest_source_period IS NULL THEN 'Latest source period is unknown'
            WHEN :latest_source_period < (CURRENT_DATE - (:freshness_threshold_days || ' days')::interval)::date
                THEN 'Latest source period is older than freshness threshold'
            ELSE NULL
        END,
        :record_count,
        :error_message,
        NOW()
    )
    ON CONFLICT (source, dataset)
    DO UPDATE SET
        expected_frequency = EXCLUDED.expected_frequency,
        freshness_threshold_days = EXCLUDED.freshness_threshold_days,
        latest_source_period = EXCLUDED.latest_source_period,
        last_loaded_at = EXCLUDED.last_loaded_at,
        last_successful_run_id = CASE
            WHEN EXCLUDED.last_status = 'success' THEN EXCLUDED.last_successful_run_id
            ELSE audit.source_freshness.last_successful_run_id
        END,
        last_status = EXCLUDED.last_status,
        is_stale = EXCLUDED.is_stale,
        stale_reason = EXCLUDED.stale_reason,
        record_count = EXCLUDED.record_count,
        error_message = EXCLUDED.error_message,
        updated_at = NOW()
    """
)


def rule_for(source: str, dataset: str) -> FreshnessRule:
    for rule in DEFAULT_FRESHNESS_RULES:
        if rule.source == source and rule.dataset == dataset:
            return rule

    return FreshnessRule(
        source=source,
        dataset=dataset,
        expected_frequency="manual",
        freshness_threshold_days=90,
    )


def seed_freshness_rules() -> int:
    with engine.begin() as connection:
        for rule in DEFAULT_FRESHNESS_RULES:
            connection.execute(
                UPSERT_RULE_SQL,
                {
                    "source": rule.source,
                    "dataset": rule.dataset,
                    "expected_frequency": rule.expected_frequency,
                    "freshness_threshold_days": rule.freshness_threshold_days,
                },
            )

    return len(DEFAULT_FRESHNESS_RULES)


def update_source_freshness(
    *,
    source: str,
    dataset: str,
    latest_source_period: date | None,
    last_loaded_at: datetime,
    status: str,
    record_count: int | None = None,
    error_message: str | None = None,
    pipeline_run_id: str | None = None,
) -> None:
    rule = rule_for(source, dataset)

    with engine.begin() as connection:
        connection.execute(
            UPSERT_FRESHNESS_SQL,
            {
                "source": source,
                "dataset": dataset,
                "expected_frequency": rule.expected_frequency,
                "freshness_threshold_days": rule.freshness_threshold_days,
                "latest_source_period": latest_source_period,
                "last_loaded_at": last_loaded_at,
                "last_successful_run_id": pipeline_run_id if status == "success" else None,
                "last_status": status,
                "record_count": record_count,
                "error_message": error_message,
            },
        )
