from __future__ import annotations

import os

from sqlalchemy import create_engine, text


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)

FEATURE_COLUMNS = {
    "price_growth_1m",
    "price_growth_3m",
    "price_growth_12m",
    "rent_growth_1m",
    "rent_growth_3m",
    "rent_growth_12m",
    "inventory_change_3m",
    "inventory_change_12m",
    "days_on_market_change_3m",
    "mortgage_rate_30y",
    "rate_change_3m",
    "unemployment_rate",
    "unemployment_change_3m",
    "price_to_income_ratio",
    "payment_to_income_ratio",
    "affordability_score",
    "cycle_score",
    "cycle_phase_encoded",
    "confidence_score",
}

TARGET_COLUMNS = {
    "target_price_growth_12m",
    "target_rent_growth_12m",
    "target_drawdown_12m",
    "target_cycle_phase_12m",
}

FORBIDDEN_FEATURE_NAMES = FEATURE_COLUMNS & TARGET_COLUMNS


def main() -> int:
    assert not FORBIDDEN_FEATURE_NAMES, f"Targets overlap features: {FORBIDDEN_FEATURE_NAMES}"

    engine = create_engine(DATABASE_URL, pool_pre_ping=True)

    with engine.connect() as connection:
        row_count = connection.execute(
            text("SELECT COUNT(*) FROM analytics.market_features_monthly")
        ).scalar_one()

        assert row_count > 0, "market_features_monthly has no rows"

        duplicate_count = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT geo_id, period_month, feature_version, COUNT(*) AS rows
                    FROM analytics.market_features_monthly
                    GROUP BY geo_id, period_month, feature_version
                    HAVING COUNT(*) > 1
                ) duplicates
                """
            )
        ).scalar_one()

        assert duplicate_count == 0, f"Duplicate feature keys found: {duplicate_count}"

        unsafe_rows = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE quality_flags ->> 'point_in_time_safe' <> 'true'
                   OR quality_flags ->> 'target_columns_separated' <> 'true'
                """
            )
        ).scalar_one()

        assert unsafe_rows == 0, f"Feature rows missing safety flags: {unsafe_rows}"

        target_without_flag = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE (
                    target_price_growth_12m IS NOT NULL
                    OR target_rent_growth_12m IS NOT NULL
                    OR target_drawdown_12m IS NOT NULL
                    OR target_cycle_phase_12m IS NOT NULL
                )
                AND target_available = false
                """
            )
        ).scalar_one()

        assert target_without_flag == 0, f"Rows have targets but target_available=false: {target_without_flag}"

        trainable_without_target = connection.execute(
            text(
                """
                SELECT COUNT(*)
                FROM analytics.market_features_monthly
                WHERE is_trainable = true
                  AND target_price_growth_12m IS NULL
                """
            )
        ).scalar_one()

        assert trainable_without_target == 0, f"Trainable rows missing target_price_growth_12m: {trainable_without_target}"

    print("Market feature no-leakage validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
