from __future__ import annotations

import os
from decimal import Decimal

from sqlalchemy import create_engine, text


DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://onehaven:onehaven_dev_password@localhost:5432/onehaven_market",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

SOURCE = "derived"
DATASET = "affordability"


def main() -> int:
    sql = text(
        """
        WITH months AS (
            SELECT DISTINCT period_month
            FROM analytics.market_monthly_metrics
        ),
        income_asof AS (
            SELECT
                base.geo_id,
                base.period_month,
                income.median_household_income,
                income.period_month AS income_source_period
            FROM analytics.market_monthly_metrics base
            LEFT JOIN LATERAL (
                SELECT
                    i.median_household_income,
                    i.period_month
                FROM analytics.market_monthly_metrics i
                WHERE i.geo_id = base.geo_id
                  AND i.median_household_income IS NOT NULL
                  AND i.period_month <= base.period_month
                ORDER BY i.period_month DESC
                LIMIT 1
            ) income ON true
        ),
        national_rates AS (
            SELECT
                period_month,
                mortgage_rate_30y
            FROM analytics.market_monthly_metrics
            WHERE geo_id = 'us'
              AND mortgage_rate_30y IS NOT NULL
        ),
        candidates AS (
            SELECT
                m.geo_id,
                m.period_month,
                COALESCE(m.zhvi, m.median_sale_price, m.home_price_index) AS price_value,
                i.median_household_income,
                r.mortgage_rate_30y,
                i.income_source_period
            FROM analytics.market_monthly_metrics m
            JOIN income_asof i
              ON i.geo_id = m.geo_id
             AND i.period_month = m.period_month
            JOIN national_rates r
              ON r.period_month = m.period_month
            WHERE COALESCE(m.zhvi, m.median_sale_price) IS NOT NULL
              AND i.median_household_income IS NOT NULL
              AND i.median_household_income > 0
              AND r.mortgage_rate_30y IS NOT NULL
        ),
        computed AS (
            SELECT
                geo_id,
                period_month,
                price_value,
                median_household_income,
                mortgage_rate_30y,
                income_source_period,
                (price_value / NULLIF(median_household_income, 0))::numeric(14, 6) AS price_to_income_ratio,
                CASE
                    WHEN mortgage_rate_30y <= 0 THEN NULL
                    ELSE (
                        price_value * 0.80 *
                        ((mortgage_rate_30y / 100.0 / 12.0) * POWER(1 + (mortgage_rate_30y / 100.0 / 12.0), 360))
                        / NULLIF(POWER(1 + (mortgage_rate_30y / 100.0 / 12.0), 360) - 1, 0)
                    )::numeric(14, 4)
                END AS estimated_monthly_payment
            FROM candidates
        ),
        final AS (
            SELECT
                geo_id,
                period_month,
                price_to_income_ratio,
                estimated_monthly_payment,
                ((estimated_monthly_payment * 12.0) / NULLIF(median_household_income, 0))::numeric(14, 6)
                    AS payment_to_income_ratio,
                jsonb_build_object(
                    'source', 'derived',
                    'dataset', 'affordability',
                    'inputs', jsonb_build_object(
                        'price_value', price_value,
                        'median_household_income', median_household_income,
                        'mortgage_rate_30y', mortgage_rate_30y,
                        'income_source_period', income_source_period
                    ),
                    'formula', 'price_to_income=price/income; payment=80pct_LTV_30yr_fixed; payment_to_income=annual_payment/income'
                ) AS derived_source_flag
            FROM computed
            WHERE estimated_monthly_payment IS NOT NULL
        )
        UPDATE analytics.market_monthly_metrics m
        SET
            price_to_income_ratio = f.price_to_income_ratio,
            estimated_monthly_payment = f.estimated_monthly_payment,
            payment_to_income_ratio = f.payment_to_income_ratio,
            source_flags = (
                COALESCE(m.source_flags::jsonb, '{}'::jsonb)
                || jsonb_build_object(
                    'price_to_income_ratio', f.derived_source_flag,
                    'estimated_monthly_payment', f.derived_source_flag,
                    'payment_to_income_ratio', f.derived_source_flag
                )
            )::json,
            updated_at = now()
        FROM final f
        WHERE m.geo_id = f.geo_id
          AND m.period_month = f.period_month
        RETURNING m.geo_id, m.period_month;
        """
    )

    with engine.begin() as connection:
        updated = connection.execute(sql).rowcount or 0

    print(f"Backfilled affordability metric rows: {updated}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
