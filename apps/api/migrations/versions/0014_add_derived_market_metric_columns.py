"""add derived market metric columns

Revision ID: 0014_derived_market_metrics
Revises: 0013_raw_fema_nri
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0014_derived_market_metrics"
down_revision: str | None = "0013_raw_fema_nri"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS estimated_monthly_payment NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS payment_to_income_ratio NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS price_to_income_ratio NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS rent_to_price_ratio NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS real_home_price_index NUMERIC(18, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS permits_per_1000_people NUMERIC(18, 6);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS permits_per_1000_people;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS real_home_price_index;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS rent_to_price_ratio;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS price_to_income_ratio;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS payment_to_income_ratio;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS estimated_monthly_payment;
        """
    )
