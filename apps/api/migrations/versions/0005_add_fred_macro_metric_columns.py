"""add fred macro metric columns

Revision ID: 0005_fred_macro_cols
Revises: 0004_market_metrics
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0005_fred_macro_cols"
down_revision: str | None = "0004_market_metrics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS treasury_2yr_rate NUMERIC(8, 4);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS treasury_5yr_rate NUMERIC(8, 4);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS treasury_10yr_rate NUMERIC(8, 4);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS treasury_30yr_rate NUMERIC(8, 4);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS treasury_10yr_2yr_spread NUMERIC(8, 4);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS treasury_10yr_3mo_spread NUMERIC(8, 4);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS recession_indicator NUMERIC(1, 0);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS recession_indicator;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS treasury_10yr_3mo_spread;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS treasury_10yr_2yr_spread;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS treasury_30yr_rate;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS treasury_10yr_rate;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS treasury_5yr_rate;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS treasury_2yr_rate;
        """
    )
