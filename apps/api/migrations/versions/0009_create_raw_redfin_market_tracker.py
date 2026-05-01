"""create raw redfin market tracker

Revision ID: 0009_raw_redfin
Revises: 0008_raw_zillow
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0009_raw_redfin"
down_revision: str | None = "0008_raw_zillow"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS pending_sales NUMERIC(18, 2);

        CREATE TABLE IF NOT EXISTS raw.redfin_market_tracker (
            id BIGSERIAL PRIMARY KEY,
            source_region_id TEXT,
            region_name TEXT NOT NULL,
            region_type TEXT,
            state_code TEXT,
            property_type TEXT,
            period_month DATE NOT NULL,
            median_sale_price NUMERIC(18, 6),
            homes_sold NUMERIC(18, 6),
            pending_sales NUMERIC(18, 6),
            new_listings NUMERIC(18, 6),
            active_listings NUMERIC(18, 6),
            months_supply NUMERIC(18, 6),
            median_days_on_market NUMERIC(18, 6),
            sale_to_list_ratio NUMERIC(18, 6),
            price_drops_pct NUMERIC(18, 6),
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_redfin_market_tracker_region_period_property_load
                UNIQUE (
                    region_name,
                    region_type,
                    property_type,
                    period_month,
                    load_date
                )
        );

        CREATE INDEX IF NOT EXISTS ix_raw_redfin_market_tracker_period
            ON raw.redfin_market_tracker (period_month);

        CREATE INDEX IF NOT EXISTS ix_raw_redfin_market_tracker_region
            ON raw.redfin_market_tracker (region_type, region_name);

        CREATE INDEX IF NOT EXISTS ix_raw_redfin_market_tracker_property
            ON raw.redfin_market_tracker (property_type);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_redfin_market_tracker_property;
        DROP INDEX IF EXISTS raw.ix_raw_redfin_market_tracker_region;
        DROP INDEX IF EXISTS raw.ix_raw_redfin_market_tracker_period;
        DROP TABLE IF EXISTS raw.redfin_market_tracker;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS pending_sales;
        """
    )
