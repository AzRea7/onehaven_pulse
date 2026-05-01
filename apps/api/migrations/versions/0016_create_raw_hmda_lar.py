"""create raw hmda lar

Revision ID: 0016_raw_hmda_lar
Revises: 0015_hud_usps_crosswalk
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0016_raw_hmda_lar"
down_revision: str | None = "0015_hud_usps_crosswalk"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS hmda_applications NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS hmda_originations NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS hmda_denials NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS hmda_denial_rate NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS hmda_median_loan_amount NUMERIC(18, 2);

        CREATE TABLE IF NOT EXISTS raw.hmda_modified_lar (
            id BIGSERIAL PRIMARY KEY,
            activity_year INTEGER NOT NULL,
            state_code TEXT,
            county_code TEXT,
            census_tract TEXT,
            lei TEXT,
            action_taken TEXT,
            loan_purpose TEXT,
            loan_type TEXT,
            lien_status TEXT,
            loan_amount NUMERIC(18, 2),
            income NUMERIC(18, 2),
            source_payload JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS ix_raw_hmda_year_state
            ON raw.hmda_modified_lar (activity_year, state_code);

        CREATE INDEX IF NOT EXISTS ix_raw_hmda_county
            ON raw.hmda_modified_lar (county_code);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_hmda_county;
        DROP INDEX IF EXISTS raw.ix_raw_hmda_year_state;
        DROP TABLE IF EXISTS raw.hmda_modified_lar;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS hmda_median_loan_amount;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS hmda_denial_rate;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS hmda_denials;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS hmda_originations;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS hmda_applications;
        """
    )
