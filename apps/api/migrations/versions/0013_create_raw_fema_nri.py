"""create raw fema nri

Revision ID: 0013_raw_fema_nri
Revises: 0012_census_bps
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0013_raw_fema_nri"
down_revision: str | None = "0012_census_bps"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS hazard_risk_score NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS expected_annual_loss NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS social_vulnerability_score NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS community_resilience_score NUMERIC(12, 6);

        CREATE TABLE IF NOT EXISTS raw.fema_nri_county_risk (
            id BIGSERIAL PRIMARY KEY,
            county_fips TEXT NOT NULL,
            county_name TEXT,
            state_name TEXT,
            state_code TEXT,
            source_year INTEGER,
            release_label TEXT,
            risk_score NUMERIC(18, 6),
            risk_rating TEXT,
            expected_annual_loss NUMERIC(18, 6),
            expected_annual_loss_score NUMERIC(18, 6),
            expected_annual_loss_rating TEXT,
            social_vulnerability_score NUMERIC(18, 6),
            social_vulnerability_rating TEXT,
            community_resilience_score NUMERIC(18, 6),
            community_resilience_rating TEXT,
            source_payload JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_fema_nri_county_load
                UNIQUE (
                    county_fips,
                    release_label,
                    load_date
                )
        );

        CREATE INDEX IF NOT EXISTS ix_raw_fema_nri_county_fips
            ON raw.fema_nri_county_risk (county_fips);

        CREATE INDEX IF NOT EXISTS ix_raw_fema_nri_release
            ON raw.fema_nri_county_risk (release_label);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_fema_nri_release;
        DROP INDEX IF EXISTS raw.ix_raw_fema_nri_county_fips;
        DROP TABLE IF EXISTS raw.fema_nri_county_risk;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS community_resilience_score;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS social_vulnerability_score;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS expected_annual_loss;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS hazard_risk_score;
        """
    )
