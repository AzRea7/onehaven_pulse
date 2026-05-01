"""create raw bls laus

Revision ID: 0011_raw_bls_laus
Revises: 0010_raw_census_acs
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0011_raw_bls_laus"
down_revision: str | None = "0010_raw_census_acs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS labor_force NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS employment NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS unemployment_count NUMERIC(18, 2);

        CREATE TABLE IF NOT EXISTS raw.bls_laus_observations (
            id BIGSERIAL PRIMARY KEY,
            series_id TEXT NOT NULL,
            geography_level TEXT NOT NULL,
            measure TEXT NOT NULL,
            geo_reference TEXT NOT NULL,
            year INTEGER NOT NULL,
            period TEXT NOT NULL,
            period_month DATE NOT NULL,
            value NUMERIC(18, 6),
            footnotes JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_bls_laus_series_period_load
                UNIQUE (
                    series_id,
                    year,
                    period,
                    load_date
                )
        );

        CREATE INDEX IF NOT EXISTS ix_raw_bls_laus_observations_period
            ON raw.bls_laus_observations (period_month);

        CREATE INDEX IF NOT EXISTS ix_raw_bls_laus_observations_geo_measure
            ON raw.bls_laus_observations (geography_level, geo_reference, measure);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_bls_laus_observations_geo_measure;
        DROP INDEX IF EXISTS raw.ix_raw_bls_laus_observations_period;
        DROP TABLE IF EXISTS raw.bls_laus_observations;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS unemployment_count;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS employment;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS labor_force;
        """
    )
