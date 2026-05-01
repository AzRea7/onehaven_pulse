"""create raw census bps

Revision ID: 0012_census_bps
Revises: 0012_raw_permits
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0012_census_bps"
down_revision: str | None = "0012_raw_permits"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS building_permits NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS single_family_permits NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS multi_family_permits NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS permit_units NUMERIC(18, 2);

        CREATE TABLE IF NOT EXISTS raw.census_building_permits (
            id BIGSERIAL PRIMARY KEY,
            geography_level TEXT NOT NULL,
            period_type TEXT NOT NULL,
            source_period_label TEXT NOT NULL,
            source_geo_id TEXT NOT NULL,
            source_name TEXT,
            state_fips TEXT,
            county_fips TEXT,
            cbsa_code TEXT,
            period_month DATE NOT NULL,
            building_permits NUMERIC(18, 6),
            single_family_permits NUMERIC(18, 6),
            multi_family_permits NUMERIC(18, 6),
            permit_units NUMERIC(18, 6),
            source_payload JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_census_bps_geo_period_load
                UNIQUE (
                    geography_level,
                    period_type,
                    source_geo_id,
                    period_month,
                    load_date
                )
        );

        CREATE INDEX IF NOT EXISTS ix_raw_census_bps_geo
            ON raw.census_building_permits (geography_level, source_geo_id);

        CREATE INDEX IF NOT EXISTS ix_raw_census_bps_period
            ON raw.census_building_permits (period_month);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_census_bps_period;
        DROP INDEX IF EXISTS raw.ix_raw_census_bps_geo;
        DROP TABLE IF EXISTS raw.census_building_permits;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS permit_units;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS multi_family_permits;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS single_family_permits;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS building_permits;
        """
    )
