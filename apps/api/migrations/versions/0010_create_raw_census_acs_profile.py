"""create raw census acs profile

Revision ID: 0010_raw_census_acs
Revises: 0009_raw_redfin
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0010_raw_census_acs"
down_revision: str | None = "0009_raw_redfin"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS housing_units NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS occupied_housing_units NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS vacant_housing_units NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS owner_occupied_housing_units NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS renter_occupied_housing_units NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS owner_occupied_share NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS renter_occupied_share NUMERIC(12, 6);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS median_gross_rent NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS rent_burden_pct NUMERIC(12, 6);

        CREATE TABLE IF NOT EXISTS raw.census_acs_profile (
            id BIGSERIAL PRIMARY KEY,
            geography_level TEXT NOT NULL,
            source_geo_id TEXT NOT NULL,
            source_name TEXT NOT NULL,
            state_fips TEXT,
            county_fips TEXT,
            cbsa_code TEXT,
            year INTEGER NOT NULL,
            source_period_start DATE NOT NULL,
            source_period_end DATE NOT NULL,
            total_population NUMERIC(18, 6),
            median_household_income NUMERIC(18, 6),
            total_housing_units NUMERIC(18, 6),
            occupied_housing_units NUMERIC(18, 6),
            vacant_housing_units NUMERIC(18, 6),
            owner_occupied_housing_units NUMERIC(18, 6),
            renter_occupied_housing_units NUMERIC(18, 6),
            median_gross_rent NUMERIC(18, 6),
            rent_burden_pct NUMERIC(12, 6),
            source_payload JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_census_acs_profile_geo_year_load
                UNIQUE (
                    geography_level,
                    source_geo_id,
                    year,
                    load_date
                )
        );

        CREATE INDEX IF NOT EXISTS ix_raw_census_acs_profile_geo
            ON raw.census_acs_profile (geography_level, source_geo_id);

        CREATE INDEX IF NOT EXISTS ix_raw_census_acs_profile_year
            ON raw.census_acs_profile (year);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_census_acs_profile_year;
        DROP INDEX IF EXISTS raw.ix_raw_census_acs_profile_geo;
        DROP TABLE IF EXISTS raw.census_acs_profile;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS rent_burden_pct;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS median_gross_rent;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS renter_occupied_share;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS owner_occupied_share;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS renter_occupied_housing_units;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS owner_occupied_housing_units;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS vacant_housing_units;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS occupied_housing_units;

        ALTER TABLE analytics.market_monthly_metrics
        DROP COLUMN IF EXISTS housing_units;
        """
    )
