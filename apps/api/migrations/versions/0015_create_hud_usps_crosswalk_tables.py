"""create hud usps crosswalk tables

Revision ID: 0015_hud_usps_crosswalk
Revises: 0014_derived_market_metrics
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0015_hud_usps_crosswalk"
down_revision: str | None = "0014_derived_market_metrics"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.hud_usps_zip_crosswalk (
            id BIGSERIAL PRIMARY KEY,
            crosswalk_type TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            query TEXT NOT NULL,
            zip_code TEXT NOT NULL,
            target_key TEXT NOT NULL,
            tract_geoid TEXT,
            county_fips TEXT,
            cbsa_code TEXT,
            residential_ratio NUMERIC(18, 8),
            business_ratio NUMERIC(18, 8),
            other_ratio NUMERIC(18, 8),
            total_ratio NUMERIC(18, 8),
            source_payload JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_hud_usps_crosswalk
                UNIQUE (
                    crosswalk_type,
                    year,
                    quarter,
                    zip_code,
                    target_key,
                    load_date
                )
        );

        CREATE INDEX IF NOT EXISTS ix_raw_hud_usps_zip
            ON raw.hud_usps_zip_crosswalk (zip_code);

        CREATE INDEX IF NOT EXISTS ix_raw_hud_usps_county
            ON raw.hud_usps_zip_crosswalk (county_fips);

        CREATE INDEX IF NOT EXISTS ix_raw_hud_usps_cbsa
            ON raw.hud_usps_zip_crosswalk (cbsa_code);

        CREATE INDEX IF NOT EXISTS ix_raw_hud_usps_target_key
            ON raw.hud_usps_zip_crosswalk (target_key);

        CREATE TABLE IF NOT EXISTS geo.zip_geo_crosswalk (
            id BIGSERIAL PRIMARY KEY,
            zip_code TEXT NOT NULL,
            target_geo_id TEXT NOT NULL,
            target_geo_type TEXT NOT NULL,
            allocation_ratio NUMERIC(18, 8),
            source TEXT NOT NULL DEFAULT 'hud_usps',
            source_year INTEGER NOT NULL,
            source_quarter INTEGER NOT NULL,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_geo_zip_geo_crosswalk
                UNIQUE (
                    zip_code,
                    target_geo_id,
                    source_year,
                    source_quarter,
                    source
                )
        );

        CREATE INDEX IF NOT EXISTS ix_geo_zip_crosswalk_zip
            ON geo.zip_geo_crosswalk (zip_code);

        CREATE INDEX IF NOT EXISTS ix_geo_zip_crosswalk_target
            ON geo.zip_geo_crosswalk (target_geo_id);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS geo.ix_geo_zip_crosswalk_target;
        DROP INDEX IF EXISTS geo.ix_geo_zip_crosswalk_zip;
        DROP TABLE IF EXISTS geo.zip_geo_crosswalk;

        DROP INDEX IF EXISTS raw.ix_raw_hud_usps_target_key;
        DROP INDEX IF EXISTS raw.ix_raw_hud_usps_cbsa;
        DROP INDEX IF EXISTS raw.ix_raw_hud_usps_county;
        DROP INDEX IF EXISTS raw.ix_raw_hud_usps_zip;
        DROP TABLE IF EXISTS raw.hud_usps_zip_crosswalk;
        """
    )
