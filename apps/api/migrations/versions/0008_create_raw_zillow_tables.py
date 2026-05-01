"""create raw zillow tables

Revision ID: 0008_raw_zillow
Revises: 0007_raw_fhfa_hpi
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0008_raw_zillow"
down_revision: str | None = "0007_raw_fhfa_hpi"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.zillow_zhvi (
            id BIGSERIAL PRIMARY KEY,
            source_region_id TEXT NOT NULL,
            region_name TEXT NOT NULL,
            region_type TEXT,
            state_name TEXT,
            metro TEXT,
            county_name TEXT,
            period_month DATE NOT NULL,
            value NUMERIC(18, 6),
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_zillow_zhvi_region_period_load
                UNIQUE (source_region_id, period_month, load_date)
        );

        CREATE INDEX IF NOT EXISTS ix_raw_zillow_zhvi_period
            ON raw.zillow_zhvi (period_month);

        CREATE INDEX IF NOT EXISTS ix_raw_zillow_zhvi_region_type_name
            ON raw.zillow_zhvi (region_type, region_name);

        CREATE TABLE IF NOT EXISTS raw.zillow_zori (
            id BIGSERIAL PRIMARY KEY,
            source_region_id TEXT NOT NULL,
            region_name TEXT NOT NULL,
            region_type TEXT,
            state_name TEXT,
            metro TEXT,
            county_name TEXT,
            period_month DATE NOT NULL,
            value NUMERIC(18, 6),
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_zillow_zori_region_period_load
                UNIQUE (source_region_id, period_month, load_date)
        );

        CREATE INDEX IF NOT EXISTS ix_raw_zillow_zori_period
            ON raw.zillow_zori (period_month);

        CREATE INDEX IF NOT EXISTS ix_raw_zillow_zori_region_type_name
            ON raw.zillow_zori (region_type, region_name);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_zillow_zori_region_type_name;
        DROP INDEX IF EXISTS raw.ix_raw_zillow_zori_period;
        DROP TABLE IF EXISTS raw.zillow_zori;

        DROP INDEX IF EXISTS raw.ix_raw_zillow_zhvi_region_type_name;
        DROP INDEX IF EXISTS raw.ix_raw_zillow_zhvi_period;
        DROP TABLE IF EXISTS raw.zillow_zhvi;
        """
    )
