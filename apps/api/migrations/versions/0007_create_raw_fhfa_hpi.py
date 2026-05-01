"""create raw fhfa hpi

Revision ID: 0007_raw_fhfa_hpi
Revises: 0006_raw_fred_obs
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0007_raw_fhfa_hpi"
down_revision: str | None = "0006_raw_fred_obs"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS raw.fhfa_hpi (
            id BIGSERIAL PRIMARY KEY,
            geo_name TEXT NOT NULL,
            geo_type TEXT NOT NULL,
            period DATE NOT NULL,
            frequency TEXT NOT NULL,
            hpi NUMERIC(18, 6),
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            CONSTRAINT uq_raw_fhfa_hpi_geo_period_frequency_load
                UNIQUE (geo_name, geo_type, period, frequency, load_date)
        );

        CREATE INDEX IF NOT EXISTS ix_raw_fhfa_hpi_geo_period
            ON raw.fhfa_hpi (geo_type, geo_name, period);

        CREATE INDEX IF NOT EXISTS ix_raw_fhfa_hpi_period
            ON raw.fhfa_hpi (period);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_fhfa_hpi_period;
        DROP INDEX IF EXISTS raw.ix_raw_fhfa_hpi_geo_period;
        DROP TABLE IF EXISTS raw.fhfa_hpi;
        """
    )
