"""create raw overture places

Revision ID: 0017_raw_overture_places
Revises: 0016_raw_hmda_lar
Create Date: 2026-05-01
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0017_raw_overture_places"
down_revision: str | None = "0016_raw_hmda_lar"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS amenity_place_count NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS amenity_school_count NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS amenity_healthcare_count NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS amenity_grocery_count NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS amenity_food_service_count NUMERIC(18, 2);

        ALTER TABLE analytics.market_monthly_metrics
        ADD COLUMN IF NOT EXISTS amenity_bank_count NUMERIC(18, 2);

        CREATE TABLE IF NOT EXISTS raw.overture_places (
            id BIGSERIAL PRIMARY KEY,
            area_slug TEXT NOT NULL,
            area_name TEXT NOT NULL,
            place_id TEXT,
            name TEXT,
            category TEXT,
            primary_category TEXT,
            latitude NUMERIC(12, 8),
            longitude NUMERIC(12, 8),
            source_payload JSONB,
            source_file_id VARCHAR(64) REFERENCES audit.source_files(id) ON DELETE SET NULL,
            load_date DATE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS ix_raw_overture_places_area
            ON raw.overture_places (area_slug);

        CREATE INDEX IF NOT EXISTS ix_raw_overture_places_category
            ON raw.overture_places (primary_category);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS raw.ix_raw_overture_places_category;
        DROP INDEX IF EXISTS raw.ix_raw_overture_places_area;
        DROP TABLE IF EXISTS raw.overture_places;

        ALTER TABLE analytics.market_monthly_metrics DROP COLUMN IF EXISTS amenity_bank_count;
        ALTER TABLE analytics.market_monthly_metrics DROP COLUMN IF EXISTS amenity_food_service_count;
        ALTER TABLE analytics.market_monthly_metrics DROP COLUMN IF EXISTS amenity_grocery_count;
        ALTER TABLE analytics.market_monthly_metrics DROP COLUMN IF EXISTS amenity_healthcare_count;
        ALTER TABLE analytics.market_monthly_metrics DROP COLUMN IF EXISTS amenity_school_count;
        ALTER TABLE analytics.market_monthly_metrics DROP COLUMN IF EXISTS amenity_place_count;
        """
    )
