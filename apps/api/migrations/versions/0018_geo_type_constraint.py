"""expand dim_geo geo_type constraint

Revision ID: 0018_geo_type_constraint
Revises: 0017_raw_overture_places
Create Date: 2026-05-05
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0018_geo_type_constraint"
down_revision: str | None = "0017_raw_overture_places"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE geo.dim_geo
        DROP CONSTRAINT IF EXISTS ck_dim_geo_geo_type_valid;

        ALTER TABLE geo.dim_geo
        ADD CONSTRAINT ck_dim_geo_geo_type_valid
        CHECK (
            geo_type IN (
                'national',
                'state',
                'county',
                'metro',
                'place',
                'zcta',
                'custom'
            )
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE geo.dim_geo
        DROP CONSTRAINT IF EXISTS ck_dim_geo_geo_type_valid;

        ALTER TABLE geo.dim_geo
        ADD CONSTRAINT ck_dim_geo_geo_type_valid
        CHECK (
            geo_type IN (
                'national',
                'state',
                'county',
                'metro'
            )
        );
        """
    )
