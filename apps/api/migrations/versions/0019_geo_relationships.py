"""create geo relationships

Revision ID: 0019_geo_relationships
Revises: 0018_geo_type_constraint
Create Date: 2026-05-05
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0019_geo_relationships"
down_revision: str | None = "0018_geo_type_constraint"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS geo.geo_relationships (
            parent_geo_id varchar NOT NULL REFERENCES geo.dim_geo(geo_id),
            child_geo_id varchar NOT NULL REFERENCES geo.dim_geo(geo_id),
            relationship_type varchar NOT NULL,
            source varchar NOT NULL,
            confidence_score numeric(5, 4) NOT NULL DEFAULT 1.0000,
            overlap_ratio numeric(8, 6),
            is_active boolean NOT NULL DEFAULT true,
            notes text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT pk_geo_relationships PRIMARY KEY (
                parent_geo_id,
                child_geo_id,
                relationship_type,
                source
            ),
            CONSTRAINT ck_geo_relationships_type_valid CHECK (
                relationship_type IN (
                    'contains',
                    'overlaps',
                    'rolls_up_to',
                    'adjacent_to'
                )
            ),
            CONSTRAINT ck_geo_relationships_not_self CHECK (
                parent_geo_id <> child_geo_id
            ),
            CONSTRAINT ck_geo_relationships_confidence CHECK (
                confidence_score >= 0
                AND confidence_score <= 1
            ),
            CONSTRAINT ck_geo_relationships_overlap CHECK (
                overlap_ratio IS NULL
                OR (
                    overlap_ratio >= 0
                    AND overlap_ratio <= 1
                )
            )
        );

        CREATE INDEX IF NOT EXISTS ix_geo_relationships_child
            ON geo.geo_relationships (
                child_geo_id,
                relationship_type,
                is_active
            );

        CREATE INDEX IF NOT EXISTS ix_geo_relationships_parent
            ON geo.geo_relationships (
                parent_geo_id,
                relationship_type,
                is_active
            );

        CREATE INDEX IF NOT EXISTS ix_geo_relationships_source
            ON geo.geo_relationships (
                source,
                relationship_type
            );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS geo.ix_geo_relationships_source;
        DROP INDEX IF EXISTS geo.ix_geo_relationships_parent;
        DROP INDEX IF EXISTS geo.ix_geo_relationships_child;
        DROP TABLE IF EXISTS geo.geo_relationships;
        """
    )
