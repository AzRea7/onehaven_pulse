"""create canonical geography model

Revision ID: 0002_geo_model
Revises: 0001_create_database_schemas
Create Date: 2026-04-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002_geo_model"
down_revision: str | None = "0001_create_database_schemas"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "dim_geo",
        sa.Column("geo_id", sa.String(length=64), primary_key=True),
        sa.Column("geo_type", sa.String(length=50), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("state_code", sa.String(length=2), nullable=True),
        sa.Column("state_name", sa.String(length=100), nullable=True),
        sa.Column("county_fips", sa.String(length=5), nullable=True),
        sa.Column("cbsa_code", sa.String(length=5), nullable=True),
        sa.Column("zcta", sa.String(length=5), nullable=True),
        sa.Column("country_code", sa.String(length=2), nullable=False, server_default="US"),
        sa.Column("latitude", sa.Numeric(precision=10, scale=6), nullable=True),
        sa.Column("longitude", sa.Numeric(precision=10, scale=6), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "geo_type IN ('national', 'state', 'metro', 'county', 'zcta')",
            name="ck_dim_geo_geo_type_valid",
        ),
        schema="geo",
    )

    op.create_index(
        "ix_dim_geo_geo_type",
        "dim_geo",
        ["geo_type"],
        schema="geo",
    )

    op.create_index(
        "ix_dim_geo_state_code",
        "dim_geo",
        ["state_code"],
        schema="geo",
    )

    op.create_index(
        "ix_dim_geo_county_fips",
        "dim_geo",
        ["county_fips"],
        schema="geo",
        unique=True,
        postgresql_where=sa.text("county_fips IS NOT NULL"),
    )

    op.create_index(
        "ix_dim_geo_cbsa_code",
        "dim_geo",
        ["cbsa_code"],
        schema="geo",
        unique=False,
        postgresql_where=sa.text("cbsa_code IS NOT NULL"),
    )

    op.create_index(
        "ix_dim_geo_zcta",
        "dim_geo",
        ["zcta"],
        schema="geo",
        unique=True,
        postgresql_where=sa.text("zcta IS NOT NULL"),
    )

    op.create_table(
        "geo_crosswalk",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("source_geo_id", sa.String(length=255), nullable=True),
        sa.Column("source_geo_name", sa.String(length=255), nullable=False),
        sa.Column("source_geo_type", sa.String(length=100), nullable=False),
        sa.Column("canonical_geo_id", sa.String(length=64), nullable=False),
        sa.Column("match_method", sa.String(length=100), nullable=False, server_default="unknown"),
        sa.Column("confidence_score", sa.Numeric(precision=5, scale=4), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["canonical_geo_id"],
            ["geo.dim_geo.geo_id"],
            name="fk_geo_crosswalk_canonical_geo_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "confidence_score >= 0 AND confidence_score <= 1",
            name="ck_geo_crosswalk_confidence_score_range",
        ),
        sa.CheckConstraint(
            """
            match_method IN (
                'manual',
                'exact_fips',
                'exact_cbsa',
                'exact_source_id',
                'state_name_match',
                'fuzzy_name_match',
                'zip_to_zcta',
                'unknown'
            )
            """,
            name="ck_geo_crosswalk_match_method_valid",
        ),
        schema="geo",
    )

    op.create_index(
        "ix_geo_crosswalk_source",
        "geo_crosswalk",
        ["source"],
        schema="geo",
    )

    op.create_index(
        "ix_geo_crosswalk_source_geo_id",
        "geo_crosswalk",
        ["source", "source_geo_id"],
        schema="geo",
    )

    op.create_index(
        "ix_geo_crosswalk_source_geo_name",
        "geo_crosswalk",
        ["source", "source_geo_name"],
        schema="geo",
    )

    op.create_index(
        "ix_geo_crosswalk_canonical_geo_id",
        "geo_crosswalk",
        ["canonical_geo_id"],
        schema="geo",
    )

    op.create_table(
        "geo_geometry",
        sa.Column("geo_id", sa.String(length=64), primary_key=True),
        sa.Column("geo_type", sa.String(length=50), nullable=False),
        sa.Column("geometry_source", sa.String(length=100), nullable=True),
        sa.Column("geometry_year", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["geo_id"],
            ["geo.dim_geo.geo_id"],
            name="fk_geo_geometry_geo_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint(
            "geo_type IN ('national', 'state', 'metro', 'county', 'zcta')",
            name="ck_geo_geometry_geo_type_valid",
        ),
        schema="geo",
    )

    op.execute(
        """
        SELECT AddGeometryColumn(
            'geo',
            'geo_geometry',
            'geometry',
            4326,
            'MULTIPOLYGON',
            2
        );
        """
    )

    op.execute(
        """
        SELECT AddGeometryColumn(
            'geo',
            'geo_geometry',
            'simplified_geometry',
            4326,
            'MULTIPOLYGON',
            2
        );
        """
    )

    op.create_index(
        "ix_geo_geometry_geo_type",
        "geo_geometry",
        ["geo_type"],
        schema="geo",
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_geo_geometry_geometry_gist
        ON geo.geo_geometry
        USING GIST (geometry);
        """
    )

    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_geo_geometry_simplified_geometry_gist
        ON geo.geo_geometry
        USING GIST (simplified_geometry);
        """
    )

    op.execute(
        """
        INSERT INTO geo.dim_geo (
            geo_id,
            geo_type,
            name,
            display_name,
            country_code,
            latitude,
            longitude
        )
        VALUES (
            'us',
            'national',
            'United States',
            'United States',
            'US',
            39.828300,
            -98.579500
        )
        ON CONFLICT (geo_id) DO NOTHING;
        """
    )

    op.execute(
        """
        INSERT INTO geo.geo_crosswalk (
            source,
            source_geo_id,
            source_geo_name,
            source_geo_type,
            canonical_geo_id,
            match_method,
            confidence_score,
            notes
        )
        VALUES
            (
                'onehaven',
                'us',
                'United States',
                'national',
                'us',
                'manual',
                1.0,
                'Seed canonical national geography'
            ),
            (
                'fred',
                'US',
                'United States',
                'national',
                'us',
                'manual',
                1.0,
                'FRED national-level macroeconomic series'
            ),
            (
                'fhfa',
                'USA',
                'United States',
                'national',
                'us',
                'manual',
                1.0,
                'FHFA national-level HPI geography'
            )
        ON CONFLICT DO NOTHING;
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS geo.ix_geo_geometry_simplified_geometry_gist")
    op.execute("DROP INDEX IF EXISTS geo.ix_geo_geometry_geometry_gist")

    op.drop_table("geo_geometry", schema="geo")
    op.drop_table("geo_crosswalk", schema="geo")
    op.drop_table("dim_geo", schema="geo")
