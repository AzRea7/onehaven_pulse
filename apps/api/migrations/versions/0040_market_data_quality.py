"""create market data quality table

Revision ID: 0040_market_data_quality
Revises: 0030_market_features
Create Date: 2026-05-07
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0040_market_data_quality"
down_revision: str | None = "0030_market_features"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_data_quality",
        sa.Column("geo_id", sa.String(), nullable=False),
        sa.Column("quality_version", sa.String(), nullable=False, server_default="v1"),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("latest_period", sa.Date(), nullable=True),

        sa.Column("coverage_score", sa.Numeric(8, 4), nullable=False),
        sa.Column("freshness_score", sa.Numeric(8, 4), nullable=False),
        sa.Column("validity_score", sa.Numeric(8, 4), nullable=False),
        sa.Column("overall_quality_score", sa.Numeric(8, 4), nullable=False),

        sa.Column("has_price", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_rent", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_inventory", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_affordability", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_labor", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_permits", sa.Boolean(), nullable=False, server_default=sa.false()),

        sa.Column("is_fresh", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_bad_values", sa.Boolean(), nullable=False, server_default=sa.false()),

        sa.Column("missing_categories", postgresql.ARRAY(sa.String()), nullable=False, server_default="{}"),
        sa.Column("stale_categories", postgresql.ARRAY(sa.String()), nullable=False, server_default="{}"),
        sa.Column("quality_issues", postgresql.ARRAY(sa.String()), nullable=False, server_default="{}"),

        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),

        schema="analytics",
    )

    op.create_primary_key(
        "pk_market_data_quality",
        "market_data_quality",
        ["geo_id", "quality_version", "as_of_date"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_data_quality_geo_id",
        "market_data_quality",
        ["geo_id"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_data_quality_overall_score",
        "market_data_quality",
        ["overall_quality_score"],
        schema="analytics",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_market_data_quality_overall_score",
        table_name="market_data_quality",
        schema="analytics",
    )
    op.drop_index(
        "ix_market_data_quality_geo_id",
        table_name="market_data_quality",
        schema="analytics",
    )
    op.drop_table("market_data_quality", schema="analytics")
