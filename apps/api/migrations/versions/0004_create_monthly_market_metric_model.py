"""create monthly market metric model

Revision ID: 0004_market_metrics
Revises: 0003_source_metadata
Create Date: 2026-04-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004_market_metrics"
down_revision: str | None = "0003_source_metadata"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_monthly_metrics",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("geo_id", sa.String(length=64), nullable=False),
        sa.Column("period_month", sa.Date(), nullable=False),

        # Price / valuation
        sa.Column("home_price_index", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("home_price_index_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("home_price_index_mom", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("real_home_price_index", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("zhvi", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("zhvi_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("zhvi_mom", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("median_sale_price", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("median_sale_price_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("median_sale_price_mom", sa.Numeric(precision=12, scale=6), nullable=True),

        # Rent
        sa.Column("zori", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("zori_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("zori_mom", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("median_rent", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("median_rent_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("rent_to_price_ratio", sa.Numeric(precision=12, scale=6), nullable=True),

        # Listing / inventory pulse
        sa.Column("active_listings", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("active_listings_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("new_listings", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("new_listings_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("homes_sold", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("homes_sold_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("months_supply", sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column("median_days_on_market", sa.Numeric(precision=12, scale=4), nullable=True),
        sa.Column("sale_to_list_ratio", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("price_drops_pct", sa.Numeric(precision=12, scale=6), nullable=True),

        # Macro / affordability
        sa.Column("mortgage_rate_30y", sa.Numeric(precision=8, scale=4), nullable=True),
        sa.Column("fed_funds_rate", sa.Numeric(precision=8, scale=4), nullable=True),
        sa.Column("cpi", sa.Numeric(precision=18, scale=6), nullable=True),
        sa.Column("unemployment_rate", sa.Numeric(precision=8, scale=4), nullable=True),
        sa.Column("recession_indicator", sa.Boolean(), nullable=True),
        sa.Column("estimated_monthly_payment", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("payment_to_income_ratio", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("price_to_income_ratio", sa.Numeric(precision=12, scale=6), nullable=True),

        # Supply / demographics
        sa.Column("building_permits", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("permits_per_1000_people", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("population", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("population_yoy", sa.Numeric(precision=12, scale=6), nullable=True),
        sa.Column("median_household_income", sa.Numeric(precision=18, scale=2), nullable=True),
        sa.Column("households", sa.Numeric(precision=18, scale=2), nullable=True),

        # Data traceability / quality
        sa.Column("source_flags", sa.JSON(), nullable=True),
        sa.Column("quality_flags", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),

        sa.ForeignKeyConstraint(
            ["geo_id"],
            ["geo.dim_geo.geo_id"],
            name="fk_market_monthly_metrics_geo_id",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "geo_id",
            "period_month",
            name="uq_market_monthly_metrics_geo_period",
        ),
        schema="analytics",
    )

    op.create_index(
        "ix_market_monthly_metrics_geo_id",
        "market_monthly_metrics",
        ["geo_id"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_monthly_metrics_period_month",
        "market_monthly_metrics",
        ["period_month"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_monthly_metrics_geo_period_desc",
        "market_monthly_metrics",
        ["geo_id", sa.text("period_month DESC")],
        schema="analytics",
    )

    op.create_index(
        "ix_market_monthly_metrics_period_geo",
        "market_monthly_metrics",
        ["period_month", "geo_id"],
        schema="analytics",
    )

    op.create_table(
        "market_metric_sources",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("geo_id", sa.String(length=64), nullable=False),
        sa.Column("period_month", sa.Date(), nullable=False),
        sa.Column("metric_name", sa.String(length=150), nullable=False),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("dataset", sa.String(length=150), nullable=False),
        sa.Column("source_file_id", sa.String(length=64), nullable=True),
        sa.Column("pipeline_run_id", sa.String(length=64), nullable=True),
        sa.Column("source_value", sa.Numeric(precision=24, scale=8), nullable=True),
        sa.Column("normalized_value", sa.Numeric(precision=24, scale=8), nullable=True),
        sa.Column("source_period", sa.Date(), nullable=True),
        sa.Column("transformation_notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),

        sa.ForeignKeyConstraint(
            ["geo_id"],
            ["geo.dim_geo.geo_id"],
            name="fk_market_metric_sources_geo_id",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_file_id"],
            ["audit.source_files.id"],
            name="fk_market_metric_sources_source_file_id",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["pipeline_run_id"],
            ["audit.pipeline_runs.id"],
            name="fk_market_metric_sources_pipeline_run_id",
            ondelete="SET NULL",
        ),
        schema="analytics",
    )

    op.create_index(
        "ix_market_metric_sources_geo_period",
        "market_metric_sources",
        ["geo_id", "period_month"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_metric_sources_metric_name",
        "market_metric_sources",
        ["metric_name"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_metric_sources_source_dataset",
        "market_metric_sources",
        ["source", "dataset"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_metric_sources_source_file_id",
        "market_metric_sources",
        ["source_file_id"],
        schema="analytics",
    )

    op.create_index(
        "ix_market_metric_sources_pipeline_run_id",
        "market_metric_sources",
        ["pipeline_run_id"],
        schema="analytics",
    )


def downgrade() -> None:
    op.drop_table("market_metric_sources", schema="analytics")
    op.drop_table("market_monthly_metrics", schema="analytics")
