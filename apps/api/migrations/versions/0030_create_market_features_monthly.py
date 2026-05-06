"""create market features monthly table

Revision ID: 0030_market_features
Revises: 0021_performance_guardrails
Create Date: 2026-05-06
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0030_market_features"
down_revision: str | None = "0021_performance_guardrails"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "market_features_monthly",
        sa.Column("geo_id", sa.String(length=64), nullable=False),
        sa.Column("period_month", sa.Date(), nullable=False),
        sa.Column("geo_type", sa.String(length=32), nullable=True),

        sa.Column("price_growth_1m", sa.Numeric(14, 6), nullable=True),
        sa.Column("price_growth_3m", sa.Numeric(14, 6), nullable=True),
        sa.Column("price_growth_12m", sa.Numeric(14, 6), nullable=True),
        sa.Column("rent_growth_1m", sa.Numeric(14, 6), nullable=True),
        sa.Column("rent_growth_3m", sa.Numeric(14, 6), nullable=True),
        sa.Column("rent_growth_12m", sa.Numeric(14, 6), nullable=True),
        sa.Column("inventory_change_3m", sa.Numeric(14, 6), nullable=True),
        sa.Column("inventory_change_12m", sa.Numeric(14, 6), nullable=True),
        sa.Column("days_on_market_change_3m", sa.Numeric(14, 6), nullable=True),
        sa.Column("mortgage_rate_30y", sa.Numeric(14, 6), nullable=True),
        sa.Column("rate_change_3m", sa.Numeric(14, 6), nullable=True),
        sa.Column("unemployment_rate", sa.Numeric(14, 6), nullable=True),
        sa.Column("unemployment_change_3m", sa.Numeric(14, 6), nullable=True),
        sa.Column("price_to_income_ratio", sa.Numeric(14, 6), nullable=True),
        sa.Column("payment_to_income_ratio", sa.Numeric(14, 6), nullable=True),
        sa.Column("affordability_score", sa.Numeric(14, 6), nullable=True),
        sa.Column("cycle_score", sa.Numeric(14, 6), nullable=True),
        sa.Column("cycle_phase_encoded", sa.Integer(), nullable=True),
        sa.Column("confidence_score", sa.Numeric(14, 6), nullable=True),

        sa.Column("has_price", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_rent", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_inventory", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_affordability", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_labor", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("has_permits", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("feature_completeness_score", sa.Numeric(8, 6), nullable=False, server_default=sa.text("0")),
        sa.Column("missing_feature_names", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("source_period_max", sa.Date(), nullable=True),
        sa.Column("is_trainable", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("quality_flags", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),

        sa.Column("target_price_growth_12m", sa.Numeric(14, 6), nullable=True),
        sa.Column("target_rent_growth_12m", sa.Numeric(14, 6), nullable=True),
        sa.Column("target_drawdown_12m", sa.Numeric(14, 6), nullable=True),
        sa.Column("target_cycle_phase_12m", sa.String(length=64), nullable=True),
        sa.Column("target_available", sa.Boolean(), nullable=False, server_default=sa.text("false")),

        sa.Column("feature_version", sa.String(length=32), nullable=False, server_default=sa.text("'v1'")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),

        sa.PrimaryKeyConstraint("geo_id", "period_month", "feature_version", name="pk_market_features_monthly"),
        schema="analytics",
    )

    op.create_index(
        "ix_market_features_monthly_geo_period",
        "market_features_monthly",
        ["geo_id", "period_month"],
        schema="analytics",
    )
    op.create_index(
        "ix_market_features_monthly_period",
        "market_features_monthly",
        ["period_month"],
        schema="analytics",
    )
    op.create_index(
        "ix_market_features_monthly_trainable",
        "market_features_monthly",
        ["is_trainable", "target_available"],
        schema="analytics",
    )


def downgrade() -> None:
    op.drop_index("ix_market_features_monthly_trainable", table_name="market_features_monthly", schema="analytics")
    op.drop_index("ix_market_features_monthly_period", table_name="market_features_monthly", schema="analytics")
    op.drop_index("ix_market_features_monthly_geo_period", table_name="market_features_monthly", schema="analytics")
    op.drop_table("market_features_monthly", schema="analytics")
