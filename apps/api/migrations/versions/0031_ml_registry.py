"""add ml model registry placeholders

Revision ID: 0031_ml_registry
Revises: 0030_market_features
Create Date: 2026-05-06
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "0031_ml_registry"
down_revision: str | None = "0030_market_features"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ml_model_registry",
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("model_name", sa.String(length=128), nullable=False),
        sa.Column("model_version", sa.String(length=64), nullable=False),
        sa.Column("model_type", sa.String(length=64), nullable=False),
        sa.Column("prediction_target", sa.String(length=128), nullable=False),
        sa.Column("prediction_horizon_months", sa.Integer(), nullable=False),
        sa.Column("feature_version", sa.String(length=32), nullable=False),
        sa.Column("training_start_period", sa.Date(), nullable=True),
        sa.Column("training_end_period", sa.Date(), nullable=True),
        sa.Column("validation_start_period", sa.Date(), nullable=True),
        sa.Column("validation_end_period", sa.Date(), nullable=True),
        sa.Column("metrics", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("feature_columns", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("target_columns", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("training_row_count", sa.Integer(), nullable=True),
        sa.Column("validation_row_count", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'placeholder'")),
        sa.Column("artifact_uri", sa.Text(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("model_id", name="pk_ml_model_registry"),
        sa.UniqueConstraint("model_name", "model_version", name="uq_ml_model_registry_name_version"),
        sa.CheckConstraint("prediction_horizon_months > 0", name="ck_ml_model_registry_horizon_positive"),
        sa.CheckConstraint(
            "status IN ('placeholder', 'training', 'validated', 'active', 'deprecated', 'failed')",
            name="ck_ml_model_registry_status",
        ),
        schema="analytics",
    )

    op.create_index(
        "ix_ml_model_registry_status",
        "ml_model_registry",
        ["status"],
        schema="analytics",
    )
    op.create_index(
        "ix_ml_model_registry_target_feature_version",
        "ml_model_registry",
        ["prediction_target", "feature_version"],
        schema="analytics",
    )

    op.create_table(
        "ml_predictions",
        sa.Column("prediction_id", sa.String(length=96), nullable=False),
        sa.Column("model_id", sa.String(length=64), nullable=False),
        sa.Column("geo_id", sa.String(length=64), nullable=False),
        sa.Column("period_month", sa.Date(), nullable=False),
        sa.Column("prediction_target", sa.String(length=128), nullable=False),
        sa.Column("prediction_horizon_months", sa.Integer(), nullable=False),
        sa.Column("predicted_value", sa.Numeric(18, 8), nullable=True),
        sa.Column("prediction_interval_low", sa.Numeric(18, 8), nullable=True),
        sa.Column("prediction_interval_high", sa.Numeric(18, 8), nullable=True),
        sa.Column("confidence_score", sa.Numeric(8, 6), nullable=True),
        sa.Column("feature_version", sa.String(length=32), nullable=False),
        sa.Column("input_feature_period", sa.Date(), nullable=False),
        sa.Column("input_feature_completeness_score", sa.Numeric(8, 6), nullable=True),
        sa.Column("input_target_available", sa.Boolean(), nullable=True),
        sa.Column("explanation", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.PrimaryKeyConstraint("prediction_id", name="pk_ml_predictions"),
        sa.ForeignKeyConstraint(
            ["model_id"],
            ["analytics.ml_model_registry.model_id"],
            name="fk_ml_predictions_model_id",
            ondelete="CASCADE",
        ),
        sa.CheckConstraint("prediction_horizon_months > 0", name="ck_ml_predictions_horizon_positive"),
        sa.CheckConstraint(
            "confidence_score IS NULL OR (confidence_score >= 0 AND confidence_score <= 1)",
            name="ck_ml_predictions_confidence_range",
        ),
        sa.CheckConstraint(
            "prediction_interval_low IS NULL OR prediction_interval_high IS NULL OR prediction_interval_low <= prediction_interval_high",
            name="ck_ml_predictions_interval_order",
        ),
        schema="analytics",
    )

    op.create_index(
        "ix_ml_predictions_model_geo_period",
        "ml_predictions",
        ["model_id", "geo_id", "period_month"],
        schema="analytics",
    )
    op.create_index(
        "ix_ml_predictions_geo_target_period",
        "ml_predictions",
        ["geo_id", "prediction_target", "period_month"],
        schema="analytics",
    )
    op.create_index(
        "ix_ml_predictions_feature_version_period",
        "ml_predictions",
        ["feature_version", "input_feature_period"],
        schema="analytics",
    )


def downgrade() -> None:
    op.drop_index("ix_ml_predictions_feature_version_period", table_name="ml_predictions", schema="analytics")
    op.drop_index("ix_ml_predictions_geo_target_period", table_name="ml_predictions", schema="analytics")
    op.drop_index("ix_ml_predictions_model_geo_period", table_name="ml_predictions", schema="analytics")
    op.drop_table("ml_predictions", schema="analytics")

    op.drop_index("ix_ml_model_registry_target_feature_version", table_name="ml_model_registry", schema="analytics")
    op.drop_index("ix_ml_model_registry_status", table_name="ml_model_registry", schema="analytics")
    op.drop_table("ml_model_registry", schema="analytics")
