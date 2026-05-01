"""create raw source metadata tables

Revision ID: 0003_source_metadata
Revises: 0002_geo_model
Create Date: 2026-04-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003_source_metadata"
down_revision: str | None = "0002_geo_model"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "pipeline_runs",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("pipeline_name", sa.String(length=150), nullable=False),
        sa.Column("source", sa.String(length=100), nullable=True),
        sa.Column("dataset", sa.String(length=150), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Numeric(precision=12, scale=3), nullable=True),
        sa.Column("records_extracted", sa.BigInteger(), nullable=True),
        sa.Column("records_loaded", sa.BigInteger(), nullable=True),
        sa.Column("records_failed", sa.BigInteger(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'success', 'failed', 'cancelled', 'skipped')",
            name="ck_pipeline_runs_status_valid",
        ),
        schema="audit",
    )

    op.create_index(
        "ix_pipeline_runs_pipeline_name",
        "pipeline_runs",
        ["pipeline_name"],
        schema="audit",
    )

    op.create_index(
        "ix_pipeline_runs_source_dataset",
        "pipeline_runs",
        ["source", "dataset"],
        schema="audit",
    )

    op.create_index(
        "ix_pipeline_runs_status",
        "pipeline_runs",
        ["status"],
        schema="audit",
    )

    op.create_index(
        "ix_pipeline_runs_started_at",
        "pipeline_runs",
        ["started_at"],
        schema="audit",
    )

    op.create_table(
        "source_files",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("pipeline_run_id", sa.String(length=64), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("dataset", sa.String(length=150), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=True),
        sa.Column("raw_file_path", sa.Text(), nullable=False),
        sa.Column("storage_backend", sa.String(length=50), nullable=False, server_default="local"),
        sa.Column("file_format", sa.String(length=50), nullable=False),
        sa.Column("checksum_sha256", sa.String(length=64), nullable=True),
        sa.Column("file_size_bytes", sa.BigInteger(), nullable=True),
        sa.Column("record_count", sa.BigInteger(), nullable=True),
        sa.Column("source_period_start", sa.Date(), nullable=True),
        sa.Column("source_period_end", sa.Date(), nullable=True),
        sa.Column("load_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("metadata_json", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.ForeignKeyConstraint(
            ["pipeline_run_id"],
            ["audit.pipeline_runs.id"],
            name="fk_source_files_pipeline_run_id",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'success', 'failed', 'skipped')",
            name="ck_source_files_status_valid",
        ),
        sa.CheckConstraint(
            "storage_backend IN ('local', 's3', 'gcs', 'azure_blob')",
            name="ck_source_files_storage_backend_valid",
        ),
        schema="audit",
    )

    op.create_index(
        "ix_source_files_source_dataset",
        "source_files",
        ["source", "dataset"],
        schema="audit",
    )

    op.create_index(
        "ix_source_files_load_date",
        "source_files",
        ["load_date"],
        schema="audit",
    )

    op.create_index(
        "ix_source_files_status",
        "source_files",
        ["status"],
        schema="audit",
    )

    op.create_index(
        "ix_source_files_pipeline_run_id",
        "source_files",
        ["pipeline_run_id"],
        schema="audit",
    )

    op.create_index(
        "ix_source_files_checksum_sha256",
        "source_files",
        ["checksum_sha256"],
        schema="audit",
    )

    op.create_table(
        "source_freshness",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("source", sa.String(length=100), nullable=False),
        sa.Column("dataset", sa.String(length=150), nullable=False),
        sa.Column("expected_frequency", sa.String(length=50), nullable=False),
        sa.Column("freshness_threshold_days", sa.Integer(), nullable=False),
        sa.Column("latest_source_period", sa.Date(), nullable=True),
        sa.Column("last_loaded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_successful_run_id", sa.String(length=64), nullable=True),
        sa.Column("last_status", sa.String(length=50), nullable=False, server_default="pending"),
        sa.Column("is_stale", sa.Boolean(), nullable=False, server_default=sa.text("TRUE")),
        sa.Column("stale_reason", sa.Text(), nullable=True),
        sa.Column("record_count", sa.BigInteger(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
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
            ["last_successful_run_id"],
            ["audit.pipeline_runs.id"],
            name="fk_source_freshness_last_successful_run_id",
            ondelete="SET NULL",
        ),
        sa.UniqueConstraint(
            "source",
            "dataset",
            name="uq_source_freshness_source_dataset",
        ),
        sa.CheckConstraint(
            "expected_frequency IN ('daily', 'weekly', 'monthly', 'quarterly', 'annual', 'manual')",
            name="ck_source_freshness_expected_frequency_valid",
        ),
        sa.CheckConstraint(
            "last_status IN ('pending', 'success', 'failed', 'cancelled', 'skipped')",
            name="ck_source_freshness_last_status_valid",
        ),
        schema="audit",
    )

    op.create_index(
        "ix_source_freshness_source_dataset",
        "source_freshness",
        ["source", "dataset"],
        schema="audit",
    )

    op.create_index(
        "ix_source_freshness_is_stale",
        "source_freshness",
        ["is_stale"],
        schema="audit",
    )

    op.create_index(
        "ix_source_freshness_latest_source_period",
        "source_freshness",
        ["latest_source_period"],
        schema="audit",
    )

    op.create_table(
        "data_quality_checks",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("pipeline_run_id", sa.String(length=64), nullable=True),
        sa.Column("source", sa.String(length=100), nullable=True),
        sa.Column("dataset", sa.String(length=150), nullable=True),
        sa.Column("table_schema", sa.String(length=100), nullable=False),
        sa.Column("table_name", sa.String(length=150), nullable=False),
        sa.Column("check_name", sa.String(length=150), nullable=False),
        sa.Column("check_type", sa.String(length=100), nullable=False),
        sa.Column("severity", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=False),
        sa.Column(
            "checked_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("failed_row_count", sa.BigInteger(), nullable=True),
        sa.Column("total_row_count", sa.BigInteger(), nullable=True),
        sa.Column("details_json", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["pipeline_run_id"],
            ["audit.pipeline_runs.id"],
            name="fk_data_quality_checks_pipeline_run_id",
            ondelete="SET NULL",
        ),
        sa.CheckConstraint(
            "severity IN ('info', 'warning', 'critical')",
            name="ck_data_quality_checks_severity_valid",
        ),
        sa.CheckConstraint(
            "status IN ('passed', 'failed', 'warning', 'skipped')",
            name="ck_data_quality_checks_status_valid",
        ),
        schema="audit",
    )

    op.create_index(
        "ix_data_quality_checks_pipeline_run_id",
        "data_quality_checks",
        ["pipeline_run_id"],
        schema="audit",
    )

    op.create_index(
        "ix_data_quality_checks_table",
        "data_quality_checks",
        ["table_schema", "table_name"],
        schema="audit",
    )

    op.create_index(
        "ix_data_quality_checks_status",
        "data_quality_checks",
        ["status"],
        schema="audit",
    )

    op.create_index(
        "ix_data_quality_checks_checked_at",
        "data_quality_checks",
        ["checked_at"],
        schema="audit",
    )

    op.execute(
        """
        INSERT INTO audit.source_freshness (
            source,
            dataset,
            expected_frequency,
            freshness_threshold_days,
            last_status,
            is_stale,
            stale_reason
        )
        VALUES
            (
                'fred',
                'macro_series',
                'weekly',
                14,
                'pending',
                TRUE,
                'Dataset configured but not loaded yet'
            ),
            (
                'fhfa',
                'hpi',
                'quarterly',
                120,
                'pending',
                TRUE,
                'Dataset configured but not loaded yet'
            ),
            (
                'zillow',
                'zhvi',
                'monthly',
                45,
                'pending',
                TRUE,
                'Dataset configured but not loaded yet'
            ),
            (
                'zillow',
                'zori',
                'monthly',
                45,
                'pending',
                TRUE,
                'Dataset configured but not loaded yet'
            ),
            (
                'redfin',
                'market_tracker',
                'monthly',
                45,
                'pending',
                TRUE,
                'Dataset configured but not loaded yet'
            ),
            (
                'census',
                'geography',
                'annual',
                450,
                'pending',
                TRUE,
                'Dataset configured but not loaded yet'
            )
        ON CONFLICT (source, dataset) DO NOTHING;
        """
    )


def downgrade() -> None:
    op.drop_table("data_quality_checks", schema="audit")
    op.drop_table("source_freshness", schema="audit")
    op.drop_table("source_files", schema="audit")
    op.drop_table("pipeline_runs", schema="audit")
