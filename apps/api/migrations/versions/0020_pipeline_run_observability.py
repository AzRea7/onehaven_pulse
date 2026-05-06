"""add pipeline run observability columns and indexes

Revision ID: 0020_pipeline_observability
Revises: 0019_geo_relationships
Create Date: 2026-05-05
"""

from collections.abc import Sequence

from alembic import op


revision: str = "0020_pipeline_observability"
down_revision: str | None = "0019_geo_relationships"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE audit.pipeline_runs
            ADD COLUMN IF NOT EXISTS unmatched_count INTEGER;

        CREATE INDEX IF NOT EXISTS ix_audit_pipeline_runs_started_at
            ON audit.pipeline_runs (started_at DESC);

        CREATE INDEX IF NOT EXISTS ix_audit_pipeline_runs_source_dataset_started_at
            ON audit.pipeline_runs (source, dataset, started_at DESC);

        CREATE INDEX IF NOT EXISTS ix_audit_pipeline_runs_status_started_at
            ON audit.pipeline_runs (status, started_at DESC);

        CREATE INDEX IF NOT EXISTS ix_audit_source_files_source_dataset_created_at
            ON audit.source_files (source, dataset, created_at DESC);

        CREATE INDEX IF NOT EXISTS ix_audit_source_freshness_stale_status
            ON audit.source_freshness (is_stale, last_status);
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS audit.ix_audit_source_freshness_stale_status;
        DROP INDEX IF EXISTS audit.ix_audit_source_files_source_dataset_created_at;
        DROP INDEX IF EXISTS audit.ix_audit_pipeline_runs_status_started_at;
        DROP INDEX IF EXISTS audit.ix_audit_pipeline_runs_source_dataset_started_at;
        DROP INDEX IF EXISTS audit.ix_audit_pipeline_runs_started_at;

        ALTER TABLE audit.pipeline_runs
            DROP COLUMN IF EXISTS unmatched_count;
        """
    )
