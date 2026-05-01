"""create raw fred observations

Revision ID: 0006_raw_fred_obs
Revises: 0005_add_fred_macro_metric_columns
Create Date: 2026-05-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op


revision: str = "0006_raw_fred_obs"
down_revision: str | None = "0005_fred_macro_cols"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "fred_observations",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("series_id", sa.String(length=50), nullable=False),
        sa.Column("observation_date", sa.Date(), nullable=False),
        sa.Column("value", sa.String(length=50), nullable=True),
        sa.Column("realtime_start", sa.Date(), nullable=True),
        sa.Column("realtime_end", sa.Date(), nullable=True),
        sa.Column("load_date", sa.Date(), nullable=False),
        sa.Column(
            "source_file_id",
            sa.String(length=64),
            sa.ForeignKey("audit.source_files.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint(
            "series_id",
            "observation_date",
            "load_date",
            name="uq_fred_observations_series_date_load",
        ),
        schema="raw",
    )

    op.create_index(
        "ix_raw_fred_observations_series_date",
        "fred_observations",
        ["series_id", "observation_date"],
        schema="raw",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_raw_fred_observations_series_date",
        table_name="fred_observations",
        schema="raw",
    )
    op.drop_table("fred_observations", schema="raw")
