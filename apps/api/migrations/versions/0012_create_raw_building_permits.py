"""compatibility shim for old raw building permits migration

Revision ID: 0012_raw_permits
Revises: 0011_raw_bls_laus
Create Date: 2026-05-01
"""

from collections.abc import Sequence


revision: str = "0012_raw_permits"
down_revision: str | None = "0011_raw_bls_laus"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Compatibility shim.
    # This revision was already stamped/applied locally before the Census BPS
    # migration replaced the generic building_permits approach.
    pass


def downgrade() -> None:
    pass
