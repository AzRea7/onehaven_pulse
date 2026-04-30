"""create database schemas

Revision ID: 0001_create_database_schemas
Revises:
Create Date: 2026-04-30
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001_create_database_schemas"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


SCHEMAS = [
    "raw",
    "staging",
    "intermediate",
    "analytics",
    "geo",
    "app",
    "audit",
]


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS postgis')
    op.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')

    for schema in SCHEMAS:
        op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS audit.schema_migration_log (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            migration_name TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    )

    op.execute(
        """
        INSERT INTO audit.schema_migration_log (
            migration_name,
            message
        )
        VALUES (
            '0001_create_database_schemas',
            'Created foundational OneHaven database schemas'
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit.schema_migration_log")

    for schema in reversed(SCHEMAS):
        op.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    op.execute('DROP EXTENSION IF EXISTS "uuid-ossp"')
    op.execute("DROP EXTENSION IF EXISTS postgis")
