"""name every venue column venue

Revision ID: b1e6f0a8c4d3
Revises: a7d3e5c19b62
Create Date: 2026-10-01 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

revision: str = "b1e6f0a8c4d3"
down_revision: str | Sequence[str] | None = "a7d3e5c19b62"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (table, old column name). Every one becomes `venue`, as in baseline_builds and simulated_trades.
RENAMED_COLUMNS = (
    ("live_market_ticks", "marketplace_source"),
    ("feed_events", "source"),
    ("listing_outcomes", "source"),
    ("ingestion_batches", "source"),
)


def _rename_not_null_constraint(table: str, old_column: str, new_column: str) -> None:
    """PostgreSQL 18 names NOT NULL constraints after their column; a column rename leaves the old name."""
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conrelid = '{table}'::regclass AND conname = '{table}_{old_column}_not_null'
            ) THEN
                ALTER TABLE {table} RENAME CONSTRAINT {table}_{old_column}_not_null TO {table}_{new_column}_not_null;
            END IF;
        END $$;
        """
    )


def upgrade() -> None:
    """Rename only: no data changes, and PostgreSQL does it without rewriting the tables."""
    for table, old_column in RENAMED_COLUMNS:
        op.alter_column(table, old_column, new_column_name="venue")
        _rename_not_null_constraint(table, old_column, "venue")


def downgrade() -> None:
    for table, old_column in RENAMED_COLUMNS:
        op.alter_column(table, "venue", new_column_name=old_column)
        _rename_not_null_constraint(table, "venue", old_column)
