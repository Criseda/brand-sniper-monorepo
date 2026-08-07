"""add ingestion batch ledger

Revision ID: e8dfb08f4745
Revises: 416f6e55f454
Create Date: 2026-08-07 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "e8dfb08f4745"
down_revision: str | Sequence[str] | None = "416f6e55f454"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the bulk-ingestion idempotency ledger."""
    op.create_table(
        "ingestion_batches",
        sa.Column("batch_id", sqlmodel.sql.sqltypes.AutoString(length=36), nullable=False),
        sa.Column("source", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("record_count", sa.Integer(), nullable=False),
        sa.Column("payload_sha256", sqlmodel.sql.sqltypes.AutoString(length=64), nullable=False),
        sa.Column("received_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', NOW())"), nullable=False),
        sa.PrimaryKeyConstraint("batch_id"),
    )
    op.create_index(op.f("ix_ingestion_batches_received_at"), "ingestion_batches", ["received_at"], unique=False)


def downgrade() -> None:
    """Drop the bulk-ingestion idempotency ledger."""
    op.drop_index(op.f("ix_ingestion_batches_received_at"), table_name="ingestion_batches")
    op.drop_table("ingestion_batches")
