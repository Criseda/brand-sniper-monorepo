"""add trade_alerts: every attempt to alert a person about an approved listing (#18)

Revision ID: 9b3d6f2e8a41
Revises: 7c4e2a9f1b38
Create Date: 2026-10-21 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "9b3d6f2e8a41"
down_revision: str | Sequence[str] | None = "7c4e2a9f1b38"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """One row per alert attempt and channel; joined to listing_outcomes on (venue, listing_id)."""
    op.create_table(
        "trade_alerts",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("venue", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("listing_id", sqlmodel.sql.sqltypes.AutoString(length=64), nullable=True),
        sa.Column("market_hash_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("price_cents", sa.Integer(), nullable=False),
        sa.Column("estimated_net_margin_cents", sa.Integer(), nullable=True),
        sa.Column("decision_source", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("approval_reason", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("z_source", sqlmodel.sql.sqltypes.AutoString(length=16), nullable=False),
        sa.Column("z_score", sa.Float(), nullable=False),
        sa.Column("channel", sqlmodel.sql.sqltypes.AutoString(length=16), nullable=False),
        sa.Column("outcome", sqlmodel.sql.sqltypes.AutoString(length=16), nullable=False),
        sa.Column("received_at", sa.DateTime(), nullable=False),
        sa.Column("alerted_at", sa.DateTime(), nullable=False),
        sa.Column("latency_ms", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_trade_alerts_alerted_at"), "trade_alerts", ["alerted_at"], unique=False)
    op.create_index("ix_trade_alerts_venue_listing_id", "trade_alerts", ["venue", "listing_id"], unique=False)


def downgrade() -> None:
    """Drop the alert history. It cannot be rebuilt: the listener logs are the only other record."""
    op.drop_index("ix_trade_alerts_venue_listing_id", table_name="trade_alerts")
    op.drop_index(op.f("ix_trade_alerts_alerted_at"), table_name="trade_alerts")
    op.drop_table("trade_alerts")
