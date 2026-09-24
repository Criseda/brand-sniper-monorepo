"""add feed_events and listing-level tick columns

Revision ID: b8006925b075
Revises: e8dfb08f4745
Create Date: 2026-09-24 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b8006925b075"
down_revision: str | Sequence[str] | None = "e8dfb08f4745"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Record every feed event verbatim and extend ticks with listing-level context (#232)."""
    op.create_table(
        "feed_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("event_type", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("received_at", sa.DateTime(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_feed_events_event_type"), "feed_events", ["event_type"], unique=False)
    op.create_index(op.f("ix_feed_events_received_at"), "feed_events", ["received_at"], unique=False)

    # All new columns are nullable, so this is a metadata-only change on existing rows.
    op.add_column("live_market_ticks", sa.Column("pattern", sa.Integer(), nullable=True))
    op.add_column("live_market_ticks", sa.Column("listing_id", sqlmodel.sql.sqltypes.AutoString(length=64), nullable=True))
    op.add_column("live_market_ticks", sa.Column("event_type", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=True))
    op.add_column("live_market_ticks", sa.Column("stickers", postgresql.JSONB(astext_type=sa.Text()), nullable=True))
    op.add_column("live_market_ticks", sa.Column("listing_url", sqlmodel.sql.sqltypes.AutoString(length=512), nullable=True))

    # live_market_ticks is the hot ingest table (~10M rows): build its indexes without blocking writes.
    with op.get_context().autocommit_block():
        op.create_index(
            op.f("ix_live_market_ticks_listing_id"),
            "live_market_ticks",
            ["listing_id"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )
        op.create_index(
            op.f("ix_live_market_ticks_event_type"),
            "live_market_ticks",
            ["event_type"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    """Drop listing-level tick columns and the raw feed event log."""
    with op.get_context().autocommit_block():
        op.drop_index(
            op.f("ix_live_market_ticks_event_type"),
            table_name="live_market_ticks",
            postgresql_concurrently=True,
            if_exists=True,
        )
        op.drop_index(
            op.f("ix_live_market_ticks_listing_id"),
            table_name="live_market_ticks",
            postgresql_concurrently=True,
            if_exists=True,
        )
    op.drop_column("live_market_ticks", "listing_url")
    op.drop_column("live_market_ticks", "stickers")
    op.drop_column("live_market_ticks", "event_type")
    op.drop_column("live_market_ticks", "listing_id")
    op.drop_column("live_market_ticks", "pattern")

    op.drop_index(op.f("ix_feed_events_received_at"), table_name="feed_events")
    op.drop_index(op.f("ix_feed_events_event_type"), table_name="feed_events")
    op.drop_table("feed_events")
