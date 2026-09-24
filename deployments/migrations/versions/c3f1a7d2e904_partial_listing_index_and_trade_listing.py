"""partial listing index, drop low-value event_type indexes, record the bought listing on trades

Revision ID: c3f1a7d2e904
Revises: b8006925b075
Create Date: 2026-09-25 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "c3f1a7d2e904"
down_revision: str | Sequence[str] | None = "b8006925b075"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Shrink the hot-table indexes and link paper trades to the listing they bought."""
    op.add_column("simulated_trades", sa.Column("listing_id", sqlmodel.sql.sqltypes.AutoString(length=64), nullable=True))
    op.add_column("simulated_trades", sa.Column("float_value", sa.Float(), nullable=True))

    # event_type has three values (listed, sold, NULL), so a standalone index is never selective.
    op.drop_index("ix_feed_events_event_type", table_name="feed_events", if_exists=True)

    # live_market_ticks is the hot ingest table (~10M rows): change its indexes without blocking writes.
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_live_market_ticks_event_type",
            table_name="live_market_ticks",
            postgresql_concurrently=True,
            if_exists=True,
        )
        # Replace the full listing_id index with a partial one: most rows are REST snapshots with a
        # NULL listing_id that would otherwise be indexed on every insert.
        op.drop_index(
            "ix_live_market_ticks_listing_id",
            table_name="live_market_ticks",
            postgresql_concurrently=True,
            if_exists=True,
        )
        op.create_index(
            "ix_live_market_ticks_listing_id",
            "live_market_ticks",
            ["listing_id"],
            unique=False,
            postgresql_where=sa.text("listing_id IS NOT NULL"),
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    """Restore the full indexes from b8006925b075 and drop the trade listing columns."""
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_live_market_ticks_listing_id",
            table_name="live_market_ticks",
            postgresql_concurrently=True,
            if_exists=True,
        )
        op.create_index(
            "ix_live_market_ticks_listing_id",
            "live_market_ticks",
            ["listing_id"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )
        op.create_index(
            "ix_live_market_ticks_event_type",
            "live_market_ticks",
            ["event_type"],
            unique=False,
            postgresql_concurrently=True,
            if_not_exists=True,
        )

    op.create_index("ix_feed_events_event_type", "feed_events", ["event_type"], unique=False, if_not_exists=True)
    op.drop_column("simulated_trades", "float_value")
    op.drop_column("simulated_trades", "listing_id")
