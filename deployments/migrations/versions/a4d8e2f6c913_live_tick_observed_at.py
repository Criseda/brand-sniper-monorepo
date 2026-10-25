"""edge receive time to the millisecond on live_market_ticks (#256)

Revision ID: a4d8e2f6c913
Revises: 9b3d6f2e8a41
Create Date: 2026-10-25 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a4d8e2f6c913"
down_revision: str | Sequence[str] | None = "9b3d6f2e8a41"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add observed_at and the index the outcome labeler reads it through.

    Existing Skinport listing rows are filled by apps/analytics/backfill_observed_at.py, not here: it can be run
    again after the listeners are upgraded, for the batches an older listener delivered in between."""
    # A nullable column without a default is a catalog change only: no table rewrite.
    op.add_column("live_market_ticks", sa.Column("observed_at", sa.DateTime(), nullable=True))

    # live_market_ticks is the hot ingest table: build the index without blocking writes.
    with op.get_context().autocommit_block():
        op.create_index(
            "ix_live_market_ticks_listing_observed",
            "live_market_ticks",
            ["venue", "event_type", "observed_at"],
            unique=False,
            postgresql_where=sa.text("listing_id IS NOT NULL AND observed_at IS NOT NULL"),
            postgresql_concurrently=True,
            if_not_exists=True,
        )


def downgrade() -> None:
    """Drop the index and the column."""
    with op.get_context().autocommit_block():
        op.drop_index(
            "ix_live_market_ticks_listing_observed",
            table_name="live_market_ticks",
            postgresql_concurrently=True,
            if_exists=True,
        )
    op.drop_column("live_market_ticks", "observed_at")
