"""buy each listing once: unique purchase indexes on simulated_trades

Revision ID: d064786e47a8
Revises: b1e6f0a8c4d3
Create Date: 2026-10-13 00:00:00.000000

"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d064786e47a8"
down_revision: str | Sequence[str] | None = "b1e6f0a8c4d3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

logger = logging.getLogger("alembic.runtime.migration")

# A row is a repeat when an earlier row (by buy time, then id) records the same purchase.
EARLIER_ROW = "(kept.simulated_buy_timestamp, kept.id) < (repeat.simulated_buy_timestamp, repeat.id)"

# The same listing bought again on the same venue, e.g. re-scored after a listener restart.
DELETE_REPEATED_LISTINGS = f"""
    DELETE FROM simulated_trades AS repeat
    USING simulated_trades AS kept
    WHERE repeat.listing_id IS NOT NULL
      AND kept.venue = repeat.venue
      AND kept.listing_id = repeat.listing_id
      AND {EARLIER_ROW}
"""

# A REST snapshot buy of an item and price already bought on that venue the same UTC day, whether the
# earlier buy was a snapshot or a listing (the next REST poll shows a bought listing as the lowest ask).
DELETE_REPEATED_SNAPSHOTS = f"""
    DELETE FROM simulated_trades AS repeat
    USING simulated_trades AS kept
    WHERE repeat.listing_id IS NULL
      AND kept.venue = repeat.venue
      AND kept.item_id = repeat.item_id
      AND kept.purchase_price_cents = repeat.purchase_price_cents
      AND date(kept.simulated_buy_timestamp) = date(repeat.simulated_buy_timestamp)
      AND {EARLIER_ROW}
"""


def upgrade() -> None:
    """Remove repeated buys, keeping the first, then let the database refuse new ones.

    The repeats are copies of a buy already recorded, made when the listener re-scored a listing it had
    forgotten. Keeping them would count one decision several times in the scorecard, and flagging them
    would make every reader filter them. The first buy of each purchase is kept, so no decision is lost.
    The downgrade drops the indexes but cannot bring the removed rows back.
    """
    bind = op.get_bind()
    removed_listings = bind.execute(sa.text(DELETE_REPEATED_LISTINGS)).rowcount
    removed_snapshots = bind.execute(sa.text(DELETE_REPEATED_SNAPSHOTS)).rowcount
    logger.info(
        "Removed %s repeated listing buys and %s repeated REST snapshot buys from simulated_trades",
        removed_listings,
        removed_snapshots,
    )

    op.create_index(
        "uq_simulated_trades_venue_listing_id",
        "simulated_trades",
        ["venue", "listing_id"],
        unique=True,
        postgresql_where=sa.text("listing_id IS NOT NULL"),
    )
    op.create_index(
        "uq_simulated_trades_snapshot_buy_day",
        "simulated_trades",
        ["venue", "item_id", "purchase_price_cents", sa.text("date(simulated_buy_timestamp)")],
        unique=True,
        postgresql_where=sa.text("listing_id IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_simulated_trades_snapshot_buy_day", table_name="simulated_trades")
    op.drop_index("uq_simulated_trades_venue_listing_id", table_name="simulated_trades")
