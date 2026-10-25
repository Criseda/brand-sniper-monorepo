"""drop historical_prices (the Kaggle Steam price history)

Revision ID: a7d4e2c9f1b3
Revises: c3e9a1d7b5f2
Create Date: 2026-10-25 00:00:00.000000

Nothing live reads this table any more (docs/data_sources.md), and it held about 10 GiB of a 32 GiB plan.
Back up the Kaggle CSVs (data/items/) before upgrading: the downgrade recreates the table empty, and the
last commit with apps/analytics/seed_historical.py can load them again.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7d4e2c9f1b3"
down_revision: str | Sequence[str] | None = "c3e9a1d7b5f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop the table and its index."""
    op.drop_index("ix_historical_prices_item_date", table_name="historical_prices", if_exists=True)
    op.drop_table("historical_prices", if_exists=True)


def downgrade() -> None:
    """Recreate the empty table with its original definition."""
    op.create_table(
        "historical_prices",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("item_id", sa.Integer(), nullable=False),
        sa.Column("sale_date", sa.DateTime(), nullable=False),
        sa.Column("median_price_cents", sa.Integer(), nullable=False),
        sa.Column("volume_sold", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["item_id"], ["market_items.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_historical_prices_item_date", "historical_prices", ["item_id", "sale_date"], unique=False)
