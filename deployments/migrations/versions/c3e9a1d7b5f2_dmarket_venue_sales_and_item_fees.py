"""venue_sales and a per item seller fee on venue_baselines (DMarket, #339)

Revision ID: c3e9a1d7b5f2
Revises: a4d8e2f6c913
Create Date: 2026-10-25 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "c3e9a1d7b5f2"
down_revision: str | Sequence[str] | None = "a4d8e2f6c913"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """The sales DMarket baselines and labels are built from, and each DMarket item's own seller fee."""
    # A nullable column without a default is a catalog change only: no table rewrite.
    op.add_column("venue_baselines", sa.Column("seller_fee_bps", sa.Integer(), nullable=True))

    op.create_table(
        "venue_sales",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("venue", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("title", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("market_hash_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("sold_at", sa.DateTime(), nullable=False),
        sa.Column("price_cents", sa.Integer(), nullable=False),
        sa.Column("sale_type", sqlmodel.sql.sqltypes.AutoString(length=16), nullable=False),
        sa.Column("float_value", sa.Float(), nullable=True),
        sa.Column("paint_seed", sa.Integer(), nullable=True),
        sa.Column("sale_key", sqlmodel.sql.sqltypes.AutoString(length=128), nullable=False),
        sa.Column("fetched_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', NOW())"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("uq_venue_sales_venue_title_sale_key", "venue_sales", ["venue", "title", "sale_key"], unique=True)
    op.create_index("ix_venue_sales_venue_name_sold_at", "venue_sales", ["venue", "market_hash_name", "sold_at"], unique=False)


def downgrade() -> None:
    """Drop the stored sales and the fee column. DMarket keeps 12 months of sales, so the sales can be read again."""
    op.drop_index("ix_venue_sales_venue_name_sold_at", table_name="venue_sales")
    op.drop_index("uq_venue_sales_venue_title_sale_key", table_name="venue_sales")
    op.drop_table("venue_sales")
    op.drop_column("venue_baselines", "seller_fee_bps")
