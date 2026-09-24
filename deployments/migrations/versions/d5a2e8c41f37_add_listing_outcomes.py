"""add listing_outcomes (versioned market-outcome labels)

Revision ID: d5a2e8c41f37
Revises: c3f1a7d2e904
Create Date: 2026-09-25 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "d5a2e8c41f37"
down_revision: str | Sequence[str] | None = "c3f1a7d2e904"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """One outcome label per listing and label version, written by the outcome labeler flow."""
    op.create_table(
        "listing_outcomes",
        sa.Column("source", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("listing_id", sqlmodel.sql.sqltypes.AutoString(length=64), nullable=False),
        sa.Column("label_version", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("market_hash_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("listed_at", sa.DateTime(), nullable=False),
        sa.Column("listed_price_cents", sa.Integer(), nullable=False),
        sa.Column("comparable_sales", sa.Integer(), nullable=False),
        sa.Column("resale_price_cents", sa.Integer(), nullable=True),
        sa.Column("resale_net_margin_cents", sa.Integer(), nullable=True),
        sa.Column("is_profitable", sa.Boolean(), nullable=True),
        sa.Column("listing_sold_within_s", sa.Integer(), nullable=True),
        sa.Column("sale_censored", sa.Boolean(), nullable=False),
        sa.Column("label_available_at", sa.DateTime(), nullable=False),
        sa.Column("labeled_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', NOW())"), nullable=False),
        sa.PrimaryKeyConstraint("source", "listing_id", "label_version"),
    )
    op.create_index(op.f("ix_listing_outcomes_listed_at"), "listing_outcomes", ["listed_at"], unique=False)
    op.create_index(op.f("ix_listing_outcomes_label_available_at"), "listing_outcomes", ["label_available_at"], unique=False)


def downgrade() -> None:
    """Drop the label table; labels are derived data and can be rebuilt from feed_events."""
    op.drop_index(op.f("ix_listing_outcomes_label_available_at"), table_name="listing_outcomes")
    op.drop_index(op.f("ix_listing_outcomes_listed_at"), table_name="listing_outcomes")
    op.drop_table("listing_outcomes")
