"""record how a paper trade's profit estimate was computed; allow no estimate

Revision ID: e2b7c9d41a58
Revises: d5a2e8c41f37
Create Date: 2026-09-25 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "e2b7c9d41a58"
down_revision: str | Sequence[str] | None = "d5a2e8c41f37"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Tag existing estimates as gross and let trades without a baseline price record no estimate."""
    op.add_column(
        "simulated_trades",
        sa.Column("profit_estimate_basis", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=True),
    )
    # Every trade written before the shared P&L function was estimated as baseline price minus buy
    # price, with no fees. Apply this migration before deploying the listener that sends net estimates.
    op.execute("UPDATE simulated_trades SET profit_estimate_basis = 'gross' WHERE profit_estimate_basis IS NULL")
    op.alter_column("simulated_trades", "estimated_profit_cents", existing_type=sa.Integer(), nullable=True)


def downgrade() -> None:
    """Restore the NOT NULL estimate (trades without one become 0) and drop the basis tag."""
    op.execute("UPDATE simulated_trades SET estimated_profit_cents = 0 WHERE estimated_profit_cents IS NULL")
    op.alter_column("simulated_trades", "estimated_profit_cents", existing_type=sa.Integer(), nullable=False)
    op.drop_column("simulated_trades", "profit_estimate_basis")
