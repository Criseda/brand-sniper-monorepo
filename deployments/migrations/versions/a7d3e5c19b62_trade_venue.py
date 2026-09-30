"""record the venue a paper trade bought on

Revision ID: a7d3e5c19b62
Revises: f4c1b9a7e3d2
Create Date: 2026-09-30 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "a7d3e5c19b62"
down_revision: str | Sequence[str] | None = "f4c1b9a7e3d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the buy venue; every trade before Waxpeer (#261) was bought on Skinport."""
    op.add_column("simulated_trades", sa.Column("venue", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=True))
    op.execute("UPDATE simulated_trades SET venue = 'skinport' WHERE venue IS NULL")
    op.alter_column("simulated_trades", "venue", existing_type=sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False)


def downgrade() -> None:
    op.drop_column("simulated_trades", "venue")
