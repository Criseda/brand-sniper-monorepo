"""add baseline_builds and venue_baselines (dated per venue baselines)

Revision ID: f4c1b9a7e3d2
Revises: e2b7c9d41a58
Create Date: 2026-09-26 00:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

revision: str = "f4c1b9a7e3d2"
down_revision: str | Sequence[str] | None = "e2b7c9d41a58"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """One row per builder run, and one row per item in each run."""
    op.create_table(
        "baseline_builds",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("venue", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("method", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("built_at", sa.DateTime(), nullable=False),
        sa.Column("item_count", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_baseline_builds_venue"), "baseline_builds", ["venue"], unique=False)
    op.create_index(op.f("ix_baseline_builds_built_at"), "baseline_builds", ["built_at"], unique=False)

    op.create_table(
        "venue_baselines",
        sa.Column("build_id", sa.Integer(), nullable=False),
        sa.Column("market_hash_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("latest_price_cents", sa.Integer(), nullable=False),
        sa.Column("rolling_30d_avg_cents", sa.Integer(), nullable=False),
        sa.Column("rolling_90d_avg_cents", sa.Integer(), nullable=False),
        sa.Column("volatility_cents", sa.Integer(), nullable=False),
        sa.Column("support_floor_cents", sa.Integer(), nullable=False),
        sa.Column("avg_volume_30d", sa.Float(), nullable=False),
        sa.Column("drift_percent", sa.Float(), nullable=False),
        sa.Column("volatility_method", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("median_24h_cents", sa.Integer(), nullable=True),
        sa.Column("volume_24h", sa.Integer(), nullable=False),
        sa.Column("median_7d_cents", sa.Integer(), nullable=True),
        sa.Column("volume_7d", sa.Integer(), nullable=False),
        sa.Column("min_30d_cents", sa.Integer(), nullable=False),
        sa.Column("volume_30d", sa.Integer(), nullable=False),
        sa.Column("volume_90d", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["build_id"], ["baseline_builds.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("build_id", "market_hash_name"),
    )


def downgrade() -> None:
    """Drop the build history. It cannot be rebuilt: sales history only covers the last 90 days."""
    op.drop_table("venue_baselines")
    op.drop_index(op.f("ix_baseline_builds_built_at"), table_name="baseline_builds")
    op.drop_index(op.f("ix_baseline_builds_venue"), table_name="baseline_builds")
    op.drop_table("baseline_builds")
