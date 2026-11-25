"""student_scores: the student model's shadow score of each feed listing (#236)

Revision ID: b8e3f1c6d2a4
Revises: 3683be323039
Create Date: 2026-11-25 00:00:00.000000

A new, empty table. The listener writes it through the batch path while the rules keep deciding.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b8e3f1c6d2a4"
down_revision: str | Sequence[str] | None = "3683be323039"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create student_scores."""
    op.create_table(
        "student_scores",
        sa.Column("venue", sa.String(length=32), nullable=False),
        sa.Column("listing_id", sa.String(length=64), nullable=False),
        sa.Column("model_version", sa.String(length=96), nullable=False),
        sa.Column("feature_version", sa.Integer(), nullable=False),
        sa.Column("probability", sa.Float(), nullable=False),
        sa.Column("threshold", sa.Float(), nullable=False),
        sa.Column("would_approve", sa.Boolean(), nullable=False),
        sa.Column("observed_at", sa.DateTime(), nullable=False),
        sa.Column("scoring_ms", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("venue", "listing_id", "model_version"),
    )
    op.create_index("ix_student_scores_observed_at", "student_scores", ["observed_at"], unique=False)


def downgrade() -> None:
    """Drop student_scores."""
    op.drop_index("ix_student_scores_observed_at", table_name="student_scores")
    op.drop_table("student_scores")
