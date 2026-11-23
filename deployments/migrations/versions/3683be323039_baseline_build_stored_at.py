"""baseline_builds.stored_at: when a build was written, so when it came into force

Revision ID: 3683be323039
Revises: a7d4e2c9f1b3
Create Date: 2026-11-23 00:00:00.000000

A build's `built_at` is when its run started. A DMarket run then syncs sales for 20 to 80 minutes before it
stores the build, and the listener can only load it after that, so replays and labels that took a build to be
in force from `built_at` used it too early. Existing builds get `stored_at = built_at` (Skinport runs store a
build within seconds), and the DMarket builds already in the live database get the end time of the Prefect
flow run that stored them. Each of those updates matches the build's ID and `built_at`, so it changes nothing
in any other database.

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "3683be323039"
down_revision: str | Sequence[str] | None = "a7d4e2c9f1b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (build ID, built_at, end of the Prefect flow run that stored it), all naive UTC.
_DMARKET_STORE_TIMES = [
    (16, "2026-10-25 19:02:21.978380", "2026-10-25 20:23:18.483835"),
    (18, "2026-10-26 23:57:38.095670", "2026-10-27 00:19:14.057535"),
    (20, "2026-10-27 20:21:16.246107", "2026-10-27 20:47:54.369559"),
    (22, "2026-10-30 23:46:29.094993", "2026-10-31 00:08:58.053793"),
    (24, "2026-11-23 19:21:17.882807", "2026-11-23 19:52:02.072252"),
]


def upgrade() -> None:
    """Add the column and fill it for every existing build."""
    # A nullable column without a default is a catalog change only: no table rewrite.
    op.add_column("baseline_builds", sa.Column("stored_at", sa.DateTime(), nullable=True))
    op.execute("UPDATE baseline_builds SET stored_at = built_at WHERE stored_at IS NULL")
    for build_id, built_at, stored_at in _DMARKET_STORE_TIMES:
        op.execute(
            sa.text(
                "UPDATE baseline_builds SET stored_at = CAST(:stored_at AS TIMESTAMP) "
                "WHERE id = :build_id AND venue = 'dmarket' AND built_at = CAST(:built_at AS TIMESTAMP)"
            ).bindparams(build_id=build_id, built_at=built_at, stored_at=stored_at)
        )


def downgrade() -> None:
    """Drop the column; readers then take every build to be in force from built_at again."""
    op.drop_column("baseline_builds", "stored_at")
