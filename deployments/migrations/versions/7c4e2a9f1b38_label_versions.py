"""label versions: the config behind each label version, and the training index on listing_outcomes

Revision ID: 7c4e2a9f1b38
Revises: d064786e47a8
Create Date: 2026-10-13 00:00:00.000000

"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "7c4e2a9f1b38"
down_revision: str | Sequence[str] | None = "d064786e47a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

logger = logging.getLogger("alembic.runtime.migration")

# The parameters every existing v1 label was written with: LabelConfig() in apps/analytics/label_outcomes.py
# on 2026-10-13, with the Skinport fees of shared_utils.pnl. Copied here so this migration never changes
# when that code does. revise_seconds is new: it says when a label becomes final, and the daily run already
# re-labeled exactly the last 3 matured days.
V1_CONFIG = {
    "venue": "skinport",
    "fees": {
        "venue": "skinport",
        "fee_tiers": [{"min_price_cents": 0, "fee_bps": 800}, {"min_price_cents": 100_000, "fee_bps": 600}],
        "buyer_fee_bps": 0,
        "hold_seconds": 7 * 86_400,
        "min_margin_cents": 0,
    },
    "horizon_seconds": 14 * 86_400,
    "min_comparable_sales": 3,
    "settle_seconds": 3600,
    "revise_seconds": 3 * 86_400,
}

# Rows a label version other than v1 would leave without a known config.
OTHER_VERSIONS = "SELECT DISTINCT label_version FROM listing_outcomes WHERE label_version <> 'v1'"

# v1 rows that contradict V1_CONFIG in what a row shows of it: the horizon, the neutral rule, the minimum
# margin and the seller fee tiers (fees rounded up to the next cent, as shared_utils.pnl does).
V1_CONTRADICTIONS = """
SELECT
    count(*) FILTER (WHERE label_available_at <> listed_at + interval '14 days') AS horizon,
    count(*) FILTER (WHERE (comparable_sales < 3) <> (is_profitable IS NULL)) AS neutral_rule,
    count(*) FILTER (WHERE is_profitable IS NOT NULL AND is_profitable <> (resale_net_margin_cents >= 0)) AS min_margin,
    count(*) FILTER (
        WHERE resale_price_cents IS NOT NULL
          AND resale_net_margin_cents <> resale_price_cents
              - (resale_price_cents * (CASE WHEN resale_price_cents >= 100000 THEN 600 ELSE 800 END) + 9999) / 10000
              - listed_price_cents
    ) AS seller_fee
FROM listing_outcomes
WHERE label_version = 'v1'
"""


def upgrade() -> None:
    """Register v1 with the config its rows were written with, then make every label point at a version.

    Before tagging the existing rows as v1 it checks them against that config, as far as a row shows it,
    and stops if any row contradicts it or another version exists. No label row is changed.
    """
    bind = op.get_bind()
    other_versions = [row[0] for row in bind.execute(sa.text(OTHER_VERSIONS))]
    if other_versions:
        raise RuntimeError(
            f"listing_outcomes holds label versions other than v1 ({', '.join(other_versions)}) whose config is unknown. "
            "Register them in this migration or delete their rows first."
        )
    contradictions = dict(bind.execute(sa.text(V1_CONTRADICTIONS)).mappings().one())
    if any(contradictions.values()):
        raise RuntimeError(f"Some v1 labels contradict the v1 config: {contradictions}")
    logger.info("Every v1 label matches the v1 config (%s)", contradictions)

    label_versions = op.create_table(
        "label_versions",
        sa.Column("version", sqlmodel.sql.sqltypes.AutoString(length=32), nullable=False),
        sa.Column("config", postgresql.JSONB(none_as_null=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("TIMEZONE('utc', NOW())"), nullable=False),
        sa.PrimaryKeyConstraint("version"),
    )
    op.bulk_insert(label_versions, [{"version": "v1", "config": V1_CONFIG}])

    op.create_foreign_key(
        "fk_listing_outcomes_label_version",
        "listing_outcomes",
        "label_versions",
        ["label_version"],
        ["version"],
    )
    op.create_index(
        "ix_listing_outcomes_version_available",
        "listing_outcomes",
        ["label_version", "label_available_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_listing_outcomes_version_available", table_name="listing_outcomes")
    op.drop_constraint("fk_listing_outcomes_label_version", "listing_outcomes", type_="foreignkey")
    op.drop_table("label_versions")
