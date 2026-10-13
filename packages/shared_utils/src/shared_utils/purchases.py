"""
What counts as one purchase (#271), shared by the live executor and the baseline scorecard so they agree.

A listing is bought once per venue. A REST snapshot names no listing, so its item and price stand in for
one: one buy per venue, item and price per UTC day. A listing bought that day also covers a snapshot of
its item at that price, since the next REST poll usually shows the listing as the item's lowest ask.

The backend enforces the same rule with the unique purchase indexes on simulated_trades.
"""

from datetime import date


def listing_purchase_key(venue: str, listing_id: str) -> str:
    return f"listing:{venue}:{listing_id}"


def snapshot_purchase_key(venue: str, market_hash_name: str, purchase_price_cents: int, utc_day: date) -> str:
    return f"snapshot:{venue}:{market_hash_name}:{purchase_price_cents}:{utc_day.isoformat()}"


def purchase_keys(
    venue: str, market_hash_name: str, purchase_price_cents: int, listing_id: str | None, utc_day: date
) -> tuple[str, str]:
    """(key, snapshot key) of a purchase. A buy repeats an earlier one when its key was bought before.

    Remember both once it is bought: for a REST snapshot they are the same key, and a listing's snapshot
    key stops the next poll from buying it again.
    """
    snapshot_key = snapshot_purchase_key(venue, market_hash_name, purchase_price_cents, utc_day)
    key = listing_purchase_key(venue, listing_id) if listing_id is not None else snapshot_key
    return key, snapshot_key
