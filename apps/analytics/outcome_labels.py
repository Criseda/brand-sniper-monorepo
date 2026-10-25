"""
Market-outcome labels (triple-barrier style) for recorded listings: the pure part, with no I/O.

For every `listed` event of a venue, asks what the market actually did afterwards:
- What did comparable items (same versioned name) sell for on that venue once the trade hold was over,
  and would reselling at that price have cleared the seller fee and the minimum margin?
- Was this exact listing (same listing ID) seen to sell, and how fast?

A listing is only labeled once its horizon has passed, so a label never contains information from after
`label_available_at`. Label definition: docs/roadmap_proven_edge.md, section 4.3.

A label version is a contract: its parameters (`LabelConfig.contract`, the venue included) are stored in
`label_versions` the first time it is used, and the labeler refuses to write under a version whose stored
parameters differ from its own. Labeling another venue therefore needs its own version name.

Reading and writing the database: outcome_data.py. The Prefect flow and the CLI: label_outcomes.py.
"""

import bisect
import json
import statistics
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from shared_utils import VenueFees, fees_for, is_profitable_margin, net_resale_margin_cents

LABEL_VERSION = "v1"
# The venue LABEL_VERSION labels. Another venue is labeled under a version of its own.
LABEL_VENUE = "skinport"


@dataclass(frozen=True)
class LabelConfig:
    """Parameters of one label version. Changing any of them requires a new `version`."""

    version: str = LABEL_VERSION
    # Venue whose listings are labeled, against that venue's own sales.
    venue: str = LABEL_VENUE
    # Fees of that venue, used for both sides: a label resells the listing on the venue it was listed on.
    fees: VenueFees = field(default_factory=lambda: fees_for(LABEL_VENUE))
    # Resale window is [listed_at + fees.hold_seconds, listed_at + horizon_seconds].
    horizon_seconds: int = 14 * 86_400
    # Fewer comparable sales than this in the resale window: neutral (insufficient evidence).
    min_comparable_sales: int = 3
    # Extra wait after the horizon before labeling, so batches still in flight from the edge
    # (listener buffer, Redis stream retries) have landed in the database.
    settle_seconds: int = 3600
    # How long after it first becomes labelable (label_available_at + settle) a label may still be
    # rewritten by a later run that sees late batches. After that it is final. The daily run
    # re-labels the last 3 matured days, which is exactly this window.
    revise_seconds: int = 3 * 86_400

    @classmethod
    def for_venue(cls, venue: str, version: str, **parameters: Any) -> "LabelConfig":
        """A label version for another venue, with that venue's registered fees."""
        return cls(version=version, venue=venue, fees=fees_for(venue), **parameters)

    def __post_init__(self) -> None:
        if self.fees.venue != self.venue:
            raise ValueError(f"fees are for {self.fees.venue}, labels for {self.venue}")
        if self.horizon_seconds <= self.fees.hold_seconds:
            raise ValueError("horizon_seconds must be longer than the trade hold")
        if self.min_comparable_sales < 1:
            raise ValueError("min_comparable_sales must be at least 1")
        if self.settle_seconds < 0:
            raise ValueError("settle_seconds must not be negative")
        if self.revise_seconds < 0:
            raise ValueError("revise_seconds must not be negative")

    def contract(self) -> dict[str, Any]:
        """The parameters that define this version, as stored in `label_versions.config`."""
        return label_contract(
            venue=self.venue,
            fees=self.fees,
            horizon_seconds=self.horizon_seconds,
            min_comparable_sales=self.min_comparable_sales,
            settle_seconds=self.settle_seconds,
            revise_seconds=self.revise_seconds,
        )

    def final_before(self, as_of: datetime) -> datetime:
        """Labels with `label_available_at` at or before this time are final as of `as_of`."""
        return final_before(self.contract(), as_of)


class LabelVersionMismatchError(RuntimeError):
    """The labeler's parameters differ from the ones stored for its label version."""


def label_contract(
    venue: str, fees: VenueFees, horizon_seconds: int, min_comparable_sales: int, settle_seconds: int, revise_seconds: int
) -> dict[str, Any]:
    """The JSON stored in `label_versions.config`. The resale rule itself (comparable sales after the hold,
    lower median) is code: changing it also needs a new version."""
    return {
        "venue": venue,
        "fees": {
            "venue": fees.venue,
            "fee_tiers": [{"min_price_cents": tier.min_price_cents, "fee_bps": tier.fee_bps} for tier in fees.fee_tiers],
            "buyer_fee_bps": fees.buyer_fee_bps,
            "hold_seconds": fees.hold_seconds,
            "min_margin_cents": fees.min_margin_cents,
        },
        "horizon_seconds": horizon_seconds,
        "min_comparable_sales": min_comparable_sales,
        "settle_seconds": settle_seconds,
        "revise_seconds": revise_seconds,
    }


def final_before(contract: dict[str, Any], as_of: datetime) -> datetime:
    """Labels with `label_available_at` at or before this time are final as of `as_of`: no run rewrites them."""
    return as_of - timedelta(seconds=int(contract["settle_seconds"]) + int(contract["revise_seconds"]))


def flatten_contract(contract: dict[str, Any], prefix: str = "") -> dict[str, str]:
    """One string per leaf (`fees.hold_seconds`, ...), lists as JSON: for diffs and MLflow parameters."""
    flat: dict[str, str] = {}
    for key, value in sorted(contract.items()):
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            flat.update(flatten_contract(value, prefix=f"{name}."))
        elif isinstance(value, list):
            flat[name] = json.dumps(value, sort_keys=True)
        else:
            flat[name] = str(value)
    return flat


def contract_differences(stored: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    """Human readable differences between two contracts, empty when they match."""
    stored_flat = flatten_contract(stored)
    expected_flat = flatten_contract(expected)
    return [
        f"{key}: stored {stored_flat.get(key, '(missing)')}, running {expected_flat.get(key, '(missing)')}"
        for key in sorted(set(stored_flat) | set(expected_flat))
        if stored_flat.get(key) != expected_flat.get(key)
    ]


def label_mlflow_params(version: str, contract: dict[str, Any]) -> dict[str, str]:
    """MLflow parameters naming the label version and its full definition."""
    params = {"label_version": version}
    params.update({f"label.{key}": value for key, value in flatten_contract(contract).items()})
    return params


@dataclass(frozen=True)
class FinalLabels:
    """The final labels of one version as of a moment, with the definition they were made with."""

    version: str
    contract: dict[str, Any]
    as_of: datetime
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class FeedSale:
    """One listed or sold event of a listing, reduced to what labeling needs."""

    listing_id: str
    market_hash_name: str  # Versioned name (phase included), as in market_items
    price_cents: int
    seen_at: datetime  # Edge receive time of the event (naive UTC, millisecond precision)


@dataclass
class SoldIndex:
    """Sold sales, looked up by item name (sorted by time) and by listing ID (first sighting)."""

    times_by_name: dict[str, list[datetime]] = field(default_factory=dict)
    sales_by_name: dict[str, list[FeedSale]] = field(default_factory=dict)
    first_sold_by_listing: dict[str, FeedSale] = field(default_factory=dict)

    @classmethod
    def build(cls, sold_sales: Iterable[FeedSale]) -> "SoldIndex":
        # One listing can only be sold once, so a repeated listing ID is the same sale seen twice.
        unique_sales = first_sighting_per_listing(sold_sales)
        index = cls(first_sold_by_listing=unique_sales)
        for sale in sorted(unique_sales.values(), key=lambda s: (s.seen_at, s.listing_id)):
            index.times_by_name.setdefault(sale.market_hash_name, []).append(sale.seen_at)
            index.sales_by_name.setdefault(sale.market_hash_name, []).append(sale)
        return index

    def comparable_prices(self, name: str, window_start: datetime, window_end: datetime, exclude_listing: str) -> list[int]:
        """Prices of sales of `name` inside [window_start, window_end], excluding one listing."""
        times = self.times_by_name.get(name, [])
        sales = self.sales_by_name.get(name, [])
        first = bisect.bisect_left(times, window_start)
        last = bisect.bisect_right(times, window_end)
        return [sale.price_cents for sale in sales[first:last] if sale.listing_id != exclude_listing]


def first_sighting_per_listing(sales: Iterable[FeedSale]) -> dict[str, FeedSale]:
    """Keeps the earliest sighting of each listing ID (ties broken by the lower price)."""
    first: dict[str, FeedSale] = {}
    for sale in sales:
        current = first.get(sale.listing_id)
        if current is None or (sale.seen_at, sale.price_cents) < (current.seen_at, current.price_cents):
            first[sale.listing_id] = sale
    return first


def label_listing(listing: FeedSale, sold: SoldIndex, config: LabelConfig) -> dict[str, Any]:
    """Builds the `listing_outcomes` row for one listing. Uses no sale after listed_at + horizon."""
    listed_at = listing.seen_at
    label_available_at = listed_at + timedelta(seconds=config.horizon_seconds)
    resale_window_start = listed_at + timedelta(seconds=config.fees.hold_seconds)

    comparable = sold.comparable_prices(listing.market_hash_name, resale_window_start, label_available_at, listing.listing_id)
    resale_price_cents: int | None = None
    net_margin_cents: int | None = None
    is_profitable: bool | None = None
    if len(comparable) >= config.min_comparable_sales:
        # Lower median: the price a resale at the typical market level would have fetched. The
        # single best sale in the window would reward outliers that a seller could not count on.
        resale_price_cents = statistics.median_low(comparable)
        net_margin_cents = net_resale_margin_cents(
            listing.price_cents, resale_price_cents, buy_fees=config.fees, sell_fees=config.fees
        )
        is_profitable = is_profitable_margin(net_margin_cents, config.fees)

    # A sale of this listing counts only inside [listed_at, label_available_at]. No sold event means
    # "not seen to sell" (the feed never reports cancellations or price changes): censored.
    listing_sold_within_s: int | None = None
    own_sale = sold.first_sold_by_listing.get(listing.listing_id)
    if own_sale is not None and listed_at <= own_sale.seen_at <= label_available_at:
        listing_sold_within_s = int((own_sale.seen_at - listed_at).total_seconds())

    return {
        "venue": config.venue,
        "listing_id": listing.listing_id,
        "label_version": config.version,
        "market_hash_name": listing.market_hash_name,
        "listed_at": listed_at,
        "listed_price_cents": listing.price_cents,
        "comparable_sales": len(comparable),
        "resale_price_cents": resale_price_cents,
        "resale_net_margin_cents": net_margin_cents,
        "is_profitable": is_profitable,
        "listing_sold_within_s": listing_sold_within_s,
        "sale_censored": listing_sold_within_s is None,
        "label_available_at": label_available_at,
    }


def latest_labelable(as_of: datetime, config: LabelConfig) -> datetime:
    """Listings seen after this moment cannot be labeled yet as of `as_of`: their horizon (plus settle time) is open."""
    return as_of - timedelta(seconds=config.horizon_seconds + config.settle_seconds)


def label_listings(
    listed_sales: Iterable[FeedSale], sold_sales: Iterable[FeedSale] | SoldIndex, config: LabelConfig, as_of: datetime
) -> list[dict[str, Any]]:
    """Labels every listing whose horizon (plus settle time) has passed by `as_of`."""
    cutoff = latest_labelable(as_of, config)
    sold = sold_sales if isinstance(sold_sales, SoldIndex) else SoldIndex.build(sold_sales)
    listings = first_sighting_per_listing(listed_sales)
    rows = [label_listing(listing, sold, config) for listing in listings.values() if listing.seen_at <= cutoff]
    return sorted(rows, key=lambda row: (row["listed_at"], row["listing_id"]))


def iter_windows(start: datetime, end: datetime, window: timedelta) -> list[tuple[datetime, datetime]]:
    """Splits [start, end) into consecutive windows of at most `window`."""
    windows = []
    cursor = start
    while cursor < end:
        window_end = min(cursor + window, end)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def default_start(as_of: datetime, days_back: int, config: LabelConfig) -> datetime:
    """Start of the most recent `days_back` days of listings whose horizon has passed by `as_of`.

    Re-labeling a few matured days on each daily run picks up batches that reached the database late."""
    return latest_labelable(as_of, config) - timedelta(days=days_back)
