"""
Market-outcome labels (triple-barrier style) for recorded listings: the pure part, with no I/O.

For every `listed` event of a venue, asks what the market actually did afterwards:
- What did comparable items (same versioned name) sell for on that venue once the trade hold was over,
  and would reselling at that price have cleared the seller fee and the minimum margin?
- Was this exact listing (same listing ID) seen to sell, and how fast?

A listing is only labeled once its horizon has passed, so a label never contains information from after
`label_available_at`. Label definition: docs/roadmap_proven_edge.md, section 4.3.

The feed only shows the sales made while the listener was running. `v2` (#368) therefore labels a listing only
when the feed was up for most of its resale window (`FeedCoverage`); `v1` labeled every listing, whatever part of
its window the feed missed.

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

# The label version written and read by default. `v1` (LABEL_V1, below) is the same rule without the feed coverage.
LABEL_VERSION = "v2"
LABEL_V1_VERSION = "v1"
# The venue LABEL_VERSION labels. Another venue is labeled under a version of its own.
LABEL_VENUE = "skinport"

# Where a label version reads the sales it compares listings with.
SALES_FROM_TICKS = "ticks"  # `sold` events in live_market_ticks, which name their listing (Skinport's sale feed)
SALES_FROM_VENUE_SALES = "venue_sales"  # The venue's own sales history, listing purchases only (DMarket, #339)

# Label parameters of a venue whose sales do not come from the feed. LabelConfig.for_venue applies them.
VENUE_LABEL_PARAMETERS: dict[str, dict[str, Any]] = {
    "dmarket": {
        "sales_source": SALES_FROM_VENUE_SALES,
        "item_seller_fees": True,
        "sales_settle_seconds": 86_400,
        "own_sale_by_item": True,
        # The venue's sales history covers the time the listener was off.
        "min_feed_coverage": 0.0,
    },
}

# Parameters of an older label version of the default venue, which LabelConfig.for_venue applies by name.
VERSION_LABEL_PARAMETERS: dict[str, dict[str, Any]] = {
    LABEL_V1_VERSION: {"min_feed_coverage": 0.0},
}


@dataclass(frozen=True)
class LabelConfig:
    """Parameters of one label version. Changing any of them requires a new `version`."""

    version: str = LABEL_VERSION
    # Venue whose listings are labeled, against that venue's own sales.
    venue: str = LABEL_VENUE
    # Fees of that venue, used for both sides: a label resells the listing on the venue it was listed on.
    fees: VenueFees = field(default_factory=lambda: fees_for(LABEL_VENUE))
    # SALES_FROM_TICKS or SALES_FROM_VENUE_SALES.
    sales_source: str = SALES_FROM_TICKS
    # Resell at each item's own seller fee, from the venue's baseline build in force when the listing was
    # recorded (`venue_baselines.seller_fee_bps`), instead of the venue's price tiers. An item that build has
    # no fee for pays the tiers, which never overstates the margin.
    item_seller_fees: bool = False
    # SALES_FROM_VENUE_SALES only. The venue's sales are fetched by the baseline builder, which does not run
    # while the PC is off, so a listing is labeled only once a sync started this long after its horizon:
    # sales show up in the history late, and a label made before they are fetched would miss them.
    sales_settle_seconds: int = 0
    # SALES_FROM_VENUE_SALES only. A sale there names no listing, so the listing's own sale is the first sale
    # of the same item (name, float, paint seed) at the listing's price (`SoldIndex.item_sale`). Off, every
    # label of the version is censored.
    own_sale_by_item: bool = False
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
    # SALES_FROM_TICKS only. A listing is labeled only when the feed was up for at least this share of its resale
    # window: with the feed down, sales are missing, not absent. 0 labels every listing (`v1`).
    min_feed_coverage: float = 0.9
    # The feed counts as down across any gap longer than this between two recorded listing or sale events. It
    # carries several events a minute; its longest quiet spell while up was about 6 minutes.
    feed_gap_seconds: int = 900

    @classmethod
    def for_venue(cls, venue: str, version: str, **parameters: Any) -> "LabelConfig":
        """A label version for another venue, with that venue's registered fees and label parameters, or an older
        version of the default venue by its name."""
        version_parameters = VERSION_LABEL_PARAMETERS.get(version, {}) if venue == LABEL_VENUE else {}
        venue_parameters = {**VENUE_LABEL_PARAMETERS.get(venue, {}), **version_parameters, **parameters}
        return cls(version=version, venue=venue, fees=fees_for(venue), **venue_parameters)

    def __post_init__(self) -> None:
        if self.fees.venue != self.venue:
            raise ValueError(f"fees are for {self.fees.venue}, labels for {self.venue}")
        if self.sales_source not in (SALES_FROM_TICKS, SALES_FROM_VENUE_SALES):
            raise ValueError(f"unknown sales_source {self.sales_source!r}")
        if self.sales_settle_seconds < 0:
            raise ValueError("sales_settle_seconds must not be negative")
        if (self.sales_settle_seconds or self.own_sale_by_item) and self.sales_source != SALES_FROM_VENUE_SALES:
            raise ValueError("sales_settle_seconds and own_sale_by_item apply only to sales from venue_sales")
        if self.horizon_seconds <= self.fees.hold_seconds:
            raise ValueError("horizon_seconds must be longer than the trade hold")
        if self.min_comparable_sales < 1:
            raise ValueError("min_comparable_sales must be at least 1")
        if self.settle_seconds < 0:
            raise ValueError("settle_seconds must not be negative")
        if self.revise_seconds < 0:
            raise ValueError("revise_seconds must not be negative")
        if not 0.0 <= self.min_feed_coverage <= 1.0:
            raise ValueError("min_feed_coverage must be between 0 and 1")
        if self.min_feed_coverage > 0 and self.sales_source != SALES_FROM_TICKS:
            raise ValueError("min_feed_coverage applies only to sales from the feed")
        if self.feed_gap_seconds < 60:
            raise ValueError("feed_gap_seconds must be at least 60")

    def contract(self) -> dict[str, Any]:
        """The parameters that define this version, as stored in `label_versions.config`."""
        return label_contract(
            venue=self.venue,
            fees=self.fees,
            horizon_seconds=self.horizon_seconds,
            min_comparable_sales=self.min_comparable_sales,
            settle_seconds=self.settle_seconds,
            revise_seconds=self.revise_seconds,
            sales_source=self.sales_source,
            item_seller_fees=self.item_seller_fees,
            sales_settle_seconds=self.sales_settle_seconds,
            own_sale_by_item=self.own_sale_by_item,
            min_feed_coverage=self.min_feed_coverage,
            feed_gap_seconds=self.feed_gap_seconds,
        )

    @property
    def needs_feed_coverage(self) -> bool:
        return self.min_feed_coverage > 0

    def final_before(self, as_of: datetime) -> datetime:
        """Labels with `label_available_at` at or before this time are final as of `as_of`."""
        return final_before(self.contract(), as_of)


class LabelVersionMismatchError(RuntimeError):
    """The labeler's parameters differ from the ones stored for its label version."""


def label_contract(
    venue: str,
    fees: VenueFees,
    horizon_seconds: int,
    min_comparable_sales: int,
    settle_seconds: int,
    revise_seconds: int,
    sales_source: str = SALES_FROM_TICKS,
    item_seller_fees: bool = False,
    sales_settle_seconds: int = 0,
    own_sale_by_item: bool = False,
    min_feed_coverage: float = 0.0,
    feed_gap_seconds: int = 900,
) -> dict[str, Any]:
    """The JSON stored in `label_versions.config`. The resale rule itself (comparable sales after the hold,
    lower median) is code: changing it also needs a new version.

    The sales source, the item fees and the feed coverage appear only when they differ from `v1`'s, so the
    versions stored before they existed (`v1`, `v1-dmarket`) keep the contract they were registered with."""
    contract: dict[str, Any] = {
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
    if sales_source != SALES_FROM_TICKS:
        contract["sales"] = {
            "source": sales_source,
            "sale_type": "offer",
            "settle_seconds": sales_settle_seconds,
            "own_sale": (
                "first sale of the same name, float and paint seed before the item's next listing, at the listing price"
                if own_sale_by_item
                else "none (censored)"
            ),
        }
    if item_seller_fees:
        contract["item_seller_fees"] = "venue_baselines.seller_fee_bps of the build in force at listed_at"
    if min_feed_coverage > 0:
        contract["feed_coverage"] = {
            "min_resale_window_share": min_feed_coverage,
            "gap_seconds": feed_gap_seconds,
            "events": "listed and sold, by minute",
        }
    return contract


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
    seen_at: datetime  # Edge receive time of the event (naive UTC, millisecond precision); a sale time for venue_sales
    # The item itself, where the venue reports it: with the name, a float and a paint seed tell one item apart.
    float_value: float | None = None
    pattern: int | None = None
    # A listing only, with `own_sale_by_item`: when the same item was next recorded as another listing. A sale
    # after that is the later listing's.
    next_listed_at: datetime | None = None


# Floats arrive as decimal strings on listings and as JSON numbers on sales; this many decimals compare them.
ITEM_FLOAT_DECIMALS = 10
# A listing's time is the edge receive time and a sale's the venue's; a sale this much earlier still counts.
OWN_SALE_CLOCK_SKEW_SECONDS = 60


def item_key(sale: FeedSale) -> tuple[str, float, int | None] | None:
    """The physical item a listing or sale is of; None for an item without a float (a case, a sticker)."""
    if sale.float_value is None:
        return None
    return sale.market_hash_name, round(sale.float_value, ITEM_FLOAT_DECIMALS), sale.pattern


@dataclass
class SoldIndex:
    """Sold sales, looked up by item name (sorted by time), by listing ID (first sighting) and by item."""

    times_by_name: dict[str, list[datetime]] = field(default_factory=dict)
    sales_by_name: dict[str, list[FeedSale]] = field(default_factory=dict)
    first_sold_by_listing: dict[str, FeedSale] = field(default_factory=dict)
    sales_by_item: dict[tuple[str, float, int | None], list[FeedSale]] = field(default_factory=dict)

    @classmethod
    def build(cls, sold_sales: Iterable[FeedSale]) -> "SoldIndex":
        # One listing can only be sold once, so a repeated listing ID is the same sale seen twice.
        unique_sales = first_sighting_per_listing(sold_sales)
        index = cls(first_sold_by_listing=unique_sales)
        for sale in sorted(unique_sales.values(), key=lambda s: (s.seen_at, s.listing_id)):
            index.times_by_name.setdefault(sale.market_hash_name, []).append(sale.seen_at)
            index.sales_by_name.setdefault(sale.market_hash_name, []).append(sale)
            key = item_key(sale)
            if key is not None:
                index.sales_by_item.setdefault(key, []).append(sale)
        return index

    def comparable_prices(self, name: str, window_start: datetime, window_end: datetime, exclude_listing: str) -> list[int]:
        """Prices of sales of `name` inside [window_start, window_end], excluding one listing."""
        times = self.times_by_name.get(name, [])
        sales = self.sales_by_name.get(name, [])
        first = bisect.bisect_left(times, window_start)
        last = bisect.bisect_right(times, window_end)
        return [sale.price_cents for sale in sales[first:last] if sale.listing_id != exclude_listing]

    def item_sale(self, listing: FeedSale, window_start: datetime, window_end: datetime) -> FeedSale | None:
        """The listing's own sale: the first sale of its item inside [window_start, window_end] and before the
        item's next recorded listing (`next_listed_at`), when it is at the listing's price.

        For a venue whose sales name no listing (DMarket). A new price is a new listing there, so a first sale
        at another price is the item sold under another listing (relisted at a price the listener did not
        record), and this listing did not sell."""
        key = item_key(listing)
        if key is None:
            return None
        for sale in self.sales_by_item.get(key, []):
            if window_start <= sale.seen_at <= window_end:
                if listing.next_listed_at is not None and sale.seen_at >= listing.next_listed_at:
                    return None
                return sale if sale.price_cents == listing.price_cents else None
        return None


@dataclass
class FeedCoverage:
    """When the feed was up, from the minutes in which the listener recorded a listing or a sale (#368).

    Two such minutes no more than `gap_seconds` apart belong to one up period; anything between up periods,
    or outside the minutes it was built from, counts as down."""

    starts: list[datetime] = field(default_factory=list)
    ends: list[datetime] = field(default_factory=list)
    # Up seconds before each period, so a window's up time takes two binary searches.
    seconds_before: list[float] = field(default_factory=list)

    @classmethod
    def build(cls, event_minutes: Iterable[datetime], gap_seconds: int) -> "FeedCoverage":
        coverage = cls()
        gap = timedelta(seconds=gap_seconds)
        total = 0.0
        for minute in sorted(set(event_minutes)):
            minute_end = minute + timedelta(minutes=1)
            if coverage.ends and minute - coverage.ends[-1] <= gap:
                total += (minute_end - coverage.ends[-1]).total_seconds()
                coverage.ends[-1] = minute_end
                continue
            coverage.starts.append(minute)
            coverage.ends.append(minute_end)
            coverage.seconds_before.append(total)
            total += 60.0
        return coverage

    def _up_seconds_until(self, moment: datetime) -> float:
        index = bisect.bisect_right(self.starts, moment) - 1
        if index < 0:
            return 0.0
        inside = (min(moment, self.ends[index]) - self.starts[index]).total_seconds()
        return self.seconds_before[index] + inside

    def share(self, start: datetime, end: datetime) -> float:
        """The share of [start, end] the feed was up, from 0 to 1."""
        length = (end - start).total_seconds()
        if length <= 0:
            return 0.0
        return (self._up_seconds_until(end) - self._up_seconds_until(start)) / length


def resale_window(listed_at: datetime, config: LabelConfig) -> tuple[datetime, datetime]:
    """The window whose sales price a listing's resale: [listed_at + trade hold, listed_at + horizon]."""
    return (
        listed_at + timedelta(seconds=config.fees.hold_seconds),
        listed_at + timedelta(seconds=config.horizon_seconds),
    )


def is_covered(listed_at: datetime, config: LabelConfig, feed_coverage: FeedCoverage | None) -> bool:
    """Whether a listing seen at `listed_at` can be labeled under `config`: always, unless the version asks for
    feed coverage, then only when the feed was up for enough of the listing's resale window."""
    if not config.needs_feed_coverage:
        return True
    if feed_coverage is None:
        raise ValueError(f"label version {config.version} needs the feed coverage; pass feed_coverage")
    return feed_coverage.share(*resale_window(listed_at, config)) >= config.min_feed_coverage


@dataclass
class ItemFees:
    """Each baseline build's seller fee per item, oldest build first, to find the fee in force at a moment."""

    # When each build was stored, the moment it came into force.
    stored_at: list[datetime] = field(default_factory=list)
    fees_by_build: list[dict[str, int]] = field(default_factory=list)

    @classmethod
    def build(cls, builds: Iterable[tuple[datetime, dict[str, int]]]) -> "ItemFees":
        """From (stored_at, fee per item) pairs, in any order."""
        ordered = sorted(builds, key=lambda build: build[0])
        return cls(stored_at=[stored_at for stored_at, _fees in ordered], fees_by_build=[fees for _stored_at, fees in ordered])

    def fee_bps(self, name: str, at: datetime) -> int | None:
        """The seller fee of `name` in the newest build stored at or before `at`; None without one."""
        index = bisect.bisect_right(self.stored_at, at) - 1
        if index < 0:
            return None
        return self.fees_by_build[index].get(name)


def first_sighting_per_listing(sales: Iterable[FeedSale]) -> dict[str, FeedSale]:
    """Keeps the earliest sighting of each listing ID (ties broken by the lower price)."""
    first: dict[str, FeedSale] = {}
    for sale in sales:
        current = first.get(sale.listing_id)
        if current is None or (sale.seen_at, sale.price_cents) < (current.seen_at, current.price_cents):
            first[sale.listing_id] = sale
    return first


def label_listing(listing: FeedSale, sold: SoldIndex, config: LabelConfig, item_fees: ItemFees | None = None) -> dict[str, Any]:
    """Builds the `listing_outcomes` row for one listing. Uses no sale after listed_at + horizon.

    With `config.item_seller_fees` the resale pays the item's fee from `item_fees` as of the listing time."""
    if config.item_seller_fees and item_fees is None:
        raise ValueError(f"label version {config.version} prices resales with item fees; pass item_fees")
    listed_at = listing.seen_at
    resale_window_start, label_available_at = resale_window(listed_at, config)

    # A sale of this listing counts only inside [listed_at, label_available_at]. No sold event means
    # "not seen to sell" (the feed never reports cancellations or price changes): censored. Without sold
    # events, the listing's sale is the first sale of its own item at its price (`own_sale_by_item`).
    if config.own_sale_by_item:
        own_sale = sold.item_sale(listing, listed_at - timedelta(seconds=OWN_SALE_CLOCK_SKEW_SECONDS), label_available_at)
    else:
        own_sale = sold.first_sold_by_listing.get(listing.listing_id)
    own_sale_id = own_sale.listing_id if own_sale is not None else listing.listing_id

    comparable = sold.comparable_prices(listing.market_hash_name, resale_window_start, label_available_at, own_sale_id)
    resale_price_cents: int | None = None
    net_margin_cents: int | None = None
    is_profitable: bool | None = None
    if len(comparable) >= config.min_comparable_sales:
        # Lower median: the price a resale at the typical market level would have fetched. The
        # single best sale in the window would reward outliers that a seller could not count on.
        resale_price_cents = statistics.median_low(comparable)
        item_fee_bps = item_fees.fee_bps(listing.market_hash_name, listed_at) if item_fees is not None else None
        net_margin_cents = net_resale_margin_cents(
            listing.price_cents,
            resale_price_cents,
            buy_fees=config.fees,
            sell_fees=config.fees,
            sell_item_fee_bps=item_fee_bps if config.item_seller_fees else None,
        )
        is_profitable = is_profitable_margin(net_margin_cents, config.fees)

    listing_sold_within_s: int | None = None
    if own_sale is not None and config.own_sale_by_item:
        # The window already allowed for the clock skew; a sale a few seconds "before" listed_at sold at once.
        listing_sold_within_s = max(0, int((own_sale.seen_at - listed_at).total_seconds()))
    elif own_sale is not None and listed_at <= own_sale.seen_at <= label_available_at:
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


def latest_labelable(as_of: datetime, config: LabelConfig, sales_synced_at: datetime | None = None) -> datetime:
    """Listings seen after this moment cannot be labeled yet as of `as_of`: their horizon (plus settle time) is open.

    With sales from `venue_sales`, `sales_synced_at` is when the newest completed sales sync started (the newest
    baseline build of the venue). A listing waits until that sync began `sales_settle_seconds` after its horizon;
    without any sync nothing is labelable."""
    cutoff = as_of - timedelta(seconds=config.horizon_seconds + config.settle_seconds)
    if config.sales_source != SALES_FROM_VENUE_SALES:
        return cutoff
    if sales_synced_at is None:
        return datetime.min
    return min(cutoff, sales_synced_at - timedelta(seconds=config.horizon_seconds + config.sales_settle_seconds))


@dataclass(frozen=True)
class LabeledListings:
    """The labels of one batch of listing sightings, and how many matured listings got none for lack of feed."""

    rows: list[dict[str, Any]]
    uncovered: int = 0


def label_listings(
    listed_sales: Iterable[FeedSale],
    sold_sales: Iterable[FeedSale] | SoldIndex,
    config: LabelConfig,
    as_of: datetime,
    item_fees: ItemFees | None = None,
    sales_synced_at: datetime | None = None,
    feed_coverage: FeedCoverage | None = None,
) -> LabeledListings:
    """Labels every listing whose horizon (plus settle time) has passed by `as_of` (see `latest_labelable`) and,
    for a version that asks for it, whose resale window the feed covered (`is_covered`)."""
    cutoff = latest_labelable(as_of, config, sales_synced_at)
    sold = sold_sales if isinstance(sold_sales, SoldIndex) else SoldIndex.build(sold_sales)
    matured = [listing for listing in first_sighting_per_listing(listed_sales).values() if listing.seen_at <= cutoff]
    covered = [listing for listing in matured if is_covered(listing.seen_at, config, feed_coverage)]
    rows = [label_listing(listing, sold, config, item_fees) for listing in covered]
    rows.sort(key=lambda row: (row["listed_at"], row["listing_id"]))
    return LabeledListings(rows=rows, uncovered=len(matured) - len(covered))


def iter_windows(start: datetime, end: datetime, window: timedelta) -> list[tuple[datetime, datetime]]:
    """Splits [start, end) into consecutive windows of at most `window`."""
    windows = []
    cursor = start
    while cursor < end:
        window_end = min(cursor + window, end)
        windows.append((cursor, window_end))
        cursor = window_end
    return windows


def default_start(as_of: datetime, days_back: int, config: LabelConfig, sales_synced_at: datetime | None = None) -> datetime:
    """Start of the most recent `days_back` days of listings whose horizon has passed by `as_of`.

    Re-labeling a few matured days on each daily run picks up batches that reached the database late. With sales
    from `venue_sales` the days end where the newest sales sync lets labels reach (`latest_labelable`)."""
    if config.sales_source == SALES_FROM_VENUE_SALES and sales_synced_at is None:
        raise ValueError(f"label version {config.version} needs the time of the newest {config.venue} sales sync")
    return latest_labelable(as_of, config, sales_synced_at) - timedelta(days=days_back)


# `v1`: the rule of `v2` without the feed coverage. Registered on 2026-10-13; reading its labels needs no config,
# this is for code that labels with it.
LABEL_V1 = LabelConfig.for_venue(LABEL_VENUE, LABEL_V1_VERSION)
