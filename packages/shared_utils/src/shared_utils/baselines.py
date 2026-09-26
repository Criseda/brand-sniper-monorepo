"""
Per venue item baselines: the normal price record the listener's Z score and DRE compare a listing
against, built from recent sales on that venue.

Skinport baselines come from one `/v1/sales/history` entry per item (min, max, avg, median and volume
over the last 24 hours, 7, 30 and 90 days). The mapping is described in docs/data_sources.md:

- latest price: 24 hour median when the day had enough sales, else the 7 day median, else the 30 day median
- 30 and 90 day averages: the 30 and 90 day medians (sticker and pattern premiums pull the mean up)
- volume: 30 day sales divided by 30
- volatility and support floor: from the item's own daily medians once enough daily builds exist,
  otherwise estimated from the 30 day median and minimum (see `sales_spread_volatility_cents`)

An item with fewer than `MIN_SALES_30D` sales in 30 days gets no baseline, so the DRE skips it.
"""

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from .item_classifier import build_versioned_name
from .pricing_utils import edge_baseline_payload, to_cents

BASELINE_METHOD = "sales-history-v1"

MIN_SALES_30D = 5
MIN_SALES_24H_FOR_LATEST = 5
MIN_SALES_7D_FOR_LATEST = 3
# Daily medians needed (within the last 30 days) before volatility and floor use them.
MIN_DAILY_POINTS = 14
# The support floor sits at the 10th percentile of the price distribution: median - 1.2816 sigma.
FLOOR_Z = 1.2816
# Volatility is never reported below 1% of the median, matching the Z score's own minimum.
MIN_VOLATILITY_FRACTION = 0.01

VOLATILITY_FROM_SALES_SPREAD = "sales_spread"
VOLATILITY_FROM_DAILY_MEDIANS = "daily_medians"

STICKER_ITEM_PREFIX = "Sticker | "

_WINDOW_KEYS = ("last_24_hours", "last_7_days", "last_30_days", "last_90_days")


def edge_baselines_key(venue: str) -> str:
    """Redis hash holding every item's baseline document for one venue (field: versioned item name)."""
    return f"baselines:{venue}"


def edge_sticker_prices_key(venue: str) -> str:
    """Redis hash of sticker prices in cents for one venue (field: sticker name as it appears on a listing)."""
    return f"sticker_prices:{venue}"


def edge_baseline_meta_key(venue: str) -> str:
    """Redis hash describing the build loaded for one venue (build_id, built_at, item_count)."""
    return f"baseline_meta:{venue}"


def applied_sticker_name(market_hash_name: str) -> str | None:
    """
    The name a sticker has when it is applied to a listed item, or None when the item is not a sticker.

    Venues list the sticker item as "Sticker | Crown (Foil)", but a listing's `stickers` array names it
    "Crown (Foil)", so sticker prices are keyed by the name without the prefix.
    """
    if not market_hash_name.startswith(STICKER_ITEM_PREFIX):
        return None
    return market_hash_name.removeprefix(STICKER_ITEM_PREFIX)


@dataclass(frozen=True, slots=True)
class SalesWindow:
    """Sale statistics over one time window, in cents."""

    min_cents: int | None
    max_cents: int | None
    avg_cents: int | None
    median_cents: int | None
    volume: int

    @classmethod
    def from_skinport(cls, window: object) -> "SalesWindow":
        if not isinstance(window, Mapping):
            return cls(None, None, None, None, 0)

        def cents(key: str) -> int | None:
            value = window.get(key)
            return to_cents(value) if isinstance(value, (int, float)) else None

        volume = window.get("volume")
        return cls(
            min_cents=cents("min"),
            max_cents=cents("max"),
            avg_cents=cents("avg"),
            median_cents=cents("median"),
            volume=volume if isinstance(volume, int) and volume > 0 else 0,
        )

    def has_median(self, min_volume: int = 1) -> bool:
        return self.median_cents is not None and self.median_cents > 0 and self.volume >= min_volume


@dataclass(frozen=True, slots=True)
class SalesHistory:
    """One item's recent sales on a venue."""

    market_hash_name: str  # Versioned name (phase included), the same key the listener's ticks carry
    last_24_hours: SalesWindow
    last_7_days: SalesWindow
    last_30_days: SalesWindow
    last_90_days: SalesWindow

    @classmethod
    def from_skinport(cls, entry: Mapping[str, Any]) -> "SalesHistory | None":
        """Parse one `/v1/sales/history` entry; None when it has no usable name or is not priced in USD."""
        name = entry.get("market_hash_name")
        if not isinstance(name, str) or not name:
            return None
        if entry.get("currency") != "USD":
            return None
        version = entry.get("version")
        windows = [SalesWindow.from_skinport(entry.get(key)) for key in _WINDOW_KEYS]
        return cls(build_versioned_name(name, version if isinstance(version, str) else None), *windows)


@dataclass(frozen=True, slots=True)
class ItemBaseline:
    """One item's baseline in one build, with the window statistics it was built from."""

    market_hash_name: str
    latest_price_cents: int
    rolling_30d_avg_cents: int
    rolling_90d_avg_cents: int
    volatility_cents: int
    support_floor_cents: int
    avg_volume_30d: float
    drift_percent: float
    volatility_method: str
    median_24h_cents: int | None
    volume_24h: int
    median_7d_cents: int | None
    volume_7d: int
    min_30d_cents: int
    volume_30d: int
    volume_90d: int

    def edge_payload(self) -> dict[str, Any]:
        """The document the edge reads for this item."""
        return edge_baseline_payload(
            support_floor_cents=self.support_floor_cents,
            latest_price_cents=self.latest_price_cents,
            rolling_30d_avg_cents=self.rolling_30d_avg_cents,
            volatility_cents=self.volatility_cents,
            drift_percent=self.drift_percent,
        )


def latest_price_cents(history: SalesHistory) -> int:
    """24 hour median when that day had enough sales, else the 7 day median, else the 30 day median."""
    day = history.last_24_hours
    week = history.last_7_days
    if day.has_median(MIN_SALES_24H_FOR_LATEST) and day.median_cents is not None:
        return day.median_cents
    if week.has_median(MIN_SALES_7D_FOR_LATEST) and week.median_cents is not None:
        return week.median_cents
    median_30d = history.last_30_days.median_cents
    if median_30d is None:
        raise ValueError(f"{history.market_hash_name}: no 30 day median")
    return median_30d


def sales_spread_volatility_cents(median_cents: int, min_cents: int, volume: int) -> int:
    """
    Estimated standard deviation of sale prices from the median and minimum of `volume` sales.

    For roughly normal prices, the lowest of n sales sits about sqrt(2 ln n) standard deviations below
    the median, so sigma is close to (median - min) / sqrt(2 ln n). Only the low side is used: rare
    patterns, stickers and floats push the maximum far above the typical price, but never pull the
    minimum down. A few underpriced sales make the estimate somewhat larger, which errs towards fewer
    approvals.
    """
    if volume < 2:
        raise ValueError("volume must be at least 2")
    spread = max(median_cents - min_cents, 0)
    return round(spread / math.sqrt(2 * math.log(volume)))


def build_item_baseline(history: SalesHistory, daily_medians_cents: Sequence[int] = ()) -> ItemBaseline | None:
    """
    Build one item's baseline, or None when it sold fewer than `MIN_SALES_30D` times in 30 days.

    `daily_medians_cents` are this item's 24 hour medians from earlier builds in the last 30 days, one
    per day. With at least `MIN_DAILY_POINTS` of them, volatility is their standard deviation and the
    floor their 10th percentile, as the Kaggle pipeline computed them; otherwise both are estimated
    from this build's 30 day window.
    """
    month = history.last_30_days
    if not month.has_median(MIN_SALES_30D) or month.median_cents is None or month.min_cents is None:
        return None

    median_30d = month.median_cents
    quarter = history.last_90_days
    median_90d = quarter.median_cents if quarter.has_median() and quarter.median_cents is not None else median_30d
    minimum_volatility = max(round(median_30d * MIN_VOLATILITY_FRACTION), 1)

    if len(daily_medians_cents) >= MIN_DAILY_POINTS:
        volatility = round(statistics.stdev(daily_medians_cents))
        deciles = statistics.quantiles(daily_medians_cents, n=10, method="inclusive")
        support_floor = round(deciles[0])
        volatility_method = VOLATILITY_FROM_DAILY_MEDIANS
    else:
        volatility = sales_spread_volatility_cents(median_30d, month.min_cents, month.volume)
        support_floor = round(median_30d - FLOOR_Z * max(volatility, minimum_volatility))
        volatility_method = VOLATILITY_FROM_SALES_SPREAD

    return ItemBaseline(
        market_hash_name=history.market_hash_name,
        latest_price_cents=latest_price_cents(history),
        rolling_30d_avg_cents=median_30d,
        rolling_90d_avg_cents=median_90d,
        volatility_cents=max(volatility, minimum_volatility),
        support_floor_cents=max(support_floor, 0),
        avg_volume_30d=month.volume / 30,
        drift_percent=(median_30d - median_90d) / median_90d * 100,
        volatility_method=volatility_method,
        median_24h_cents=history.last_24_hours.median_cents if history.last_24_hours.has_median() else None,
        volume_24h=history.last_24_hours.volume,
        median_7d_cents=history.last_7_days.median_cents if history.last_7_days.has_median() else None,
        volume_7d=history.last_7_days.volume,
        min_30d_cents=month.min_cents,
        volume_30d=month.volume,
        volume_90d=quarter.volume,
    )
