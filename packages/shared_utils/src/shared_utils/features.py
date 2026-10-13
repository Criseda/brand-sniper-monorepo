"""
The student model's features (#234): one definition shared by the replay harness, the training flow and,
later, the live listener (#236), so a feature means the same thing wherever it is computed.

Every feature comes from what the listener knows when it decides on a listing: the price window as
`shared_utils.price_window` defines it (#269), the baseline build in effect, the listing itself, the
sticker prices of the resale venue and the listings seen before it. Nothing here reads a later price.

The Z score and the window mean are the listener's own (`zscore.calculate_z_score` on the price window),
passed in by the caller, so the model sees exactly the numbers the rules see. Changing a feature, or
adding one, bumps FEATURE_VERSION and updates section 4.5 of docs/roadmap_proven_edge.md.
"""

import math
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

FEATURE_VERSION = 1

# The model's input columns, in order. The ONNX model takes them as one float32 row per listing.
FEATURE_NAMES = (
    "price_to_window_mean",
    "z_score",
    "z_source",
    "price_to_30d_avg",
    "price_to_support_floor",
    "macro_cv",
    "avg_volume_30d",
    "float_value",
    "float_tier_boundary_distance",
    "sticker_value_ratio",
    "listings_per_hour_item",
)

# `z_source` as a number: how the Z score was computed (zscore.ZSCORE_SOURCES). Missing when unscored.
Z_SOURCE_CODES = {"local": 0, "hybrid": 1, "macro": 2}

# Lower float bound of each wear tier: Factory New, Minimal Wear, Field-Tested, Well-Worn, Battle-Scarred.
WEAR_TIER_LOWER_BOUNDS = (0.0, 0.07, 0.15, 0.38, 0.45)

# `listings_per_hour_item` counts the item's listings on the venue in this many seconds before the listing.
LISTING_RATE_WINDOW_SECONDS = 3600


def float_tier_boundary_distance(float_value: float | None) -> float | None:
    """How far a float sits above the lower bound of its wear tier; 0 is the best float in the tier."""
    if float_value is None:
        return None
    lower_bound = max(bound for bound in WEAR_TIER_LOWER_BOUNDS if bound <= float_value)
    return float_value - lower_bound


def _ratio(numerator: float, denominator: float | None) -> float | None:
    if denominator is None or denominator <= 0:
        return None
    return numerator / denominator


@dataclass(frozen=True, slots=True)
class FeatureInputs:
    """What the listener knows about one listing when it decides on it."""

    price_cents: int
    z_score: float | None  # None when the window could not be scored
    mean_cents: float | None  # The mean the Z score compared against (window, or 30 day average for macro)
    z_source: str | None  # local, hybrid or macro
    baseline: Mapping[str, Any] | None  # The edge baseline document of the resale venue
    float_value: float | None
    sticker_value_cents: int  # Sum of the resale venue's prices of the applied stickers
    listings_last_hour: int  # The item's listings on the venue in the hour before this one


def compute_features(inputs: FeatureInputs) -> dict[str, float | int | None]:
    """The features of one listing, keyed by name; None where the input is missing."""
    baseline = inputs.baseline or {}
    price = inputs.price_cents
    latest_price = baseline.get("latest_price_cents")
    return {
        "price_to_window_mean": _ratio(price, inputs.mean_cents),
        "z_score": inputs.z_score,
        "z_source": Z_SOURCE_CODES.get(inputs.z_source) if inputs.z_source is not None else None,
        "price_to_30d_avg": _ratio(price, baseline.get("rolling_30d_avg_cents")),
        "price_to_support_floor": _ratio(price, baseline.get("support_floor_cents")),
        "macro_cv": baseline.get("coefficient_of_variation"),
        "avg_volume_30d": baseline.get("avg_volume_30d"),
        "float_value": inputs.float_value,
        "float_tier_boundary_distance": float_tier_boundary_distance(inputs.float_value),
        "sticker_value_ratio": _ratio(inputs.sticker_value_cents, latest_price) if latest_price else None,
        "listings_per_hour_item": inputs.listings_last_hour,
    }


def feature_vector(features: Mapping[str, Any], names: Sequence[str] = FEATURE_NAMES) -> list[float]:
    """The model input row: features in `names` order, a missing feature as NaN."""
    row = []
    for name in names:
        value = features.get(name)
        row.append(math.nan if value is None else float(value))
    return row


class ListingRateTracker:
    """Counts each item's listings over the last hour. Keys are (venue, item); times are epoch seconds."""

    def __init__(self, window_seconds: int = LISTING_RATE_WINDOW_SECONDS) -> None:
        self._window_seconds = window_seconds
        self._times: dict[tuple[str, str], deque[int]] = {}

    def record(self, venue: str, market_hash_name: str, timestamp: int) -> int:
        """Adds a listing and returns how many of the item's listings came in the window before it."""
        times = self._times.setdefault((venue, market_hash_name), deque())
        while times and timestamp - times[0] >= self._window_seconds:
            times.popleft()
        earlier = len(times)
        times.append(timestamp)
        return earlier
