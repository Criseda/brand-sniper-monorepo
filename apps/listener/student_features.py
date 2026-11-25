"""
Student model features (#234) of one listing, gathered from the edge store at decision time.

The definitions live in `shared_utils.features`; this module only collects their inputs from the same
places the rules read them (the scored price window, the resale venue's baseline and sticker prices), so
the replay harness and the live listener's shadow mode (#236) build identical rows.
"""

import json
from typing import Any

from detection import WindowScore
from models import MarketTick
from redis.asyncio import Redis
from rules_engine import sticker_value_cents
from shared_utils import FeatureInputs, ListingRateTracker, compute_features, edge_baselines_key, resale_venue_for


async def _baseline(tick: MarketTick, cache: Redis) -> dict[str, Any] | None:
    raw = await cache.hget(edge_baselines_key(resale_venue_for(tick.venue)), tick.market_hash_name)
    return json.loads(raw) if raw else None


def record_listing(tick: MarketTick, rates: ListingRateTracker) -> int:
    """Records a listing in `rates` and returns its item's listings over the last hour, this one included.

    Call it once per scored listing, in decision order: the live shadow mode (#236) calls it on the tick
    consumer before handing the rest of the work to a background task, so the count matches the replay."""
    return rates.record(tick.venue, tick.market_hash_name, tick.timestamp)


async def features_with_rate(
    tick: MarketTick, window_score: WindowScore | None, cache: Redis, listings_last_hour: int
) -> dict[str, float | int | None]:
    """The features of a listing whose rate `record_listing` has already counted."""
    baseline = window_score.baseline if window_score is not None else await _baseline(tick, cache)
    return compute_features(
        FeatureInputs(
            price_cents=tick.price_cents,
            z_score=window_score.z_score if window_score is not None else None,
            mean_cents=window_score.mean_cents if window_score is not None else None,
            z_source=window_score.source if window_score is not None else None,
            baseline=baseline,
            float_value=tick.float_value,
            sticker_value_cents=await sticker_value_cents(tick, cache),
            listings_last_hour=listings_last_hour,
        )
    )


async def listing_features(
    tick: MarketTick, window_score: WindowScore | None, cache: Redis, rates: ListingRateTracker
) -> dict[str, float | int | None]:
    """
    The features of a listing that has just entered its price window.

    Call it once per scored listing, in decision order: it also records the listing in `rates`.
    """
    return await features_with_rate(tick, window_score, cache, record_listing(tick, rates))
