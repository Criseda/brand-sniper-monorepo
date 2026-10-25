"""
What a listener records from a venue whose listings are too many to record in full (Waxpeer, DMarket).

Such a venue's listings are judged against an item price the edge Redis already holds (the latest baseline
price of the venue the listing would be resold on), and only those that could be bought for less are
recorded and scored. Everything else is counted in `listener_feed_events_filtered_total` and dropped. The
raw payloads of the recorded listings are grouped into one feed_events row at a time.
"""

import json
from collections.abc import Mapping
from typing import Any

from models import FeedEvent, MarketTick
from redis.asyncio import Redis
from rules_engine import STICKER_PREMIUM_MAX_PERCENT, STICKER_VALUE_MIN_CENTS
from shared_utils import edge_baseline_meta_key, edge_baselines_key, edge_sticker_prices_key

# What the recording filter did with a feed event (the `outcome` label of listener_feed_events_filtered_total).
OUTCOME_RECORDED = "recorded"
OUTCOME_NO_BASELINE = "no_baseline"  # The resale venue has no baseline for the item
OUTCOME_ABOVE_RESALE_PRICE = "above_resale_price"  # Not below the item's resale price
OUTCOME_MALFORMED = "malformed"

# Raw payloads of recorded events are grouped into one feed_events row, so PostgreSQL compresses them.
RAW_BATCH_EVENT_TYPE = "batch"
RAW_BATCH_MAX_EVENTS = 500
RAW_BATCH_MAX_SECONDS = 10


def _text(value: str | bytes) -> str:
    """A Redis reply as str, whether or not the client decodes responses."""
    return value.decode("utf-8") if isinstance(value, bytes) else value


class ResalePrices:
    """
    The resale venue's latest price per item and its sticker prices, held in memory for the recording filter.

    Read from the edge Redis hashes the baseline loader fills, and read again whenever the loader has stored a
    different build. The feed is too fast to ask Redis once per event.
    """

    def __init__(self, venue: str) -> None:
        self.venue = venue
        self.build_id: str | None = None
        self.latest_price_cents: dict[str, int] = {}
        self.sticker_prices_cents: dict[str, int] = {}

    async def refresh(self, cache: Redis) -> bool:
        """Loads the build in the edge Redis when it differs from the one held. Returns True when it loaded one."""
        meta = {
            _text(field): _text(value) for field, value in (await cache.hgetall(edge_baseline_meta_key(self.venue))).items()
        }
        build_id = meta.get("build_id")
        if build_id is None or build_id == self.build_id:
            return False
        baselines = await cache.hgetall(edge_baselines_key(self.venue))
        sticker_prices = await cache.hgetall(edge_sticker_prices_key(self.venue))

        latest_price_cents: dict[str, int] = {}
        for name, document in baselines.items():
            try:
                price = json.loads(document).get("latest_price_cents")
            except (ValueError, AttributeError):
                continue
            if isinstance(price, int) and price > 0:
                latest_price_cents[_text(name)] = price
        sticker_prices_cents: dict[str, int] = {}
        for name, price_text in sticker_prices.items():
            try:
                sticker_prices_cents[_text(name)] = int(price_text)
            except ValueError:
                continue

        self.latest_price_cents = latest_price_cents
        self.sticker_prices_cents = sticker_prices_cents
        self.build_id = build_id
        return True

    def judge(self, tick: MarketTick) -> str:
        """
        OUTCOME_RECORDED when the listing is worth recording and scoring, else why not.

        A listing is worth it when it costs less than the item's resale price, or when its stickers are worth
        enough that the DRE's sticker rules could approve it above that price.
        """
        resale_price_cents = self.latest_price_cents.get(tick.market_hash_name)
        if resale_price_cents is None:
            return OUTCOME_NO_BASELINE
        if tick.price_cents < resale_price_cents:
            return OUTCOME_RECORDED
        sticker_value_cents = sum(self.sticker_prices_cents.get(sticker.get("name", ""), 0) for sticker in tick.stickers)
        premium_cents = tick.price_cents - resale_price_cents
        if (
            sticker_value_cents > STICKER_VALUE_MIN_CENTS
            and premium_cents * 100 <= STICKER_PREMIUM_MAX_PERCENT * sticker_value_cents
        ):
            return OUTCOME_RECORDED
        return OUTCOME_ABOVE_RESALE_PRICE


class RawEventBatch:
    """Raw payloads of recorded events, handed on as one FeedEvent per RAW_BATCH_MAX_EVENTS or RAW_BATCH_MAX_SECONDS."""

    def __init__(
        self,
        venue: str,
        context: Mapping[str, Any] | None = None,
        max_events: int = RAW_BATCH_MAX_EVENTS,
        max_seconds: float = RAW_BATCH_MAX_SECONDS,
    ) -> None:
        self.venue = venue
        # Written into every batch payload next to the venue, such as the feed channel the events came from.
        self.context = dict(context or {})
        self.max_events = max_events
        self.max_age_ms = int(max_seconds * 1000)
        self._events: list[dict[str, Any]] = []

    def add(self, event: str, payload: Any, received_at_ms: int) -> None:
        self._events.append({"event": event, "received_at_ms": received_at_ms, "data": payload})

    def take_if_due(self, now_ms: int) -> FeedEvent | None:
        if not self._events:
            return None
        oldest_ms = self._events[0]["received_at_ms"]
        if len(self._events) >= self.max_events or now_ms - oldest_ms >= self.max_age_ms:
            return self.take()
        return None

    def take(self) -> FeedEvent | None:
        """The pending events as one FeedEvent (received at the first event's time), or None when empty."""
        if not self._events:
            return None
        events, self._events = self._events, []
        return FeedEvent(
            event_type=RAW_BATCH_EVENT_TYPE,
            received_at_ms=events[0]["received_at_ms"],
            payload={"venue": self.venue, **self.context, "events": events},
        )
