"""
The listener's decision path: tick deduplication, the sliding price window, Z-score scoring, and the
hand-off to the Edge DRE.

Live, `update_window_and_detect` runs these steps against the edge Redis. The replay harness
(`backtest`) runs the same functions against an in-memory store, so both make identical decisions.
"""

import json
import os
import time
from collections import OrderedDict
from dataclasses import dataclass
from functools import partial
from typing import Any

from executor import ExecutionService
from listener_telemetry import (
    anomalies_confirmed_total,
    anomalies_detected_total,
    anomalies_rejected_total,
    dedup_cache_evictions_total,
    dedup_cache_size,
    redis_operation_latency_seconds,
    rules_engine_latency_seconds,
)
from models import MarketTick
from redis.asyncio import Redis
from rules_engine import APPROVAL_REASONS, dre_approval_reason
from shared_utils import PROFIT_ESTIMATE_BASIS_NET, edge_baselines_key, fees_for, get_logger, net_resale_margin_cents
from task_supervisor import BoundedTaskPool
from zscore import ZSCORE_SOURCES, calculate_z_score, should_trigger_anomaly

logger = get_logger("listener.main")

# --- Tunable Detection Parameters (configurable via .env) ---
# Sliding window size for Redis price history
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "20"))
# Dedup cache max entries per venue (LRU eviction above this cap). Skinport's REST poll alone returns about
# 21,300 items, so each venue gets its own cap and one venue's items never push out another's.
DEDUP_CACHE_MAX_SIZE = int(os.getenv("DEDUP_CACHE_MAX_SIZE", "25000"))
# A tick at the same price as the item's previous tick within this many seconds is a duplicate.
DEDUP_WINDOW_SECONDS = 300

# Dedup state and price windows belong to one item on one venue: venues differ in fees and price levels,
# so their prices never share a window (#270). Written to the replay log header.
STATE_KEY = "venue_and_item"
PRICE_WINDOW_KEY_PREFIX = "price_window:"
# Before #270 a window was keyed by item name only, and every window held Skinport prices.
LEGACY_PRICE_WINDOW_KEY_PREFIX = "market:ticks:"
LEGACY_PRICE_WINDOW_VENUE = "skinport"

# `tick_kind` label on the anomaly counters: a REST lowest ask or a live feed listing.
TICK_KIND_REST_SNAPSHOT = "rest_snapshot"
TICK_KIND_LISTING = "listing"
TICK_KINDS = (TICK_KIND_REST_SNAPSHOT, TICK_KIND_LISTING)


@dataclass(frozen=True, slots=True)
class DedupEntry:
    """What the dedup rules remember about one item on one venue."""

    timestamp: int  # Time of the item's last tick that was not a duplicate
    price_cents: int  # Price of that tick
    snapshot_price_cents: int | None  # Price of the item's last REST snapshot; None before the first one


class DedupCache:
    """What the dedup rules remember, per venue and item, in one LRU cache per venue."""

    def __init__(self) -> None:
        self._venues: dict[str, OrderedDict[str, DedupEntry]] = {}

    def get(self, tick: MarketTick) -> DedupEntry | None:
        entries = self._venues.get(tick.venue)
        return entries.get(tick.market_hash_name) if entries is not None else None

    def put(self, tick: MarketTick, entry: DedupEntry) -> int:
        """Stores the entry as the venue's most recent one; returns how many of the venue's entries were evicted."""
        entries = self._venues.setdefault(tick.venue, OrderedDict())
        entries[tick.market_hash_name] = entry
        entries.move_to_end(tick.market_hash_name)
        evicted = 0
        while len(entries) > DEDUP_CACHE_MAX_SIZE:
            entries.popitem(last=False)
            evicted += 1
        return evicted

    def size(self, venue: str) -> int:
        return len(self._venues.get(venue, ()))

    def items(self, venue: str) -> list[str]:
        """The venue's item names, least recently used first."""
        return list(self._venues.get(venue, ()))


@dataclass(frozen=True, slots=True)
class WindowScore:
    """Z-score of the newest price in an item's window, plus the baseline it was scored against."""

    z_score: float
    mean_cents: float
    source: str  # local, hybrid, or macro (see zscore.calculate_z_score)
    window_size: int
    baseline: dict[str, Any] | None


def _decode_zset_element(element: str | bytes) -> str:
    """Decodes a Redis sorted-set member to str regardless of client decode mode."""
    return element.decode("utf-8") if isinstance(element, bytes) else element


def tick_kind_label(tick: MarketTick) -> str:
    return TICK_KIND_REST_SNAPSHOT if tick.is_rest_snapshot else TICK_KIND_LISTING


def initialise_anomaly_counters() -> None:
    """
    Creates every label combination of the anomaly counters at zero.

    Prometheus `increase()` cannot see the first increment of a series that appears already at 1, and
    approvals are rare enough that losing the first one per rule, source and tick kind would show.
    """
    for source in ZSCORE_SOURCES:
        for tick_kind in TICK_KINDS:
            anomalies_detected_total.labels(source=source, tick_kind=tick_kind)
            anomalies_rejected_total.labels(source=source, tick_kind=tick_kind)
            for reason in APPROVAL_REASONS:
                anomalies_confirmed_total.labels(source=source, tick_kind=tick_kind, reason=reason)


def price_window_key(venue: str, market_hash_name: str) -> str:
    """Edge Redis key of one item's price window on one venue."""
    return f"{PRICE_WINDOW_KEY_PREFIX}{venue}:{market_hash_name}"


async def migrate_legacy_price_windows(cache: Redis) -> int:
    """
    Moves the price windows written before #270 (`market:ticks:<item>`) to their venue keyed names.

    Every legacy window holds Skinport prices, so it becomes `price_window:skinport:<item>` and the listener
    keeps its history across the deploy. When a window already exists under the new name, it is newer, so it
    is kept and the legacy one is dropped. Returns how many legacy windows were found.
    """
    found = 0
    async for key in cache.scan_iter(match=f"{LEGACY_PRICE_WINDOW_KEY_PREFIX}*", count=1000):
        key_text = _decode_zset_element(key)
        market_hash_name = key_text.removeprefix(LEGACY_PRICE_WINDOW_KEY_PREFIX)
        if not await cache.renamenx(key_text, price_window_key(LEGACY_PRICE_WINDOW_VENUE, market_hash_name)):
            await cache.delete(key_text)
        found += 1
    return found


def is_duplicate(tick: MarketTick, dedup_cache: DedupCache) -> bool:
    """Returns True if this tick is a duplicate (same price within the dedup window)."""
    entry = dedup_cache.get(tick)
    if entry is None:
        return False
    return tick.price_cents == entry.price_cents and (tick.timestamp - entry.timestamp) < DEDUP_WINDOW_SECONDS


def is_unchanged_snapshot(tick: MarketTick, dedup_cache: DedupCache) -> bool:
    """
    Returns True for a REST snapshot at the same price as the item's previous REST snapshot, however old.

    Polls are further apart than the dedup window, so `is_duplicate` never drops these. The price still
    enters the window as before, but it is not scored again: the lowest ask has not changed, so scoring it
    would repeat the previous decision (and its paper trade). Check it before `update_dedup_cache`.
    """
    entry = dedup_cache.get(tick)
    return entry is not None and tick.is_rest_snapshot and tick.price_cents == entry.snapshot_price_cents


def update_dedup_cache(tick: MarketTick, dedup_cache: DedupCache) -> None:
    """Updates the venue's LRU dedup cache with the latest tick, evicting the oldest items over capacity."""
    previous = dedup_cache.get(tick)
    if tick.is_rest_snapshot:
        snapshot_price_cents: int | None = tick.price_cents
    else:
        snapshot_price_cents = previous.snapshot_price_cents if previous is not None else None

    entry = DedupEntry(timestamp=tick.timestamp, price_cents=tick.price_cents, snapshot_price_cents=snapshot_price_cents)
    evicted = dedup_cache.put(tick, entry)
    if evicted:
        # An evicted item that never changes price is scored again on its next snapshot, so this should stay 0.
        dedup_cache_evictions_total.labels(venue=tick.venue).inc(evicted)
    dedup_cache_size.labels(venue=tick.venue).set(dedup_cache.size(tick.venue))


async def push_to_window(tick: MarketTick, cache: Redis) -> None:
    """Adds the tick's price to the item's sliding window and trims the window to its last N prices."""
    redis_key = price_window_key(tick.venue, tick.market_hash_name)
    value_string = f"{tick.timestamp}:{tick.price_cents}"
    _t0 = time.monotonic()
    await cache.zadd(redis_key, {value_string: tick.timestamp})
    redis_operation_latency_seconds.observe(time.monotonic() - _t0)

    # Keep only the last N ticks
    _t1 = time.monotonic()
    card = await cache.zcard(redis_key)
    redis_operation_latency_seconds.observe(time.monotonic() - _t1)
    if card > SLIDING_WINDOW_SIZE:
        _t2 = time.monotonic()
        await cache.zremrangebyrank(redis_key, 0, card - SLIDING_WINDOW_SIZE - 1)
        redis_operation_latency_seconds.observe(time.monotonic() - _t2)


async def score_window(tick: MarketTick, cache: Redis) -> WindowScore | None:
    """Scores the newest price in the tick's item window; None when there is too little history."""
    redis_key = price_window_key(tick.venue, tick.market_hash_name)
    _t3 = time.monotonic()
    raw_elements = await cache.zrange(redis_key, 0, -1)
    redis_operation_latency_seconds.observe(time.monotonic() - _t3)
    prices = [int(_decode_zset_element(element).split(":")[1]) for element in raw_elements if isinstance(element, (str, bytes))]

    # Fetch the venue's baseline for the volatility-aware Z-score (Layers 1-2)
    _t4 = time.monotonic()
    baseline_raw = await cache.hget(edge_baselines_key(tick.venue), tick.market_hash_name)
    redis_operation_latency_seconds.observe(time.monotonic() - _t4)
    baseline_data: dict[str, Any] | None = json.loads(baseline_raw) if baseline_raw else None

    macro_avg = baseline_data.get("rolling_30d_avg_cents") if baseline_data else None
    macro_vol = baseline_data.get("volatility_cents") if baseline_data else None
    macro_cv = baseline_data.get("coefficient_of_variation") if baseline_data else None

    result = calculate_z_score(prices, macro_avg, macro_vol, macro_cv)
    if result is None:
        return None
    z_score, mean_cents, source = result
    return WindowScore(z_score=z_score, mean_cents=mean_cents, source=source, window_size=len(prices), baseline=baseline_data)


def estimate_net_profit_cents(tick: MarketTick, baseline: dict[str, Any]) -> int | None:
    """Fee-aware estimate: resell at the baseline price, after the venue's seller fee.

    Without a baseline price there is nothing to resell against, so there is no estimate.
    """
    resale_price_cents = baseline.get("latest_price_cents")
    if resale_price_cents is None:
        return None
    return net_resale_margin_cents(
        buy_price_cents=tick.price_cents,
        resale_price_cents=resale_price_cents,
        fees=fees_for(tick.venue),
    )


async def evaluate_and_execute(
    tick: MarketTick,
    z_score: float,
    cache: Redis,
    executor: ExecutionService,
    baseline: dict | None = None,
    source: str = "local",
) -> None:
    """Evaluates an anomaly locally on the edge and executes the trade if valid."""
    _dre_t0 = time.monotonic()
    approval_reason = await dre_approval_reason(tick, cache, baseline)
    rules_engine_latency_seconds.observe(time.monotonic() - _dre_t0)
    tick_kind = tick_kind_label(tick)
    if approval_reason is not None:
        anomalies_confirmed_total.labels(source=source, tick_kind=tick_kind, reason=approval_reason).inc()
        logger.info(
            "[ANOMALY] Confirmed true outlier by Edge DRE (%s, %s, rule %s)! %s dropped to $%.2f. Executing trade (Z=%.2f)...",
            source,
            tick_kind,
            approval_reason,
            tick.market_hash_name,
            tick.price_usd,
            z_score,
        )

        if baseline is None:
            baseline_raw = await cache.hget(edge_baselines_key(tick.venue), tick.market_hash_name)
            baseline = json.loads(baseline_raw) if baseline_raw else {}

        await executor.execute(
            market_hash_name=tick.market_hash_name,
            purchase_price_cents=tick.price_cents,
            estimated_profit_cents=estimate_net_profit_cents(tick, baseline),
            profit_estimate_basis=PROFIT_ESTIMATE_BASIS_NET,
            z_score=z_score,
            listing_id=tick.listing_id,
            float_value=tick.float_value,
        )
    else:
        anomalies_rejected_total.labels(source=source, tick_kind=tick_kind).inc()
        logger.info(
            "[ANOMALY] False outlier filtered by Edge DRE (%s, %s): %s at $%.2f.",
            source,
            tick_kind,
            tick.market_hash_name,
            tick.price_usd,
        )


async def update_window_and_detect(
    tick: MarketTick,
    cache: Redis,
    anomaly_pool: BoundedTaskPool,
    executor: ExecutionService,
) -> None:
    """Pushes a tick into the sliding price window and hands Z-score outliers to the Edge DRE."""
    await push_to_window(tick, cache)
    score = await score_window(tick, cache)
    if score is None:
        return
    if not should_trigger_anomaly(score.z_score, score.mean_cents, tick, score.source):
        return

    sticker_count = len(tick.stickers)
    sticker_tag = f" ({sticker_count} stickers)" if sticker_count > 0 else ""
    logger.info(
        "[ANOMALY] Outlier potential detected (%s): %s%s at $%.2f (Z=%.2f). Running Edge DRE...",
        score.source,
        tick.market_hash_name,
        sticker_tag,
        tick.price_usd,
        score.z_score,
    )
    anomalies_detected_total.labels(source=score.source, tick_kind=tick_kind_label(tick)).inc()
    await anomaly_pool.submit(
        partial(
            evaluate_and_execute,
            tick,
            score.z_score,
            cache,
            executor,
            score.baseline,
            score.source,
        )
    )
