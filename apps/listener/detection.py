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
    redis_operation_latency_seconds,
    rules_engine_latency_seconds,
)
from models import MarketTick
from redis.asyncio import Redis
from rules_engine import evaluate_opportunity
from shared_utils import PROFIT_ESTIMATE_BASIS_NET, edge_baselines_key, fees_for, get_logger, net_resale_margin_cents
from task_supervisor import BoundedTaskPool
from zscore import calculate_z_score, should_trigger_anomaly

logger = get_logger("listener.main")

# --- Tunable Detection Parameters (configurable via .env) ---
# Sliding window size for Redis price history
SLIDING_WINDOW_SIZE = int(os.getenv("SLIDING_WINDOW_SIZE", "20"))
# Dedup cache max entries (LRU eviction above this cap)
DEDUP_CACHE_MAX_SIZE = int(os.getenv("DEDUP_CACHE_MAX_SIZE", "25000"))
# A tick at the same price as the item's previous tick within this many seconds is a duplicate.
DEDUP_WINDOW_SECONDS = 300

DedupCache = OrderedDict[str, tuple[int, int]]


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


def is_duplicate(tick: MarketTick, dedup_cache: DedupCache) -> bool:
    """Returns True if this tick is a duplicate (same price within the dedup window)."""
    last_ts, last_price = dedup_cache.get(tick.market_hash_name, (0, 0))
    return tick.price_cents == last_price and (tick.timestamp - last_ts) < DEDUP_WINDOW_SECONDS


def update_dedup_cache(tick: MarketTick, dedup_cache: DedupCache) -> None:
    """Updates the LRU dedup cache with the latest tick, evicting oldest if over capacity."""
    # Move to end if exists (LRU touch), or insert fresh
    if tick.market_hash_name in dedup_cache:
        dedup_cache.move_to_end(tick.market_hash_name)
    dedup_cache[tick.market_hash_name] = (tick.timestamp, tick.price_cents)
    # Evict oldest entries when cache exceeds capacity
    while len(dedup_cache) > DEDUP_CACHE_MAX_SIZE:
        dedup_cache.popitem(last=False)


async def push_to_window(tick: MarketTick, cache: Redis) -> None:
    """Adds the tick's price to the item's sliding window and trims the window to its last N prices."""
    redis_key = f"market:ticks:{tick.market_hash_name}"
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
    redis_key = f"market:ticks:{tick.market_hash_name}"
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
    is_approved = await evaluate_opportunity(tick, cache, baseline)
    rules_engine_latency_seconds.observe(time.monotonic() - _dre_t0)
    if is_approved:
        anomalies_confirmed_total.inc()
        logger.info(
            "[ANOMALY] Confirmed true outlier by Edge DRE (%s)! %s dropped to $%.2f. Executing trade (Z=%.2f)...",
            source,
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
        anomalies_rejected_total.inc()
        logger.info(
            "[ANOMALY] False outlier filtered by Edge DRE (%s): %s at $%.2f.", source, tick.market_hash_name, tick.price_usd
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
    anomalies_detected_total.labels(source=score.source).inc()
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
