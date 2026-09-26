"""
Pluggable decision strategies. A strategy sees one tick after it has entered the price window and
returns a Decision; the harness owns routing, deduplication, and the window itself, so every strategy
(and every strategy in a shadow comparison) decides on identical inputs.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import detection
import zscore
from backtest.store import InMemoryEdgeStore
from models import MarketTick
from redis.asyncio import Redis
from rules_engine import dre_approval_reason

# Reasons shared by all strategies.
REASON_DUPLICATE = "duplicate"  # Repeats the item's price (see detection.is_duplicate); never scored.

# Reasons specific to the Z-score/DRE strategy. Approvals carry the DRE rule (rules_engine.REASON_*).
REASON_INSUFFICIENT_HISTORY = "insufficient_history"
REASON_BELOW_THRESHOLD = "below_threshold"
REASON_DRE_REJECTED = "dre_rejected"


@dataclass(frozen=True, slots=True)
class Decision:
    approve: bool
    reason: str
    score: float | None = None
    features: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DecisionContext:
    """Edge state at decision time: the price windows, baselines, and sticker prices."""

    store: InMemoryEdgeStore

    @property
    def cache(self) -> Redis:
        """The store typed as the Redis client the live decision functions expect (it duck-types it)."""
        return cast(Redis, self.store)


class Strategy(Protocol):
    name: str

    def config(self) -> dict[str, Any]:
        """Every parameter that changes this strategy's decisions; written to the decision log header."""
        ...

    async def decide(self, tick: MarketTick, context: DecisionContext) -> Decision: ...


class ZScoreDreStrategy:
    """The live rules: Z-score outlier detection on the price window, confirmed by the Edge DRE."""

    name = "zscore_dre"

    def config(self) -> dict[str, Any]:
        return {
            "sliding_window_size": detection.SLIDING_WINDOW_SIZE,
            "dedup_window_seconds": detection.DEDUP_WINDOW_SECONDS,
            "dedup_cache_max_size": detection.DEDUP_CACHE_MAX_SIZE,
            "z_score_threshold": zscore.Z_SCORE_THRESHOLD,
            "z_score_sticker_threshold": zscore.Z_SCORE_STICKER_THRESHOLD,
            "min_savings_cents": zscore.MIN_SAVINGS_CENTS,
            "min_std_dev_factor": zscore.MIN_STD_DEV_FACTOR,
            "min_history_points": zscore.MIN_HISTORY_POINTS,
            "macro_zscore_fallback": zscore.MACRO_ZSCORE_FALLBACK,
            "macro_prior_weight": zscore.MACRO_PRIOR_WEIGHT,
        }

    async def decide(self, tick: MarketTick, context: DecisionContext) -> Decision:
        window_score = await detection.score_window(tick, context.cache)
        if window_score is None:
            return Decision(approve=False, reason=REASON_INSUFFICIENT_HISTORY)

        features: dict[str, Any] = {
            "mean_cents": window_score.mean_cents,
            "window_size": window_score.window_size,
            "z_source": window_score.source,
        }
        if not zscore.should_trigger_anomaly(window_score.z_score, window_score.mean_cents, tick, window_score.source):
            return Decision(approve=False, reason=REASON_BELOW_THRESHOLD, score=window_score.z_score, features=features)

        approval_reason = await dre_approval_reason(tick, context.cache, window_score.baseline)
        if approval_reason is None:
            return Decision(approve=False, reason=REASON_DRE_REJECTED, score=window_score.z_score, features=features)

        # Same estimate the live executor records with the paper trade.
        features["estimated_net_profit_cents"] = detection.estimate_net_profit_cents(tick, window_score.baseline or {})
        return Decision(approve=True, reason=approval_reason, score=window_score.z_score, features=features)


STRATEGIES: dict[str, Callable[[], Strategy]] = {ZScoreDreStrategy.name: ZScoreDreStrategy}
