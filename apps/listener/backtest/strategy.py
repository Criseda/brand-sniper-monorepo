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
from shared_utils import (
    FEATURE_VERSION,
    LISTING_RATE_WINDOW_SECONDS,
    PRICE_WINDOW_RULE,
    UNCHANGED_SNAPSHOT_WINDOW_SECONDS,
    ListingRateTracker,
)
from student_features import listing_features

# Reasons shared by all strategies.
# A feed listing seen again at the same price, or a REST snapshot at the same price as the item's previous tick
# inside the dedup window; never scored.
REASON_DUPLICATE = "duplicate"
# A REST snapshot at the same price as the item's previous snapshot: never scored (#265), and windowed only
# while the window is short or once an hour (#269).
REASON_UNCHANGED_SNAPSHOT = "unchanged_snapshot"
# How the harness and the listener treat repeated prices; written to the log header so runs are comparable.
# Before #312 a feed listing was also a duplicate at the item's previous price (`time_window_and_unchanged_snapshot`).
DEDUP_RULE = "listing_id_and_time_window_and_unchanged_snapshot"

# Reasons specific to the Z-score/DRE strategy. Approvals carry the DRE rule (rules_engine.REASON_*).
REASON_INSUFFICIENT_HISTORY = "insufficient_history"
REASON_BELOW_THRESHOLD = "below_threshold"
REASON_DRE_REJECTED = "dre_rejected"
# Scored with the macro Z score only (the window is too short, as right after a restart), which never
# triggers (#308, zscore.source_can_trigger).
REASON_MACRO_ONLY = "macro_only"


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
    # Also record what a threshold sweep needs (see ZScoreDreSweepStrategy); never changes a decision.
    records_sweep_inputs = False

    def config(self) -> dict[str, Any]:
        return {
            "sliding_window_size": detection.SLIDING_WINDOW_SIZE,
            "state_key": detection.STATE_KEY,
            "dedup_rule": DEDUP_RULE,
            "dedup_window_seconds": detection.DEDUP_WINDOW_SECONDS,
            "window_rule": PRICE_WINDOW_RULE,
            "unchanged_snapshot_window_seconds": UNCHANGED_SNAPSHOT_WINDOW_SECONDS,
            "dedup_cache_max_size_per_venue": detection.DEDUP_CACHE_MAX_SIZE,
            "z_score_threshold": zscore.Z_SCORE_THRESHOLD,
            "z_score_sticker_threshold": zscore.Z_SCORE_STICKER_THRESHOLD,
            "min_savings_cents": zscore.MIN_SAVINGS_CENTS,
            "min_std_dev_factor": zscore.MIN_STD_DEV_FACTOR,
            "min_history_points": zscore.MIN_HISTORY_POINTS,
            "macro_zscore_fallback": zscore.MACRO_ZSCORE_FALLBACK,
            "macro_prior_weight": zscore.MACRO_PRIOR_WEIGHT,
            "macro_zscore_triggers": zscore.MACRO_ZSCORE_TRIGGERS,
        }

    async def decide(self, tick: MarketTick, context: DecisionContext) -> Decision:
        window_score = await detection.score_window(tick, context.cache)
        return await self.decide_scored(tick, context, window_score)

    async def decide_scored(
        self, tick: MarketTick, context: DecisionContext, window_score: detection.WindowScore | None
    ) -> Decision:
        """The decision on a tick whose window has been scored (`window_score` None: too little history)."""
        if window_score is None:
            return Decision(approve=False, reason=REASON_INSUFFICIENT_HISTORY)

        features: dict[str, Any] = {
            "mean_cents": window_score.mean_cents,
            "window_size": window_score.window_size,
            "z_source": window_score.source,
        }
        if self.records_sweep_inputs:
            features["sticker_count"] = len(tick.stickers)
        if not zscore.source_can_trigger(window_score.source):
            return Decision(approve=False, reason=REASON_MACRO_ONLY, score=window_score.z_score, features=features)
        if not zscore.should_trigger_anomaly(window_score.z_score, window_score.mean_cents, tick, window_score.source):
            if self.records_sweep_inputs:
                # The DRE does not read the Z-score thresholds, so its verdict here is the one a looser
                # threshold would have got.
                features["dre_reason"] = await dre_approval_reason(tick, context.cache, window_score.baseline)
            return Decision(approve=False, reason=REASON_BELOW_THRESHOLD, score=window_score.z_score, features=features)

        approval_reason = await dre_approval_reason(tick, context.cache, window_score.baseline)
        if approval_reason is None:
            return Decision(approve=False, reason=REASON_DRE_REJECTED, score=window_score.z_score, features=features)

        # Same estimate the live executor records with the paper trade.
        features["estimated_net_profit_cents"] = detection.estimate_net_profit_cents(tick, window_score.baseline or {})
        return Decision(approve=True, reason=approval_reason, score=window_score.z_score, features=features)


class ZScoreDreSweepStrategy(ZScoreDreStrategy):
    """
    The live rules with the same decisions, plus what the scorecard's threshold sweep needs (#249).

    Every scored tick also records its sticker count, and a tick below the Z threshold records the DRE
    verdict it would have got (`dre_reason`, None when the DRE rejects it). The Z threshold and the
    savings floor only decide which ticks reach the DRE, so any stricter or looser combination can then be
    evaluated from one log instead of one replay per combination.
    """

    name = "zscore_dre_sweep"
    records_sweep_inputs = True


class ZScoreDreFeaturesStrategy(ZScoreDreSweepStrategy):
    """
    The sweep strategy's decisions and inputs, plus the student model's features of every feed listing (#234).

    Each scored listing carries `features["student"]` (shared_utils.features, computed by
    student_features.listing_features), so the training flow builds its rows from the same replay that the
    scorecard reads. REST snapshots name no listing and have no label, so they get no student features.
    """

    name = "zscore_dre_features"

    def __init__(self) -> None:
        self._rates = ListingRateTracker()

    def config(self) -> dict[str, Any]:
        return {
            **super().config(),
            "feature_version": FEATURE_VERSION,
            "listing_rate_window_seconds": LISTING_RATE_WINDOW_SECONDS,
        }

    async def decide(self, tick: MarketTick, context: DecisionContext) -> Decision:
        window_score = await detection.score_window(tick, context.cache)
        decision = await self.decide_scored(tick, context, window_score)
        if tick.listing_id is None:
            return decision
        student = await listing_features(tick, window_score, context.cache, self._rates)
        return Decision(
            approve=decision.approve,
            reason=decision.reason,
            score=decision.score,
            features={**decision.features, "student": student},
        )


STRATEGIES: dict[str, Callable[[], Strategy]] = {
    ZScoreDreStrategy.name: ZScoreDreStrategy,
    ZScoreDreSweepStrategy.name: ZScoreDreSweepStrategy,
    ZScoreDreFeaturesStrategy.name: ZScoreDreFeaturesStrategy,
}
