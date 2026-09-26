"""
Baseline scorecard (#249): how the current Z-score and DRE rules would have done, measured against what
the market actually did.

The input is a decision log from the replay harness (apps/listener, `python -m backtest run`), made with the
`zscore_dre_sweep` strategy: the live decisions plus the inputs the threshold sweep needs. Approved feed
listings join to their `listing_outcomes` labels. Approved REST snapshots have no listing ID, so they are
labeled here with the same v1 rule (`label_outcomes.label_listing`), from the sold sales after the trade
hold. Nothing uses a label before its `label_available_at`.

Every number that rests on fewer than `MIN_SAMPLE` labeled trades is reported as insufficient data.
Proportions carry Wilson intervals; mean P&L per trade carries a seeded bootstrap interval, so a rerun
with the same inputs and `--as-of` writes the same report.

Usage (from apps/analytics), or both steps at once with `make scorecard START=... END=...`:
    uv run python baseline_scorecard.py --decisions ../../data/scorecard/decisions.jsonl

See docs/backtesting.md.
"""

import argparse
import asyncio
import hashlib
import json
import math
import os
import random
import statistics
import subprocess
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from shared_utils import setup_script_environment

REPO_ROOT = setup_script_environment(__file__)

from label_outcomes import LABEL_VERSION, SOURCE, FeedSale, LabelConfig, SoldIndex, fetch_feed_sales, label_listing
from mlflow.client import MlflowClient
from shared_utils import get_logger, parse_item_meta, parse_version_from_name, utc_fromtimestamp_naive, utc_now_naive
from shared_utils.db_connection import async_engine
from sqlalchemy import text

logger = get_logger("analytics.scorecard")

SCORECARD_VERSION = 1
SWEEP_STRATEGY = "zscore_dre_sweep"
SUPPORTED_STRATEGIES = ("zscore_dre", SWEEP_STRATEGY)

# REST snapshots were trade locked lowest asks until #275. The first tradable poll ran at 15:46:56 UTC on
# 2026-09-26, and the price windows (20 prices, one per poll) still held trade locked asks for 20 polls
# after it. Decisions before this time compared against the wrong prices, so the scorecard leaves them out.
TRADABLE_REST_FROM = datetime(2026, 9, 26, 17, 30)

# Below this many labeled trades (or profitable listings, for recall) a cell reports insufficient data.
MIN_SAMPLE = 30
CONFIDENCE_Z = 1.96  # 95% intervals
BOOTSTRAP_RESAMPLES = 2000
BOOTSTRAP_SEED = 249
# Above this many trades the bootstrap gets slow and the normal interval is as good, so it is used instead.
BOOTSTRAP_MAX_TRADES = 500

TICK_KIND_LISTING = "listing"
TICK_KIND_REST_SNAPSHOT = "rest_snapshot"

OUTCOME_LABELED = "labeled"  # Profitable or not
OUTCOME_NEUTRAL = "neutral"  # Too few comparable sales to say
OUTCOME_PENDING = "pending"  # The label horizon has not passed yet
OUTCOME_UNLABELED = "unlabeled"  # The horizon has passed but no label row exists: run label_outcomes.py

REASON_BELOW_THRESHOLD = "below_threshold"

# Buy price tiers and liquidity buckets: (lower bound, label), in ascending order.
PRICE_TIERS = ((0, "under $5"), (500, "$5 to $50"), (5_000, "$50 to $500"), (50_000, "$500 and over"))
LIQUIDITY_BUCKETS = (
    (0.0, "under 1 sale a day"),
    (1.0, "1 to 10 sales a day"),
    (10.0, "10 to 50 sales a day"),
    (50.0, "50 or more sales a day"),
)
NO_BASELINE = "no baseline"

# Threshold sweep grid. The sticker Z threshold is kept at its live value.
SWEEP_Z_THRESHOLDS = (-1.5, -2.0, -2.5, -3.0)
SWEEP_MIN_SAVINGS_CENTS = (25, 50, 100, 200)

_ID_CHUNK_SIZE = 5_000


def parse_utc(value: str) -> datetime:
    """ISO timestamp as naive UTC; a value without an offset is taken as UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _epoch_ms(naive_utc: datetime) -> int:
    return (naive_utc - datetime(1970, 1, 1)) // timedelta(milliseconds=1)


# --- Decision log ---


@dataclass(frozen=True)
class LoggedDecision:
    """One decision row, with the baseline build that was in effect when it was made."""

    row: dict[str, Any]
    build_id: int | None
    look_ahead: bool

    @property
    def decided_at(self) -> datetime:
        return utc_fromtimestamp_naive(self.row["timestamp"])

    @property
    def tick_kind(self) -> str:
        return TICK_KIND_LISTING if self.row["listing_id"] is not None else TICK_KIND_REST_SNAPSHOT


@dataclass(frozen=True)
class DecisionLog:
    header: dict[str, Any]
    decisions: list[LoggedDecision]
    summary: dict[str, Any] | None
    sha256: str
    venue: str
    start: datetime
    end: datetime
    # Replay time ranges (epoch ms, end exclusive) whose baselines came from a later build.
    look_ahead_ranges: list[tuple[int, int]] = field(default_factory=list)

    @property
    def strategy(self) -> str:
        return str(self.header["strategy"])

    @property
    def config(self) -> dict[str, Any]:
        return dict(self.header.get("config") or {})


def parse_database_source(source: str) -> tuple[str, datetime, datetime]:
    """(venue, start, end) from a database replay's source label, `database:<venue>:<start>/<end>`."""
    kind, _, rest = source.partition(":")
    venue, _, time_range = rest.partition(":")
    start_text, _, end_text = time_range.partition("/")
    if kind != "database" or not venue or not start_text or not end_text:
        raise ValueError(f"The scorecard needs a database replay; this log's source is '{source}'")
    return venue, parse_utc(start_text), parse_utc(end_text)


def read_decision_log(path: Path) -> DecisionLog:
    """Reads a decision log, noting for each decision the baseline build it was made with."""
    raw = path.read_bytes()
    header: dict[str, Any] | None = None
    summary: dict[str, Any] | None = None
    decisions: list[LoggedDecision] = []
    switches: list[tuple[int, bool]] = []
    build_id: int | None = None
    look_ahead = False
    for line in raw.decode("utf-8").splitlines():
        if not line.strip():
            continue
        record = json.loads(line)
        record_type = record.get("type")
        if record_type == "run":
            header = record
        elif record_type == "baseline":
            # The harness writes a switch before the decisions it applies to, so log order is enough.
            build_id = record["build_id"]
            look_ahead = bool(record["look_ahead"])
            switches.append((record["from_ms"], look_ahead))
        elif record_type == "decision":
            decisions.append(LoggedDecision(row=record, build_id=build_id, look_ahead=look_ahead))
        elif record_type == "summary":
            summary = record
    if header is None:
        raise ValueError(f"{path} has no run header; is it a decision log?")
    if header.get("strategy") not in SUPPORTED_STRATEGIES:
        raise ValueError(f"The scorecard scores the live rules; this log was made with '{header.get('strategy')}'")

    venue, start, end = parse_database_source(str(header.get("source", "")))
    end_ms = _epoch_ms(end)
    look_ahead_ranges = [
        (from_ms, switches[index + 1][0] if index + 1 < len(switches) else end_ms)
        for index, (from_ms, is_look_ahead) in enumerate(switches)
        if is_look_ahead
    ]
    return DecisionLog(
        header=header,
        decisions=decisions,
        summary=summary,
        sha256=hashlib.sha256(raw).hexdigest(),
        venue=venue,
        start=start,
        end=end,
        look_ahead_ranges=look_ahead_ranges,
    )


def scorable_start(log: DecisionLog) -> datetime:
    """The replay range starts at the log start, but never before tradable REST prices filled the windows."""
    return max(log.start, TRADABLE_REST_FROM)


def is_scorable(decision: LoggedDecision, start: datetime) -> bool:
    return not decision.look_ahead and decision.decided_at >= start


def in_look_ahead(moment: datetime, look_ahead_ranges: Sequence[tuple[int, int]]) -> bool:
    moment_ms = _epoch_ms(moment)
    return any(range_start <= moment_ms < range_end for range_start, range_end in look_ahead_ranges)


# --- Trades and labels ---


@dataclass
class Trade:
    """One approval the live rules would have paper traded, and what the market did afterwards."""

    decided_at: datetime
    tick_kind: str
    listing_id: str | None
    market_hash_name: str
    price_cents: int
    rule: str  # The DRE rule that approved it
    z_source: str | None
    build_id: int | None
    outcome: str = OUTCOME_PENDING
    net_margin_cents: int | None = None
    is_profitable: bool | None = None
    avg_volume_30d: float | None = None

    @classmethod
    def from_decision(cls, decision: LoggedDecision) -> "Trade":
        row = decision.row
        return cls(
            decided_at=decision.decided_at,
            tick_kind=decision.tick_kind,
            listing_id=row["listing_id"],
            market_hash_name=row["market_hash_name"],
            price_cents=int(row["price_cents"]),
            rule=row["reason"],
            z_source=(row.get("features") or {}).get("z_source"),
            build_id=decision.build_id,
        )


def collect_trades(decisions: Iterable[LoggedDecision], start: datetime) -> list[Trade]:
    """
    Approved decisions as trades, in decision order.

    A feed listing is one trade however many times it was approved: it can only be bought once. Each
    approved REST snapshot is a trade, as live paper trades it; since #265 an unchanged lowest ask is not
    scored again, so a repeat means the price changed.
    """
    trades: list[Trade] = []
    seen_listings: set[str] = set()
    for decision in decisions:
        if not decision.row["approve"] or not is_scorable(decision, start):
            continue
        listing_id = decision.row["listing_id"]
        if listing_id is not None:
            if listing_id in seen_listings:
                continue
            seen_listings.add(listing_id)
        trades.append(Trade.from_decision(decision))
    return trades


def matures_by(decided_at: datetime, config: LabelConfig, as_of: datetime) -> bool:
    """True once the labeler would have labeled a listing made at `decided_at`."""
    return decided_at + timedelta(seconds=config.horizon_seconds + config.settle_seconds) <= as_of


def apply_listing_label(trade: Trade, label: dict[str, Any] | None, config: LabelConfig, as_of: datetime) -> None:
    """Sets a listing trade's outcome from its `listing_outcomes` row (None when there is none)."""
    if label is None or label["label_available_at"] > as_of:
        trade.outcome = OUTCOME_UNLABELED if matures_by(trade.decided_at, config, as_of) else OUTCOME_PENDING
        return
    set_outcome(trade, label["is_profitable"], label["resale_net_margin_cents"])


def set_outcome(trade: Trade, is_profitable: bool | None, net_margin_cents: int | None) -> None:
    if is_profitable is None:
        trade.outcome = OUTCOME_NEUTRAL
        return
    trade.outcome = OUTCOME_LABELED
    trade.is_profitable = is_profitable
    trade.net_margin_cents = net_margin_cents


def label_snapshot_trades(trades: Sequence[Trade], sold: SoldIndex, config: LabelConfig, as_of: datetime) -> None:
    """
    Labels approved REST snapshots with the v1 rule, as if each lowest ask were a listing seen at decision time.

    The snapshot's own sale cannot be excluded from the comparable sales (a snapshot has no listing ID),
    which only matters when the lowest ask itself sold within the resale window.
    """
    for index, trade in enumerate(trades):
        if not matures_by(trade.decided_at, config, as_of):
            trade.outcome = OUTCOME_PENDING
            continue
        listing = FeedSale(
            listing_id=f"rest-snapshot-{index}",
            market_hash_name=trade.market_hash_name,
            feed_name=parse_version_from_name(trade.market_hash_name)[0],
            price_cents=trade.price_cents,
            seen_at=trade.decided_at,
        )
        label = label_listing(listing, sold, config)
        set_outcome(trade, label["is_profitable"], label["resale_net_margin_cents"])


def _chunks(values: Sequence[Any]) -> Iterable[Sequence[Any]]:
    """Keeps each `= ANY(:values)` parameter list to a size PostgreSQL handles comfortably."""
    size = _ID_CHUNK_SIZE
    for start in range(0, len(values), size):
        yield values[start : start + size]


_LABELS_QUERY = """
SELECT listing_id, is_profitable, resale_net_margin_cents, label_available_at
FROM listing_outcomes
WHERE source = :source AND label_version = :label_version AND listing_id = ANY(:listing_ids)
"""

_PROFITABLE_LISTINGS_QUERY = """
SELECT listing_id, listed_at
FROM listing_outcomes
WHERE source = :source AND label_version = :label_version AND is_profitable IS TRUE
  AND listed_at >= :start AND listed_at < :end AND label_available_at <= :as_of
"""

_VOLUME_QUERY = """
SELECT build_id, market_hash_name, avg_volume_30d
FROM venue_baselines
WHERE build_id = ANY(:build_ids) AND market_hash_name = ANY(:names)
"""


async def fetch_listing_labels(listing_ids: Sequence[str], label_version: str) -> dict[str, dict[str, Any]]:
    labels: dict[str, dict[str, Any]] = {}
    async with async_engine.connect() as conn:
        for chunk in _chunks(sorted(set(listing_ids))):
            params = {"source": SOURCE, "label_version": label_version, "listing_ids": list(chunk)}
            result = await conn.execute(text(_LABELS_QUERY), params)
            for listing_id, is_profitable, margin, available_at in result.fetchall():
                labels[listing_id] = {
                    "is_profitable": is_profitable,
                    "resale_net_margin_cents": margin,
                    "label_available_at": available_at,
                }
    return labels


async def fetch_profitable_listings(start: datetime, end: datetime, label_version: str, as_of: datetime) -> dict[str, datetime]:
    """Every listing first seen in [start, end) whose label says it was profitable: the recall denominator."""
    params = {"source": SOURCE, "label_version": label_version, "start": start, "end": end, "as_of": as_of}
    async with async_engine.connect() as conn:
        result = await conn.execute(text(_PROFITABLE_LISTINGS_QUERY), params)
        return {listing_id: listed_at for listing_id, listed_at in result.fetchall()}


async def fetch_volumes(trades: Sequence[Trade]) -> dict[tuple[int, str], float]:
    """30 day sales per day of each traded item, from the baseline build each trade was decided with."""
    build_ids = sorted({trade.build_id for trade in trades if trade.build_id is not None})
    names = sorted({trade.market_hash_name for trade in trades})
    if not build_ids or not names:
        return {}
    volumes: dict[tuple[int, str], float] = {}
    async with async_engine.connect() as conn:
        for chunk in _chunks(names):
            result = await conn.execute(text(_VOLUME_QUERY), {"build_ids": build_ids, "names": list(chunk)})
            for build_id, name, volume in result.fetchall():
                volumes[(build_id, name)] = float(volume)
    return volumes


# --- Statistics ---


def wilson_interval(successes: int, trials: int, z: float = CONFIDENCE_Z) -> tuple[float, float]:
    """Wilson score interval of a proportion; well behaved for small samples and rates near 0 or 1."""
    if trials <= 0:
        raise ValueError("trials must be positive")
    rate = successes / trials
    denominator = 1 + z * z / trials
    centre = (rate + z * z / (2 * trials)) / denominator
    half_width = z * math.sqrt(rate * (1 - rate) / trials + z * z / (4 * trials * trials)) / denominator
    return max(0.0, centre - half_width), min(1.0, centre + half_width)


def mean_interval(values: Sequence[int]) -> tuple[float, float, str]:
    """95% interval of the mean: a seeded percentile bootstrap for small samples, the normal interval above."""
    count = len(values)
    mean = sum(values) / count
    if count > BOOTSTRAP_MAX_TRADES:
        half_width = CONFIDENCE_Z * statistics.stdev(values) / math.sqrt(count)
        return mean - half_width, mean + half_width, "normal"
    rng = random.Random(BOOTSTRAP_SEED)
    means = sorted(sum(rng.choices(values, k=count)) / count for _ in range(BOOTSTRAP_RESAMPLES))
    low_index = int(BOOTSTRAP_RESAMPLES * 0.025)
    high_index = int(BOOTSTRAP_RESAMPLES * 0.975) - 1
    return means[low_index], means[high_index], "bootstrap"


def max_drawdown_cents(margins: Iterable[int]) -> int:
    """Largest fall of cumulative net P&L from its running peak (which starts at 0), in trade order."""
    cumulative = 0
    peak = 0
    drawdown = 0
    for margin in margins:
        cumulative += margin
        peak = max(peak, cumulative)
        drawdown = max(drawdown, peak - cumulative)
    return drawdown


def score_trades(trades: Sequence[Trade]) -> dict[str, Any]:
    """Counts, precision, and net P&L of a group of trades; metrics only at `MIN_SAMPLE` labeled trades."""
    ordered = sorted(trades, key=lambda trade: trade.decided_at)
    labeled = [trade for trade in ordered if trade.outcome == OUTCOME_LABELED]
    cell: dict[str, Any] = {
        "trades": len(ordered),
        "labeled": len(labeled),
        "neutral": sum(1 for trade in ordered if trade.outcome == OUTCOME_NEUTRAL),
        "pending": sum(1 for trade in ordered if trade.outcome == OUTCOME_PENDING),
        "unlabeled": sum(1 for trade in ordered if trade.outcome == OUTCOME_UNLABELED),
        "status": "ok" if len(labeled) >= MIN_SAMPLE else "insufficient_data",
        "precision": None,
        "net_pnl_cents": None,
        "max_drawdown_cents": None,
    }
    if cell["status"] != "ok":
        return cell
    profitable = sum(1 for trade in labeled if trade.is_profitable)
    low, high = wilson_interval(profitable, len(labeled))
    cell["precision"] = {"value": profitable / len(labeled), "low": low, "high": high}
    margins = [int(trade.net_margin_cents or 0) for trade in labeled]
    mean_low, mean_high, method = mean_interval(margins)
    cell["net_pnl_cents"] = {
        "total": sum(margins),
        "mean": sum(margins) / len(margins),
        "mean_low": mean_low,
        "mean_high": mean_high,
        "interval": method,
    }
    cell["max_drawdown_cents"] = max_drawdown_cents(margins)
    return cell


def score_recall(trades: Sequence[Trade], profitable_listings: Iterable[str]) -> dict[str, Any]:
    """Share of the listings that turned out profitable which the rules approved (feed listings only)."""
    universe = set(profitable_listings)
    found = sum(1 for trade in trades if trade.listing_id in universe and trade.is_profitable)
    recall: dict[str, Any] = {
        "profitable_listings": len(universe),
        "approved": found,
        "status": "ok" if len(universe) >= MIN_SAMPLE else "insufficient_data",
        "value": None,
        "low": None,
        "high": None,
    }
    if recall["status"] == "ok":
        recall["value"] = found / len(universe)
        recall["low"], recall["high"] = wilson_interval(found, len(universe))
    return recall


# --- Breakdowns ---


def bucket(value: float, buckets: Sequence[tuple[float, str]]) -> str:
    label = buckets[0][1]
    for lower_bound, bucket_label in buckets:
        if value >= lower_bound:
            label = bucket_label
    return label


def price_tier(trade: Trade) -> str:
    return bucket(trade.price_cents, PRICE_TIERS)


def item_type(trade: Trade) -> str:
    return parse_item_meta(trade.market_hash_name)[1]


def liquidity_bucket(trade: Trade) -> str:
    return NO_BASELINE if trade.avg_volume_30d is None else bucket(trade.avg_volume_30d, LIQUIDITY_BUCKETS)


BREAKDOWNS: dict[str, Callable[[Trade], str]] = {
    "tick_kind": lambda trade: trade.tick_kind,
    "dre_rule": lambda trade: trade.rule,
    "z_source": lambda trade: trade.z_source or "none",
    "price_tier": price_tier,
    "item_type": item_type,
    "liquidity": liquidity_bucket,
}


def breakdown(trades: Sequence[Trade], key: Callable[[Trade], str]) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[Trade]] = {}
    for trade in trades:
        groups.setdefault(key(trade), []).append(trade)
    return {name: score_trades(groups[name]) for name in sorted(groups)}


def time_slices(start: datetime, end: datetime, days: int) -> list[tuple[datetime, datetime]]:
    slices = []
    cursor = start
    while cursor < end:
        slice_end = min(cursor + timedelta(days=days), end)
        slices.append((cursor, slice_end))
        cursor = slice_end
    return slices


# --- Threshold sweep ---


def would_trigger(
    z_score: float,
    mean_cents: float,
    price_cents: int,
    sticker_count: int,
    *,
    z_threshold: float,
    sticker_z_threshold: float,
    min_savings_cents: int,
) -> bool:
    """`zscore.should_trigger_anomaly` with the thresholds as arguments (a parity test keeps them equal)."""
    threshold = sticker_z_threshold if sticker_count > 0 else z_threshold
    if z_score >= threshold:
        return False
    if sticker_count == 0 and mean_cents - price_cents < min_savings_cents:
        return False
    return True


def dre_would_approve(row: dict[str, Any]) -> bool:
    """The DRE verdict on a scored tick. It does not depend on the Z thresholds, so it holds for any of them."""
    if row["approve"]:
        return True
    if row["reason"] == REASON_BELOW_THRESHOLD:
        return (row.get("features") or {}).get("dre_reason") is not None
    return False


def sweep_thresholds(
    decisions: Sequence[LoggedDecision], log: DecisionLog, start: datetime, listing_trade: Callable[[LoggedDecision], Trade]
) -> dict[str, Any]:
    """
    Precision and volume of the listing trades under other Z thresholds and savings floors, from one log.

    In sample: the grid is scored on the same data it is read from, so the best cell flatters itself.
    Only feed listings are swept; the log holds a REST snapshot only when the live rules approved it.
    `listing_trade` turns a decision into a trade carrying its listing's outcome.
    """
    if log.strategy != SWEEP_STRATEGY:
        return {"available": False, "reason": f"the decision log was not made with the {SWEEP_STRATEGY} strategy"}
    config = log.config
    live_z = float(config["z_score_threshold"])
    live_savings = int(config["min_savings_cents"])
    sticker_z = float(config["z_score_sticker_threshold"])
    scored = [
        decision
        for decision in decisions
        if decision.tick_kind == TICK_KIND_LISTING and decision.row["score"] is not None and is_scorable(decision, start)
    ]
    logged_approvals = {decision.row["listing_id"] for decision in scored if decision.row["approve"]}
    rows = []
    live_matches_log = False
    for z_threshold in SWEEP_Z_THRESHOLDS:
        for min_savings in SWEEP_MIN_SAVINGS_CENTS:
            approved: dict[str, Trade] = {}
            for decision in scored:
                row = decision.row
                features = row.get("features") or {}
                triggered = would_trigger(
                    row["score"],
                    features["mean_cents"],
                    row["price_cents"],
                    features["sticker_count"],
                    z_threshold=z_threshold,
                    sticker_z_threshold=sticker_z,
                    min_savings_cents=min_savings,
                )
                if triggered and dre_would_approve(row) and row["listing_id"] not in approved:
                    approved[row["listing_id"]] = listing_trade(decision)
            is_live = z_threshold == live_z and min_savings == live_savings
            if is_live:
                # The recomputed live cell must approve exactly what the replay approved.
                live_matches_log = set(approved) == logged_approvals
            rows.append(
                {
                    "z_threshold": z_threshold,
                    "min_savings_cents": min_savings,
                    "live": is_live,
                    **score_trades(list(approved.values())),
                }
            )
    return {
        "available": True,
        "in_sample": True,
        "tick_kind": TICK_KIND_LISTING,
        "sticker_z_threshold": sticker_z,
        "live": {"z_threshold": live_z, "min_savings_cents": live_savings},
        "live_in_grid": any(row["live"] for row in rows),
        "live_matches_log": live_matches_log,
        "rows": rows,
    }


# --- The scorecard ---


@dataclass(frozen=True)
class ScorecardInputs:
    log: DecisionLog
    label_version: str
    as_of: datetime
    slice_days: int = 7


async def build_scorecard(inputs: ScorecardInputs) -> dict[str, Any]:
    log = inputs.log
    if log.venue != SOURCE:
        raise ValueError(f"Labels exist for {SOURCE} only; this log replays {log.venue}")
    config = LabelConfig()
    start = scorable_start(log)
    end = log.end
    if end <= start:
        raise ValueError(f"The replay ends before {TRADABLE_REST_FROM.isoformat()}, when tradable REST prices began (#275)")

    trades = collect_trades(log.decisions, start)
    listing_trades = [trade for trade in trades if trade.tick_kind == TICK_KIND_LISTING]
    snapshot_trades = [trade for trade in trades if trade.tick_kind == TICK_KIND_REST_SNAPSHOT]

    # Labels for every scored listing, so the sweep can score approvals the live rules did not make.
    sweep_listing_ids = [
        decision.row["listing_id"]
        for decision in log.decisions
        if decision.tick_kind == TICK_KIND_LISTING and decision.row["score"] is not None and is_scorable(decision, start)
    ]
    labels = await fetch_listing_labels(
        sweep_listing_ids + [str(trade.listing_id) for trade in listing_trades], inputs.label_version
    )
    for trade in listing_trades:
        apply_listing_label(trade, labels.get(str(trade.listing_id)), config, inputs.as_of)

    notes: list[str] = []
    if inputs.label_version == config.version:
        mature = [trade for trade in snapshot_trades if matures_by(trade.decided_at, config, inputs.as_of)]
        if mature:
            first = min(trade.decided_at for trade in mature)
            last = max(trade.decided_at for trade in mature)
            names = sorted({parse_version_from_name(trade.market_hash_name)[0] for trade in mature})
            sold = await fetch_feed_sales("sold", first, last + timedelta(seconds=config.horizon_seconds), names=names)
            label_snapshot_trades(snapshot_trades, SoldIndex.build(sold), config, inputs.as_of)
        else:
            label_snapshot_trades(snapshot_trades, SoldIndex(), config, inputs.as_of)
    elif snapshot_trades:
        for trade in snapshot_trades:
            trade.outcome = OUTCOME_UNLABELED
        notes.append(
            f"REST snapshots can only be labeled with the current rule ({config.version}), so they are left unlabeled."
        )

    volumes = await fetch_volumes(trades)
    for trade in trades:
        if trade.build_id is not None:
            trade.avg_volume_30d = volumes.get((trade.build_id, trade.market_hash_name))

    profitable = await fetch_profitable_listings(start, end, inputs.label_version, inputs.as_of)
    profitable = {
        listing_id: listed_at
        for listing_id, listed_at in profitable.items()
        if not in_look_ahead(listed_at, log.look_ahead_ranges)
    }

    unlabeled = sum(1 for trade in listing_trades if trade.outcome == OUTCOME_UNLABELED)
    if unlabeled:
        notes.append(
            f"{unlabeled} approved listings are past their label horizon but have no {inputs.label_version} label. "
            "Run label_outcomes.py over the range, then run the scorecard again."
        )

    overall = score_trades(trades)
    overall["recall"] = score_recall(listing_trades, profitable)
    slices = []
    for slice_start, slice_end in time_slices(start, end, inputs.slice_days):
        in_slice = [trade for trade in trades if slice_start <= trade.decided_at < slice_end]
        cell = score_trades(in_slice)
        cell["recall"] = score_recall(
            [trade for trade in in_slice if trade.tick_kind == TICK_KIND_LISTING],
            [listing_id for listing_id, listed_at in profitable.items() if slice_start <= listed_at < slice_end],
        )
        slices.append({"start": slice_start.isoformat(), "end": slice_end.isoformat(), **cell})

    def listing_trade(decision: LoggedDecision) -> Trade:
        trade = Trade.from_decision(decision)
        apply_listing_label(trade, labels.get(str(trade.listing_id)), config, inputs.as_of)
        return trade

    sweep = sweep_thresholds(log.decisions, log, start, listing_trade)

    excluded_before = sum(1 for decision in log.decisions if decision.row["approve"] and decision.decided_at < start)
    excluded_look_ahead = sum(
        1 for decision in log.decisions if decision.row["approve"] and decision.look_ahead and decision.decided_at >= start
    )
    return {
        "scorecard_version": SCORECARD_VERSION,
        "as_of": inputs.as_of.isoformat(),
        "git_commit": git_commit(),
        "data": {
            "venue": log.venue,
            "replay_start": log.start.isoformat(),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "tradable_rest_from": TRADABLE_REST_FROM.isoformat(),
            "decision_log_sha256": log.sha256,
            "strategy": log.strategy,
            "strategy_config": log.config,
            "baseline_builds": log.header.get("baseline_builds"),
            "look_ahead_ranges_ms": [list(item) for item in log.look_ahead_ranges],
            "replay_summary": log.summary,
        },
        "labels": {
            "version": inputs.label_version,
            "horizon_seconds": config.horizon_seconds,
            "min_comparable_sales": config.min_comparable_sales,
            "settle_seconds": config.settle_seconds,
        },
        "fees": {
            "venue": config.fees.venue,
            "fee_tiers": [{"min_price_cents": tier.min_price_cents, "fee_bps": tier.fee_bps} for tier in config.fees.fee_tiers],
            "hold_seconds": config.fees.hold_seconds,
            "min_margin_cents": config.fees.min_margin_cents,
        },
        "rules": {
            "min_sample": MIN_SAMPLE,
            "confidence": 0.95,
            "bootstrap_resamples": BOOTSTRAP_RESAMPLES,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "bootstrap_max_trades": BOOTSTRAP_MAX_TRADES,
        },
        "coverage": {
            "approved_decisions": sum(1 for decision in log.decisions if decision.row["approve"]),
            "excluded_trade_locked_rest": excluded_before,
            "excluded_look_ahead": excluded_look_ahead,
            "trades": len(trades),
            "listing_trades": len(listing_trades),
            "rest_snapshot_trades": len(snapshot_trades),
            "scored_listings": len(set(sweep_listing_ids)),
        },
        "notes": notes,
        "overall": overall,
        "slices": slices,
        "breakdowns": {name: breakdown(trades, key) for name, key in BREAKDOWNS.items()},
        "sweep": sweep,
    }


def git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=True, timeout=10
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return result.stdout.strip() or None


# --- Rendering ---

INSUFFICIENT = "insufficient data"
BREAKDOWN_TITLES = {
    "tick_kind": "Tick kind",
    "dre_rule": "DRE rule",
    "z_source": "Z-score source",
    "price_tier": "Buy price",
    "item_type": "Item type",
    "liquidity": "Liquidity (30 day sales per day, from the baseline in effect)",
}


def dollars(cents: float) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) / 100:,.2f}"


def percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _precision_text(cell: dict[str, Any]) -> str:
    precision = cell["precision"]
    if precision is None:
        return INSUFFICIENT
    return f"{percent(precision['value'])} ({percent(precision['low'])} to {percent(precision['high'])})"


def _mean_text(cell: dict[str, Any]) -> str:
    pnl = cell["net_pnl_cents"]
    if pnl is None:
        return INSUFFICIENT
    return f"{dollars(pnl['mean'])} ({dollars(pnl['mean_low'])} to {dollars(pnl['mean_high'])})"


def _total_text(cell: dict[str, Any]) -> str:
    return INSUFFICIENT if cell["net_pnl_cents"] is None else dollars(cell["net_pnl_cents"]["total"])


def _drawdown_text(cell: dict[str, Any]) -> str:
    return INSUFFICIENT if cell["max_drawdown_cents"] is None else dollars(cell["max_drawdown_cents"])


def _recall_text(recall: dict[str, Any]) -> str:
    if recall["value"] is None:
        return f"{INSUFFICIENT} ({recall['approved']} of {recall['profitable_listings']})"
    return f"{percent(recall['value'])} ({percent(recall['low'])} to {percent(recall['high'])})"


def _counts(cell: dict[str, Any]) -> list[str]:
    return [str(cell[key]) for key in ("trades", "labeled", "neutral", "pending", "unlabeled")]


def _table(headers: Sequence[str], rows: Iterable[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return lines


CELL_HEADERS = ("Trades", "Labeled", "Neutral", "Pending", "Unlabeled", "Precision (95% CI)", "Net P&L", "Per trade (95% CI)")


def _cell_row(cell: dict[str, Any]) -> list[str]:
    return _counts(cell) + [_precision_text(cell), _total_text(cell), _mean_text(cell)]


def render_markdown(card: dict[str, Any]) -> str:
    data = card["data"]
    labels = card["labels"]
    fees = card["fees"]
    rules = card["rules"]
    overall = card["overall"]
    fee_text = ", ".join(f"{tier['fee_bps'] / 100:g}% from {dollars(tier['min_price_cents'])}" for tier in fees["fee_tiers"])
    lines = [
        "# Baseline scorecard",
        "",
        "How the current Z-score and DRE rules would have done, scored against what the market did afterwards (#249).",
        "Generated by `apps/analytics/baseline_scorecard.py`; see [backtesting.md](../backtesting.md#baseline-scorecard).",
        "",
        "## Run",
        "",
        f"- Data: {data['venue']}, decisions from {data['start']} to {data['end']} UTC "
        f"(replay from {data['replay_start']}; nothing before {data['tradable_rest_from']}, "
        "when tradable REST prices had filled the price windows).",
        f"- Strategy: `{data['strategy']}`, Z threshold {data['strategy_config'].get('z_score_threshold')}, "
        f"sticker Z threshold {data['strategy_config'].get('z_score_sticker_threshold')}, "
        f"savings floor {dollars(data['strategy_config'].get('min_savings_cents', 0))}.",
        f"- Labels: `{labels['version']}`, horizon {labels['horizon_seconds'] // 86_400} days, "
        f"at least {labels['min_comparable_sales']} comparable sales, labels read as of {card['as_of']} UTC.",
        f"- Fees: {fees['venue']} seller fee {fee_text}, trade hold {fees['hold_seconds'] // 86_400} days, "
        f"minimum margin {dollars(fees['min_margin_cents'])}.",
        f"- Decision log sha256 `{data['decision_log_sha256']}`, commit `{card['git_commit']}`.",
        f"- Minimum sample: {rules['min_sample']} labeled trades (or profitable listings, for recall). "
        "Below it a cell says insufficient data. Precision and recall carry Wilson 95% intervals; the mean P&L "
        f"per trade carries a bootstrap interval ({rules['bootstrap_resamples']} resamples, seed {rules['bootstrap_seed']}) "
        f"up to {rules['bootstrap_max_trades']} trades and a normal interval above.",
        "",
        "Net P&L is the fee-aware margin from reselling at the median comparable sale after the trade hold. "
        "Neutral trades had too few comparable sales to say, pending ones have not reached their label horizon, "
        "and unlabeled ones are past it with no label yet.",
        "",
    ]
    for note in card["notes"]:
        lines += [f"> {note}", ""]

    coverage = card["coverage"]
    lines += [
        "## Overall",
        "",
        *_table(
            CELL_HEADERS + ("Max drawdown", "Recall (95% CI)"),
            [_cell_row(overall) + [_drawdown_text(overall), _recall_text(overall["recall"])]],
        ),
        "",
        f"Trades: {coverage['trades']} ({coverage['listing_trades']} from feed listings, "
        f"{coverage['rest_snapshot_trades']} from REST snapshots). Left out: {coverage['excluded_trade_locked_rest']} "
        f"approvals before tradable REST prices and {coverage['excluded_look_ahead']} made with a later baseline build. "
        "Recall counts feed listings only.",
        "",
        "## Time slices",
        "",
        *_table(
            ("Slice (UTC)",) + CELL_HEADERS + ("Max drawdown", "Recall (95% CI)"),
            [
                [f"{item['start']} to {item['end']}"] + _cell_row(item) + [_drawdown_text(item), _recall_text(item["recall"])]
                for item in card["slices"]
            ],
        ),
        "",
    ]
    for name, groups in card["breakdowns"].items():
        lines += [f"## {BREAKDOWN_TITLES[name]}", ""]
        lines += _table(
            (BREAKDOWN_TITLES[name].split(" (")[0],) + CELL_HEADERS,
            [[group] + _cell_row(cell) for group, cell in groups.items()],
        )
        lines.append("")

    sweep = card["sweep"]
    lines += ["## Threshold sweep (in sample)", ""]
    if not sweep["available"]:
        lines += [f"Not available: {sweep['reason']}.", ""]
    else:
        lines += [
            "Feed listings only, scored on the same data the grid is read from, so the best cell flatters itself. "
            f"The sticker Z threshold stays at {sweep['sticker_z_threshold']}. Changing a threshold is a follow-up issue, "
            "not part of this scorecard.",
            "",
        ]
        if sweep["live_in_grid"] and not sweep["live_matches_log"]:
            lines += ["> The sweep's live cell does not match the logged decisions; do not trust this table.", ""]
        lines += _table(
            ("Z threshold", "Savings floor", "Live") + CELL_HEADERS,
            [
                [str(row["z_threshold"]), dollars(row["min_savings_cents"]), "yes" if row["live"] else ""] + _cell_row(row)
                for row in sweep["rows"]
            ],
        )
        lines.append("")
    return "\n".join(lines)


def write_reports(card: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "baseline_scorecard.json"
    markdown_path = out_dir / "baseline_scorecard.md"
    json_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")
    markdown_path.write_text(render_markdown(card), encoding="utf-8", newline="\n")
    return json_path, markdown_path


# --- MLflow ---

MLFLOW_EXPERIMENT = "baseline-scorecard"


def mlflow_metrics(card: dict[str, Any]) -> dict[str, float]:
    """Headline numbers for MLflow; cells below the minimum sample are left out, not logged as zero."""
    overall = card["overall"]
    metrics: dict[str, float] = {
        "trades": overall["trades"],
        "labeled_trades": overall["labeled"],
        "neutral_trades": overall["neutral"],
        "pending_trades": overall["pending"],
    }
    if overall["precision"] is not None:
        metrics["precision"] = overall["precision"]["value"]
        metrics["precision_low"] = overall["precision"]["low"]
        metrics["net_pnl_total_cents"] = overall["net_pnl_cents"]["total"]
        metrics["net_pnl_mean_cents"] = overall["net_pnl_cents"]["mean"]
        metrics["max_drawdown_cents"] = overall["max_drawdown_cents"]
    if overall["recall"]["value"] is not None:
        metrics["recall"] = overall["recall"]["value"]
    return metrics


def mlflow_params(card: dict[str, Any]) -> dict[str, str]:
    data = card["data"]
    params = {
        "strategy": data["strategy"],
        "venue": data["venue"],
        "start": data["start"],
        "end": data["end"],
        "label_version": card["labels"]["version"],
        "as_of": card["as_of"],
        "decision_log_sha256": data["decision_log_sha256"],
        "git_commit": str(card["git_commit"]),
        "min_sample": str(card["rules"]["min_sample"]),
        "fee_tiers": json.dumps(card["fees"]["fee_tiers"], sort_keys=True),
        "hold_seconds": str(card["fees"]["hold_seconds"]),
    }
    params.update({f"config.{key}": str(value) for key, value in sorted(data["strategy_config"].items())})
    return params


def log_to_mlflow(card: dict[str, Any], artifacts: Sequence[Path]) -> str:
    """Logs the scorecard as one MLflow run, so model runs (#234) compare against it. Returns the run ID."""

    client = MlflowClient(tracking_uri=os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    experiment = client.get_experiment_by_name(MLFLOW_EXPERIMENT)
    experiment_id = experiment.experiment_id if experiment else client.create_experiment(MLFLOW_EXPERIMENT)
    run_name = f"{card['data']['strategy']} {card['data']['start'][:10]} to {card['data']['end'][:10]}"
    run = client.create_run(experiment_id, run_name=run_name, tags={"scorecard_version": str(SCORECARD_VERSION)})
    run_id = run.info.run_id
    try:
        for key, value in mlflow_params(card).items():
            client.log_param(run_id, key, value)
        for key, metric in mlflow_metrics(card).items():
            client.log_metric(run_id, key, float(metric))
        for artifact in artifacts:
            client.log_artifact(run_id, str(artifact))
    except Exception:
        client.set_terminated(run_id, status="FAILED")
        raise
    client.set_terminated(run_id, status="FINISHED")
    return run_id


# --- CLI ---


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Score the live rules' replayed decisions against outcome labels.")
    parser.add_argument("--decisions", type=Path, required=True, help="Decision log from `python -m backtest run`")
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "docs" / "benchmarks", help="Where to write the report")
    parser.add_argument(
        "--label-version", default=LABEL_VERSION, help=f"Label version to score against (default {LABEL_VERSION})"
    )
    parser.add_argument("--slice-days", type=int, default=7, help="Length of each time slice in days (default 7)")
    parser.add_argument("--as-of", type=parse_utc, help="Read labels as of this time (ISO, UTC; default now)")
    parser.add_argument("--no-mlflow", action="store_true", help="Write the report without logging an MLflow run")
    return parser


async def run(args: argparse.Namespace) -> int:
    if args.slice_days < 1:
        raise SystemExit("--slice-days must be at least 1")
    log = read_decision_log(args.decisions)
    as_of = args.as_of or utc_now_naive()
    card = await build_scorecard(
        ScorecardInputs(log=log, label_version=args.label_version, as_of=as_of, slice_days=args.slice_days)
    )
    json_path, markdown_path = write_reports(card, args.out_dir)
    overall = card["overall"]
    logger.info(
        "[SCORECARD] %d trades, %d labeled, precision %s -> %s",
        overall["trades"],
        overall["labeled"],
        _precision_text(overall),
        markdown_path,
    )
    for note in card["notes"]:
        logger.warning("[SCORECARD] %s", note)
    if args.no_mlflow:
        return 0
    try:
        run_id = log_to_mlflow(card, [json_path, markdown_path])
    except Exception as error:
        logger.error("[SCORECARD] MLflow logging failed (%s); the report is written. Use --no-mlflow to skip it.", error)
        return 1
    logger.info("[SCORECARD] Logged MLflow run %s in experiment %s", run_id, MLFLOW_EXPERIMENT)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(build_parser().parse_args(argv)))


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    from shared_utils import validate_required_env

    validate_required_env(["DATABASE_URL"])
    sys.exit(main())
