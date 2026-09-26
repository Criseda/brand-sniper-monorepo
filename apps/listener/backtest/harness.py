"""
The replay loop. Routes stream items exactly as `main.tick_consumer` does (outcome ticks recorded only,
duplicates dropped, unchanged REST snapshots windowed but not scored, everything else pushed into the price
window) and asks a strategy for a decision.

The decision log is JSON Lines: a `run` header, one `decision` row per logged tick, and a `summary`.
With a baseline schedule, a `baseline` row marks each point where a new build took effect. The log holds
no wall-clock times or durations, so identical input produces a byte-identical log; timings go to the
optional `on_timing` hook instead.
"""

import asyncio
import json
import time
from collections import OrderedDict
from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Any

import detection
from backtest.sources import BaselineSchedule, BaselineSnapshot, RecordedEvent, expand_event
from backtest.store import InMemoryEdgeStore
from backtest.strategy import REASON_DUPLICATE, REASON_UNCHANGED_SNAPSHOT, Decision, DecisionContext, Strategy
from models import FeedEvent, MarketTick

LOG_FORMAT_VERSION = 2
# Floats in the log are rounded so the bytes do not depend on the last bits of float arithmetic.
FLOAT_DIGITS = 6

type TimingHook = Callable[[MarketTick, Decision, int], None]
type Sleep = Callable[[float], Awaitable[None]]


@dataclass(slots=True)
class ReplaySummary:
    events: int = 0
    feed_events: int = 0
    ticks: int = 0
    outcome_ticks: int = 0
    duplicates: int = 0
    unchanged_snapshots: int = 0
    decisions: int = 0
    logged: int = 0
    approved: int = 0

    def to_json(self) -> dict[str, int]:
        return {
            "events": self.events,
            "feed_events": self.feed_events,
            "ticks": self.ticks,
            "outcome_ticks": self.outcome_ticks,
            "duplicates": self.duplicates,
            "unchanged_snapshots": self.unchanged_snapshots,
            "decisions": self.decisions,
            "logged": self.logged,
            "approved": self.approved,
        }


def _round_floats(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, FLOAT_DIGITS)
    if isinstance(value, dict):
        return {key: _round_floats(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_round_floats(item) for item in value]
    return value


def _log_line(record: dict[str, Any]) -> str:
    return json.dumps(_round_floats(record), sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def should_log(tick: MarketTick, decision: Decision) -> bool:
    """Every listing-level tick is logged (they join to listing_outcomes); REST snapshots only when approved."""
    return tick.listing_id is not None or decision.approve


def decision_record(sequence: int, tick: MarketTick, decision: Decision) -> dict[str, Any]:
    return {
        "type": "decision",
        "seq": sequence,
        "timestamp": tick.timestamp,
        "venue": tick.venue,
        "listing_id": tick.listing_id,
        "event_type": tick.event_type,
        "market_hash_name": tick.market_hash_name,
        "price_cents": tick.price_cents,
        "approve": decision.approve,
        "reason": decision.reason,
        "score": decision.score,
        "features": dict(decision.features),
    }


async def _as_async(events: Iterable[RecordedEvent] | AsyncIterable[RecordedEvent]) -> AsyncIterable[RecordedEvent]:
    if isinstance(events, AsyncIterable):
        async for event in events:
            yield event
    else:
        for event in events:
            yield event


class _BaselineSwitcher:
    """Loads each scheduled build into the store once replay time reaches it, and logs the switch."""

    def __init__(self, schedule: BaselineSchedule, store: InMemoryEdgeStore, log: IO[str]) -> None:
        self._schedule = schedule
        self._store = store
        self._log = log
        self._active: int | None = None

    async def advance(self, time_ms: int) -> None:
        index = self._schedule.active_index(time_ms)
        if index == self._active:
            return
        self._active = index
        build = self._schedule.builds[index]
        snapshot = await self._schedule.load(build.build_id)
        self._store.load_baselines(snapshot.baselines, snapshot.sticker_prices)
        self._log.write(
            _log_line(
                {
                    "type": "baseline",
                    "build_id": build.build_id,
                    "built_at": build.built_at,
                    "from_ms": time_ms,
                    "look_ahead": build.effective_from_ms > time_ms,
                    "sha256": snapshot.sha256(),
                }
            )
        )


async def run_replay(
    events: Iterable[RecordedEvent] | AsyncIterable[RecordedEvent],
    strategy: Strategy,
    baselines: BaselineSnapshot | BaselineSchedule,
    log: IO[str],
    *,
    source_label: str,
    log_from_ms: int | None = None,
    speed: float | None = None,
    sleep: Sleep = asyncio.sleep,
    on_timing: TimingHook | None = None,
) -> ReplaySummary:
    """
    Replay `events` (already in replay order) through `strategy` and write the decision log to `log`.

    `baselines` is one fixed snapshot, or a schedule of dated builds that take effect as replay time
    reaches them. Events before `log_from_ms` only warm up the price windows and dedup cache; they are
    not logged. `speed` replays at recorded pace (1.0 = real time, 10.0 = ten times faster); None runs
    flat out.
    """
    store = InMemoryEdgeStore()
    switcher: _BaselineSwitcher | None = None
    header_baselines: dict[str, Any]
    if isinstance(baselines, BaselineSchedule):
        if not baselines.builds:
            raise ValueError("The baseline schedule has no builds")
        switcher = _BaselineSwitcher(baselines, store, log)
        header_baselines = {
            "baseline_mode": "schedule",
            "baseline_builds": [build.build_id for build in baselines.builds],
        }
    else:
        store.load_baselines(baselines.baselines, baselines.sticker_prices)
        header_baselines = {
            "baseline_mode": "fixed",
            "baseline_sha256": baselines.sha256(),
            "baseline_as_of": baselines.as_of,
        }
    context = DecisionContext(store=store)
    dedup_cache: detection.DedupCache = OrderedDict()
    summary = ReplaySummary()
    previous_time_ms: int | None = None

    log.write(
        _log_line(
            {
                "type": "run",
                "format": LOG_FORMAT_VERSION,
                "strategy": strategy.name,
                "config": strategy.config(),
                "source": source_label,
                **header_baselines,
                "log_from_ms": log_from_ms,
            }
        )
    )

    async for event in _as_async(events):
        if speed is not None and previous_time_ms is not None and event.time_ms > previous_time_ms:
            await sleep((event.time_ms - previous_time_ms) / 1000 / speed)
        previous_time_ms = event.time_ms
        if switcher is not None:
            await switcher.advance(event.time_ms)
        summary.events += 1
        is_logged_period = log_from_ms is None or event.time_ms >= log_from_ms

        for item in expand_event(event):
            if isinstance(item, FeedEvent):
                summary.feed_events += 1
                continue
            summary.ticks += 1
            if not item.feeds_price_window:
                # Sold events are outcomes: never deduplicated, windowed, or scored.
                summary.outcome_ticks += 1
                continue

            if detection.is_duplicate(item, dedup_cache):
                summary.duplicates += 1
                decision = Decision(approve=False, reason=REASON_DUPLICATE)
            elif detection.is_unchanged_snapshot(item, dedup_cache):
                # Live keeps the window as it was but does not score the same lowest ask again.
                detection.update_dedup_cache(item, dedup_cache)
                await detection.push_to_window(item, context.cache)
                summary.unchanged_snapshots += 1
                decision = Decision(approve=False, reason=REASON_UNCHANGED_SNAPSHOT)
            else:
                detection.update_dedup_cache(item, dedup_cache)
                started_ns = time.perf_counter_ns()
                await detection.push_to_window(item, context.cache)
                decision = await strategy.decide(item, context)
                if on_timing is not None:
                    on_timing(item, decision, time.perf_counter_ns() - started_ns)
                summary.decisions += 1

            if is_logged_period and should_log(item, decision):
                summary.logged += 1
                summary.approved += int(decision.approve)
                log.write(_log_line(decision_record(summary.logged, item, decision)))

    log.write(_log_line({"type": "summary", **summary.to_json()}))
    return summary


def open_log(path: Path) -> IO[str]:
    """Decision logs are written with LF line endings on every platform, so their bytes compare equal."""
    return path.open("w", encoding="utf-8", newline="\n")
