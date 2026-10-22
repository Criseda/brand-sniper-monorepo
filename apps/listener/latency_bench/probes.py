"""
Stage probes: wrap the listener's own functions from the outside, so the code under test is the code that runs
live, and record time.perf_counter_ns() at each stage boundary per listing (keyed by productId).

Two levels:
    e2e   only what end to end latency needs: when the decision on each listing is complete
    full  every stage boundary as well (parse, queue, window write, window read and Z score, DRE)

Comparing the end to end latency of the two levels bounds what the extra probes cost.
"""

import asyncio
import contextvars
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import detection
from executor import ExecutionService, Purchase
from latency_bench.listener import listener_main
from models import FeedEvent, MarketTick
from scrapers import skinport

PROBE_LEVELS = ("e2e", "full")

# The listing whose window is being scored, so the Z score inside score_window can be attributed to it.
_scoring_listing: contextvars.ContextVar[str | None] = contextvars.ContextVar("scoring_listing", default=None)


@dataclass(slots=True)
class ListingTimes:
    """Stage boundaries of one listing, in perf_counter nanoseconds; None when the stage did not happen."""

    parse_start: int | None = None
    parse_end: int | None = None
    enqueued: int | None = None
    dequeued: int | None = None
    window_start: int | None = None
    window_end: int | None = None
    score_start: int | None = None
    score_end: int | None = None
    zscore_ns: int | None = None  # Duration of calculate_z_score
    detect_end: int | None = None  # update_window_and_detect returned
    triggered: bool = False
    dre_start: int | None = None
    dre_end: int | None = None
    dre_reason: str | None = None
    executed: int | None = None
    decided: int | None = None  # The decision on the listing is complete
    decisions: int = 0  # How many times the listener decided on it (more than one is a duplicate)


@dataclass
class StageRecorder:
    level: str = "full"
    listings: dict[str, ListingTimes] = field(default_factory=dict)
    decided_count: int = 0
    all_decided: asyncio.Event = field(default_factory=asyncio.Event)
    expected: int = 0

    def times(self, listing_id: str | None) -> ListingTimes | None:
        if listing_id is None:
            return None
        times = self.listings.get(listing_id)
        if times is None:
            times = ListingTimes()
            self.listings[listing_id] = times
        return times

    def decide(self, times: ListingTimes, at_ns: int) -> None:
        times.decisions += 1
        if times.decided is None:
            times.decided = at_ns
            self.decided_count += 1
            if self.expected and self.decided_count >= self.expected:
                self.all_decided.set()


class InstrumentedQueue(asyncio.Queue):
    """The listener's tick queue, noting when each listing is put on it and taken off it."""

    def __init__(self, recorder: StageRecorder, maxsize: int = 0) -> None:
        super().__init__(maxsize=maxsize)
        self._recorder = recorder

    async def put(self, item: Any) -> None:
        if isinstance(item, MarketTick):
            times = self._recorder.times(item.listing_id)
            if times is not None:
                times.enqueued = time.perf_counter_ns()
        await super().put(item)

    async def get(self) -> Any:
        item = await super().get()
        if isinstance(item, MarketTick):
            times = self._recorder.times(item.listing_id)
            if times is not None:
                times.dequeued = time.perf_counter_ns()
        return item


class BenchExecutor(ExecutionService):
    """The executor with the backend call left out: notes when the order would be placed."""

    def __init__(self, recorder: StageRecorder) -> None:
        super().__init__()
        self._recorder = recorder
        self.orders = 0

    async def place_order(self, purchase: Purchase) -> None:
        times = self._recorder.times(purchase.listing_id)
        if times is not None:
            times.executed = time.perf_counter_ns()
        self.orders += 1


async def acknowledge_batch(store: Any, batch: Any, **_: Any) -> None:
    """Stands in for batch delivery to the backend: the batch is acknowledged as if the backend took it."""
    await store.acknowledge(batch.record_id)


@contextmanager
def _patched(target: Any, name: str, replacement: Callable[..., Any]) -> Iterator[None]:
    original = getattr(target, name)
    setattr(target, name, replacement)
    try:
        yield
    finally:
        setattr(target, name, original)


@contextmanager
def install(recorder: StageRecorder) -> Iterator[None]:
    """Wrap the listener's stage functions for the duration of the block."""
    main = listener_main()
    original_parse = skinport.parse_sale_feed_event
    original_detect = main.update_window_and_detect
    original_push = detection.push_to_window
    original_score = detection.score_window
    original_zscore = detection.calculate_z_score
    original_trigger = detection.should_trigger_anomaly
    original_dre = detection.dre_approval_reason
    full = recorder.level == "full"

    def parse(payload: object, received_at_ms: int) -> tuple[FeedEvent, list[MarketTick]]:
        started = time.perf_counter_ns()
        feed_event, ticks = original_parse(payload, received_at_ms)
        ended = time.perf_counter_ns()
        for tick in ticks:
            times = recorder.times(tick.listing_id)
            if times is not None:
                times.parse_start = started
                times.parse_end = ended
        return feed_event, ticks

    async def push_to_window(tick: MarketTick, cache: Any) -> None:
        times = recorder.times(tick.listing_id)
        started = time.perf_counter_ns()
        await original_push(tick, cache)
        if times is not None:
            times.window_start = started
            times.window_end = time.perf_counter_ns()

    async def score_window(tick: MarketTick, cache: Any) -> Any:
        times = recorder.times(tick.listing_id)
        token = _scoring_listing.set(tick.listing_id)
        started = time.perf_counter_ns()
        try:
            return await original_score(tick, cache)
        finally:
            if times is not None:
                times.score_start = started
                times.score_end = time.perf_counter_ns()
            _scoring_listing.reset(token)

    def calculate_z_score(*args: Any, **kwargs: Any) -> Any:
        started = time.perf_counter_ns()
        result = original_zscore(*args, **kwargs)
        times = recorder.listings.get(_scoring_listing.get() or "")
        if times is not None:
            times.zscore_ns = time.perf_counter_ns() - started
        return result

    def should_trigger_anomaly(z_score: float, mean_cents: float, tick: MarketTick, source: str = "local") -> bool:
        triggered = original_trigger(z_score, mean_cents, tick, source)
        times = recorder.times(tick.listing_id)
        if times is not None:
            times.triggered = triggered
        return triggered

    async def update_window_and_detect(tick: MarketTick, *args: Any) -> None:
        await original_detect(tick, *args)
        ended = time.perf_counter_ns()
        times = recorder.times(tick.listing_id)
        if times is not None:
            times.detect_end = ended
            # A listing that reached the DRE is decided when the DRE returns.
            if not times.triggered:
                recorder.decide(times, ended)

    async def dre_approval_reason(tick: MarketTick, *args: Any) -> str | None:
        started = time.perf_counter_ns()
        reason = await original_dre(tick, *args)
        ended = time.perf_counter_ns()
        times = recorder.times(tick.listing_id)
        if times is not None:
            times.dre_start = started
            times.dre_end = ended
            times.dre_reason = reason
            recorder.decide(times, ended)
        return reason

    with (
        _patched(main, "update_window_and_detect", update_window_and_detect),
        _patched(main, "deliver_stored_batch", acknowledge_batch),
        _patched(detection, "should_trigger_anomaly", should_trigger_anomaly),
        _patched(detection, "dre_approval_reason", dre_approval_reason),
    ):
        if not full:
            yield
            return
        with (
            _patched(skinport, "parse_sale_feed_event", parse),
            _patched(detection, "push_to_window", push_to_window),
            _patched(detection, "score_window", score_window),
            _patched(detection, "calculate_z_score", calculate_z_score),
        ):
            yield
