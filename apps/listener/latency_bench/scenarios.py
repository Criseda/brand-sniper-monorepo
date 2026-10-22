"""
Benchmark scenarios: seeded schedules of saleFeed events built from the fixture's recorded sales.

Every listing gets its own productId, so no listing is a duplicate and each one is timed from the moment its
event leaves the feed server to the moment the listener has decided on it. A warm-up phase first puts a few
prices of every item into its price window; it is played but not measured.
"""

import copy
import random
from dataclasses import dataclass
from typing import Any

from latency_bench.fixture import LISTED, SOLD, Fixture, versioned_name

# Prices each item gets during the warm-up, above zscore.MIN_HISTORY_POINTS so the window Z score is used.
WARMUP_PRICES_PER_ITEM = 6
WARMUP_SALES_PER_EVENT = 20
WARMUP_EVENT_GAP_MS = 20.0

# A normal listing is priced around the item's latest sale price with this relative spread.
PRICE_SPREAD = 0.03
# Share of listings priced far below the item's support floor, so the DRE approves them and the execution
# path is measured too.
BARGAIN_SHARE = 0.01
BARGAIN_PRICE_FACTOR = 0.5

# Product IDs in the benchmark start here, far from the IDs in the fixture.
FIRST_PRODUCT_ID = 900_000_000


@dataclass(frozen=True, slots=True)
class Scenario:
    name: str
    description: str
    measured_listings: int
    sales_per_event: int | None  # None: drawn from the recorded listed event sizes
    events_per_burst: int
    event_gap_ms: float  # Between events of one burst
    burst_gap_ms: float  # Between the starts of two bursts
    sold_events: bool  # Interleave sold events at the recorded ratio of sold to listed events


SCENARIOS: dict[str, Scenario] = {
    scenario.name: scenario
    for scenario in (
        Scenario(
            name="single",
            description="One listing per event, 100 events a second",
            measured_listings=10_000,
            sales_per_event=1,
            events_per_burst=1,
            event_gap_ms=0.0,
            burst_gap_ms=10.0,
            sold_events=False,
        ),
        Scenario(
            name="burst",
            description="Recorded event sizes and sold events, bursts of 10 events 10 ms apart every 500 ms",
            measured_listings=10_000,
            sales_per_event=None,
            events_per_burst=10,
            event_gap_ms=10.0,
            burst_gap_ms=500.0,
            sold_events=True,
        ),
        Scenario(
            name="large",
            description="100 listings per event (the largest recorded event had 108), one event a second",
            measured_listings=10_000,
            sales_per_event=100,
            events_per_burst=1,
            event_gap_ms=0.0,
            burst_gap_ms=1000.0,
            sold_events=False,
        ),
        Scenario(
            name="saturation",
            description="Recorded event sizes and sold events sent back to back, to find the sustained throughput",
            measured_listings=10_000,
            sales_per_event=None,
            events_per_burst=1,
            event_gap_ms=0.0,
            burst_gap_ms=0.0,
            sold_events=True,
        ),
        # Twice as much: the backlog outgrows Redis's Pub/Sub output buffer (32 MB by default), Redis closes the
        # listener's subscription, and what is published until it subscribes again is lost.
        Scenario(
            name="overload",
            description="As saturation, with twice the listings, to see what happens when the listener falls far behind",
            measured_listings=20_000,
            sales_per_event=None,
            events_per_burst=1,
            event_gap_ms=0.0,
            burst_gap_ms=0.0,
            sold_events=True,
        ),
    )
}


@dataclass(frozen=True, slots=True)
class ScheduledEvent:
    offset_ms: float  # From the start of play
    payload: dict[str, Any]  # The saleFeed event
    measured: bool

    @property
    def event_type(self) -> str:
        return str(self.payload["eventType"])

    @property
    def product_ids(self) -> list[str]:
        return [str(sale["productId"]) for sale in self.payload["sales"]]

    @property
    def key(self) -> str:
        """The event's first productId: unique, and what the feed server keys its emit times by."""
        return str(self.payload["sales"][0]["productId"])


class _SizeSampler:
    """Draws event sizes in proportion to how often the recorded feed sent them."""

    def __init__(self, sizes: dict[int, int], rng: random.Random) -> None:
        if not sizes:
            raise ValueError("the fixture has no recorded event sizes")
        self._sizes = list(sizes)
        self._weights = list(sizes.values())
        self._rng = rng

    def draw(self) -> int:
        return self._rng.choices(self._sizes, weights=self._weights)[0]


class ScenarioBuilder:
    """Builds the event schedule of one scenario. The same fixture, scenario and seed give the same schedule."""

    def __init__(self, fixture: Fixture, seed: int) -> None:
        if not fixture.listed_sales:
            raise ValueError("the fixture has no listed sales")
        self._fixture = fixture
        self._rng = random.Random(seed)
        self._next_product_id = FIRST_PRODUCT_ID

    def _product_id(self) -> int:
        product_id = self._next_product_id
        self._next_product_id += 1
        return product_id

    def _price_cents(self, sale: dict[str, Any], bargain: bool) -> int:
        name = versioned_name(sale)
        baseline = self._fixture.baselines.baselines.get(name or "", {})
        latest = int(baseline.get("latest_price_cents") or sale["salePrice"])
        if bargain:
            support_floor = int(baseline.get("support_floor_cents") or latest)
            return max(1, min(support_floor - 1, round(latest * BARGAIN_PRICE_FACTOR)))
        return max(1, round(latest * self._rng.gauss(1.0, PRICE_SPREAD)))

    def _listing(self, template: dict[str, Any], bargain: bool = False) -> dict[str, Any]:
        sale = copy.deepcopy(template)
        sale["productId"] = self._product_id()
        sale["salePrice"] = self._price_cents(template, bargain)
        return sale

    def _sold(self) -> dict[str, Any]:
        sale = copy.deepcopy(self._rng.choice(self._fixture.sold_sales))
        sale["productId"] = self._product_id()
        return sale

    def warmup(self) -> list[ScheduledEvent]:
        """A few normally priced listings of every item, in events of WARMUP_SALES_PER_EVENT."""
        listings = [self._listing(template) for _ in range(WARMUP_PRICES_PER_ITEM) for template in self._fixture.listed_sales]
        events: list[ScheduledEvent] = []
        for index in range(0, len(listings), WARMUP_SALES_PER_EVENT):
            payload = {"eventType": LISTED, "sales": listings[index : index + WARMUP_SALES_PER_EVENT]}
            events.append(ScheduledEvent(offset_ms=len(events) * WARMUP_EVENT_GAP_MS, payload=payload, measured=False))
        return events

    def measured(self, scenario: Scenario, measured_listings: int | None = None) -> list[ScheduledEvent]:
        """The scenario's measured events, until `measured_listings` listings have been scheduled."""
        target = measured_listings if measured_listings is not None else scenario.measured_listings
        listed_sizes = _SizeSampler(self._fixture.event_sizes.get(LISTED, {}), self._rng)
        sold_sizes = _SizeSampler(self._fixture.event_sizes.get(SOLD, {}), self._rng) if scenario.sold_events else None
        sold_ratio = _sold_ratio(self._fixture.event_sizes) if scenario.sold_events else 0.0

        events: list[ScheduledEvent] = []
        scheduled = 0
        listed_events = 0
        while scheduled < target:
            burst, position = divmod(listed_events, scenario.events_per_burst)
            offset_ms = burst * scenario.burst_gap_ms + position * scenario.event_gap_ms
            size = min(scenario.sales_per_event or listed_sizes.draw(), target - scheduled)
            sales = [
                self._listing(self._rng.choice(self._fixture.listed_sales), bargain=self._rng.random() < BARGAIN_SHARE)
                for _ in range(size)
            ]
            events.append(ScheduledEvent(offset_ms=offset_ms, payload={"eventType": LISTED, "sales": sales}, measured=True))
            scheduled += size
            listed_events += 1
            if sold_sizes is not None and self._fixture.sold_sales and self._rng.random() < sold_ratio:
                sold = [self._sold() for _ in range(sold_sizes.draw())]
                events.append(ScheduledEvent(offset_ms=offset_ms, payload={"eventType": SOLD, "sales": sold}, measured=True))
        return events

    def schedule(self, scenario: Scenario, measured_listings: int | None = None) -> list[ScheduledEvent]:
        """The warm-up, then the measured events. Each phase's offsets count from the start of that phase."""
        return self.warmup() + self.measured(scenario, measured_listings)


def _sold_ratio(event_sizes: dict[str, dict[int, int]]) -> float:
    """Recorded sold events per listed event, capped at one (one sold event at most follows each listed event)."""
    listed = sum(event_sizes.get(LISTED, {}).values())
    sold = sum(event_sizes.get(SOLD, {}).values())
    return min(1.0, sold / listed) if listed else 0.0


def listed_count(events: list[ScheduledEvent], measured: bool | None = None) -> int:
    """Listings in listed events; only measured (True) or warm-up (False) ones when given."""
    return sum(
        len(event.payload["sales"])
        for event in events
        if event.event_type == LISTED and (measured is None or event.measured == measured)
    )
