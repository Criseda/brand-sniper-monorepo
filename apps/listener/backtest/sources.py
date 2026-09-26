"""
Recorded replay input: raw feed events and REST snapshots, merged into one timestamp-ordered stream,
plus the baseline snapshot the decision path reads. Pure: no database or network access (see database.py).

Fixture format (JSON Lines, one event per line):
    {"kind": "feed", "received_at_ms": <epoch ms>, "payload": <raw saleFeed event>}
    {"kind": "snapshot", "observed_at_ms": <epoch ms>, "market_hash_name": <versioned name>, "price_cents": <int>}

Baseline file (JSON):
    {"baselines": {<versioned name>: <edge baseline document>}, "sticker_prices": {<sticker name>: <cents>},
     "as_of": <optional ISO timestamp of the newest baseline>}
"""

import hashlib
import heapq
import json
from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from models import FeedEvent, MarketTick
from scrapers.skinport import VENUE, parse_sale_feed_message

# Sale fields the listener parser and the outcome labeler read. Fixtures keep only these, which drops
# personal data carried by the live feed (for example the seller's `steamid`) and unused bulk.
SANITIZED_SALE_FIELDS = frozenset(
    {
        "currency",
        "finish",
        "marketHashName",
        "pattern",
        "productId",
        "saleId",
        "salePrice",
        "stickers",
        "url",
        "version",
        "wear",
    }
)

# REST snapshot rows carry only the server's insert time until the edge time is persisted. One poll
# lands as several batches over a few seconds, so rows closer together than this belong to one poll.
POLL_GAP_MS = 60_000

# Snapshots sort before feed events recorded in the same millisecond (the order carries no meaning).
_SNAPSHOT_RANK = 0
_FEED_RANK = 1


@dataclass(frozen=True, slots=True)
class RecordedFeedEvent:
    received_at_ms: int
    payload: dict[str, Any]

    @property
    def time_ms(self) -> int:
        return self.received_at_ms


@dataclass(frozen=True, slots=True)
class RecordedSnapshot:
    observed_at_ms: int
    market_hash_name: str
    price_cents: int

    @property
    def time_ms(self) -> int:
        return self.observed_at_ms


type RecordedEvent = RecordedFeedEvent | RecordedSnapshot
type StreamItem = FeedEvent | MarketTick


@dataclass(frozen=True, slots=True)
class BaselineSnapshot:
    baselines: dict[str, dict[str, Any]] = field(default_factory=dict)
    sticker_prices: dict[str, int] = field(default_factory=dict)
    as_of: str | None = None

    def to_json(self) -> dict[str, Any]:
        document: dict[str, Any] = {"baselines": self.baselines, "sticker_prices": self.sticker_prices}
        if self.as_of is not None:
            document["as_of"] = self.as_of
        return document

    def sha256(self) -> str:
        """Content hash for the decision log header, so runs on different baselines are never confused."""
        canonical = json.dumps(self.to_json(), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def load(cls, path: Path) -> "BaselineSnapshot":
        document = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(document, dict) or not isinstance(document.get("baselines"), dict):
            raise ValueError(f"{path}: baseline file must be an object with a 'baselines' object")
        sticker_prices = document.get("sticker_prices") or {}
        return cls(
            baselines=document["baselines"],
            sticker_prices={name: int(price) for name, price in sticker_prices.items()},
            as_of=document.get("as_of"),
        )

    def write(self, path: Path) -> None:
        text = json.dumps(self.to_json(), sort_keys=True, indent=2, ensure_ascii=False) + "\n"
        path.write_text(text, encoding="utf-8", newline="\n")


def sort_key(event: RecordedEvent, sequence: int) -> tuple[int, int, int]:
    """Total replay order: event time, then snapshots before feed events, then recording order."""
    rank = _SNAPSHOT_RANK if isinstance(event, RecordedSnapshot) else _FEED_RANK
    return (event.time_ms, rank, sequence)


def sanitize_feed_payload(
    payload: dict[str, Any], keep_sale: Callable[[dict[str, Any]], bool] | None = None
) -> dict[str, Any] | None:
    """
    Copy of a saleFeed payload with each sale reduced to SANITIZED_SALE_FIELDS, keeping only the sales
    `keep_sale` accepts (all when None). None when no sale is left.
    """
    sales = payload.get("sales")
    kept_sales = []
    for sale in sales if isinstance(sales, list) else []:
        if not isinstance(sale, dict):
            continue
        if keep_sale is not None and not keep_sale(sale):
            continue
        kept_sales.append({key: value for key, value in sale.items() if key in SANITIZED_SALE_FIELDS})
    if not kept_sales:
        return None
    return {"eventType": payload.get("eventType"), "sales": kept_sales}


def expand_event(event: RecordedEvent) -> list[StreamItem]:
    """
    The stream items the live listener's producers put on its queue for this recorded event.

    Feed events go through the real sidecar-message parser (the raw FeedEvent, then one tick per
    sale). Snapshots become the MarketTick the REST poller yields.
    """
    if isinstance(event, RecordedFeedEvent):
        envelope = json.dumps({"receivedAt": event.received_at_ms, "payload": event.payload})
        feed_event, ticks = parse_sale_feed_message(envelope)
        return [feed_event, *ticks]
    return [
        MarketTick(
            venue=VENUE,
            market_hash_name=event.market_hash_name,
            price_usd=event.price_cents / 100.0,
            timestamp=event.observed_at_ms // 1000,
        )
    ]


class PollClock:
    """
    Stamps REST snapshot rows, read in insert order, with the start of the poll they came from.

    Live, every tick of one poll carries (nearly) the same parse time, but its rows are inserted over
    several batches. Stamping each row with its own insert time would spread one poll over seconds and
    change which ticks the 300-second dedup window drops.
    """

    def __init__(self) -> None:
        self._poll_started_ms: int | None = None
        self._previous_ms: int | None = None

    def stamp(self, inserted_at_ms: int) -> int:
        if self._poll_started_ms is None or self._previous_ms is None or inserted_at_ms - self._previous_ms > POLL_GAP_MS:
            self._poll_started_ms = inserted_at_ms
        self._previous_ms = inserted_at_ms
        return self._poll_started_ms


async def merge_ordered(*streams: AsyncIterator[RecordedEvent]) -> AsyncIterator[RecordedEvent]:
    """Merge streams that are each already in replay order into one stream in replay order."""
    heap: list[tuple[tuple[int, int, int], int, RecordedEvent]] = []
    sequence = 0

    async def push_next(stream_index: int) -> None:
        nonlocal sequence
        event = await anext(streams[stream_index], None)
        if event is not None:
            # The global arrival counter keeps the merge stable and the heap free of event comparisons.
            heapq.heappush(heap, (sort_key(event, sequence), stream_index, event))
            sequence += 1

    for index in range(len(streams)):
        await push_next(index)
    while heap:
        _, stream_index, event = heapq.heappop(heap)
        yield event
        await push_next(stream_index)


def parse_fixture_line(line: str, line_number: int) -> RecordedEvent:
    record = json.loads(line)
    kind = record.get("kind") if isinstance(record, dict) else None
    if kind == "feed" and isinstance(record.get("payload"), dict) and isinstance(record.get("received_at_ms"), int):
        return RecordedFeedEvent(received_at_ms=record["received_at_ms"], payload=record["payload"])
    if (
        kind == "snapshot"
        and isinstance(record.get("observed_at_ms"), int)
        and isinstance(record.get("market_hash_name"), str)
        and isinstance(record.get("price_cents"), int)
    ):
        return RecordedSnapshot(
            observed_at_ms=record["observed_at_ms"],
            market_hash_name=record["market_hash_name"],
            price_cents=record["price_cents"],
        )
    raise ValueError(f"line {line_number}: not a valid feed or snapshot record")


def load_fixture(path: Path) -> list[RecordedEvent]:
    """Read a fixture file and return its events in replay order (the file order breaks ties)."""
    events: list[tuple[tuple[int, int, int], RecordedEvent]] = []
    with path.open(encoding="utf-8") as fixture:
        for line_number, line in enumerate(fixture, start=1):
            if not line.strip():
                continue
            try:
                event = parse_fixture_line(line, line_number)
            except (ValueError, json.JSONDecodeError) as err:
                raise ValueError(f"{path}: {err}") from err
            events.append((sort_key(event, line_number), event))
    events.sort(key=lambda keyed: keyed[0])
    return [event for _, event in events]


def fixture_record(event: RecordedEvent) -> dict[str, Any]:
    if isinstance(event, RecordedFeedEvent):
        return {"kind": "feed", "received_at_ms": event.received_at_ms, "payload": event.payload}
    return {
        "kind": "snapshot",
        "observed_at_ms": event.observed_at_ms,
        "market_hash_name": event.market_hash_name,
        "price_cents": event.price_cents,
    }


def write_fixture(path: Path, events: Iterable[RecordedEvent]) -> int:
    """Write events as a fixture file (one sorted-key JSON object per line); returns the event count."""
    count = 0
    with path.open("w", encoding="utf-8", newline="\n") as fixture:
        for event in events:
            fixture.write(json.dumps(fixture_record(event), sort_keys=True, separators=(",", ":"), ensure_ascii=False))
            fixture.write("\n")
            count += 1
    return count
