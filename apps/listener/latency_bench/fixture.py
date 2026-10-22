"""
The benchmark fixture: recorded Skinport sales with identifying values replaced, the baselines of their items,
and how many sales the recorded feed events carried.

The replay fixtures (backtest.sources) keep only the fields the listener reads, which is about a tenth of a live
sale. Parsing cost grows with the payload, so this fixture keeps every field and replaces only the values that
identify a person or an asset with synthetic values of the same length and type.

Fixture directory:
    sales.jsonl    one {"event_type": "listed" | "sold", "sale": <sale>} per line
    baselines.json the edge baselines and sticker prices of those items (backtest.sources.BaselineSnapshot)
    fixture.json   where the sales came from, and the recorded event sizes per event type
"""

import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backtest.sources import BaselineSnapshot, RecordedEvent, RecordedFeedEvent
from shared_utils import build_versioned_name

FIXTURE_VERSION = 1
LISTED = "listed"
SOLD = "sold"

# Values that point at a person or at one asset in someone's inventory. They are replaced, keeping their
# length and type, so the payload still has the size and shape of a live one.
IDENTIFYING_SALE_FIELDS = frozenset(
    {"assetId", "assetid", "classid", "customName", "link", "saleId", "screenshots", "shortId", "steamid"}
)


def deidentify(value: Any) -> Any:
    """Same length and type: digits become 0 and letters x; a whole number keeps its digit count."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return 10 ** (len(str(abs(value))) - 1)
    if isinstance(value, str):
        return "".join("0" if char.isdigit() else "x" if char.isalpha() else char for char in value)
    if isinstance(value, list):
        return [deidentify(item) for item in value]
    if isinstance(value, dict):
        return {key: deidentify(item) for key, item in value.items()}
    return value


def deidentify_sale(sale: dict[str, Any]) -> dict[str, Any]:
    return {key: deidentify(value) if key in IDENTIFYING_SALE_FIELDS else value for key, value in sale.items()}


def versioned_name(sale: dict[str, Any]) -> str | None:
    name = sale.get("marketHashName")
    return build_versioned_name(name, sale.get("version")) if isinstance(name, str) and name else None


@dataclass(frozen=True, slots=True)
class Fixture:
    listed_sales: list[dict[str, Any]]  # One template per item, each with a baseline
    sold_sales: list[dict[str, Any]]
    baselines: BaselineSnapshot
    # Recorded event sizes: {"listed": {sales per event: events}, "sold": {...}}
    event_sizes: dict[str, dict[int, int]]

    @classmethod
    def load(cls, directory: Path) -> "Fixture":
        listed: list[dict[str, Any]] = []
        sold: list[dict[str, Any]] = []
        with (directory / "sales.jsonl").open(encoding="utf-8") as lines:
            for line in lines:
                if not line.strip():
                    continue
                record = json.loads(line)
                (listed if record["event_type"] == LISTED else sold).append(record["sale"])
        metadata = json.loads((directory / "fixture.json").read_text(encoding="utf-8"))
        if metadata.get("version") != FIXTURE_VERSION:
            raise ValueError(f"{directory}: fixture version {metadata.get('version')}, expected {FIXTURE_VERSION}")
        event_sizes = {
            event_type: {int(size): count for size, count in sizes.items()}
            for event_type, sizes in metadata["event_sizes"].items()
        }
        return cls(
            listed_sales=listed,
            sold_sales=sold,
            baselines=BaselineSnapshot.load(directory / "baselines.json"),
            event_sizes=event_sizes,
        )


def recorded_event_sizes(events: Iterable[RecordedEvent]) -> dict[str, dict[int, int]]:
    """How many sales each recorded listed and sold event carried."""
    sizes: dict[str, Counter[int]] = {LISTED: Counter(), SOLD: Counter()}
    for event in events:
        if not isinstance(event, RecordedFeedEvent):
            continue
        event_type = event.payload.get("eventType")
        sales = event.payload.get("sales")
        if event_type in sizes and isinstance(sales, list) and sales:
            sizes[event_type][len(sales)] += 1
    return {event_type: dict(sorted(counter.items())) for event_type, counter in sizes.items()}


def select_templates(
    events: Iterable[RecordedEvent], baselines: BaselineSnapshot, max_items: int, max_sold: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The first listed sale of each of the first `max_items` items with a baseline, and the first `max_sold` sold sales."""
    listed: dict[str, dict[str, Any]] = {}
    sold: list[dict[str, Any]] = []
    for event in events:
        if not isinstance(event, RecordedFeedEvent):
            continue
        event_type = event.payload.get("eventType")
        for sale in event.payload.get("sales") or []:
            if not isinstance(sale, dict):
                continue
            if event_type == LISTED and len(listed) < max_items:
                name = versioned_name(sale)
                if name is not None and name in baselines.baselines and name not in listed:
                    listed[name] = deidentify_sale(sale)
            elif event_type == SOLD and len(sold) < max_sold:
                sold.append(deidentify_sale(sale))
    return list(listed.values()), sold


def fixture_baselines(listed_sales: list[dict[str, Any]], baselines: BaselineSnapshot) -> BaselineSnapshot:
    """The baselines of the template items, and the prices of the stickers applied to them."""
    names = sorted({name for sale in listed_sales if (name := versioned_name(sale)) is not None})
    sticker_names = {
        sticker["name"]
        for sale in listed_sales
        for sticker in sale.get("stickers") or []
        if isinstance(sticker, dict) and sticker.get("name")
    }
    return BaselineSnapshot(
        baselines={name: baselines.baselines[name] for name in names},
        sticker_prices={
            name: baselines.sticker_prices[name] for name in sorted(sticker_names) if name in baselines.sticker_prices
        },
        as_of=baselines.as_of,
    )


def write_fixture(
    directory: Path,
    listed_sales: list[dict[str, Any]],
    sold_sales: list[dict[str, Any]],
    baselines: BaselineSnapshot,
    event_sizes: dict[str, dict[int, int]],
    source: dict[str, Any],
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "sales.jsonl").open("w", encoding="utf-8", newline="\n") as lines:
        for event_type, sales in ((LISTED, listed_sales), (SOLD, sold_sales)):
            for sale in sales:
                record = {"event_type": event_type, "sale": sale}
                lines.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
    baselines.write(directory / "baselines.json")
    metadata = {
        "version": FIXTURE_VERSION,
        "source": source,
        "listed_templates": len(listed_sales),
        "sold_templates": len(sold_sales),
        "event_sizes": {
            event_type: {str(size): count for size, count in sizes.items()} for event_type, sizes in event_sizes.items()
        },
    }
    (directory / "fixture.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n"
    )
