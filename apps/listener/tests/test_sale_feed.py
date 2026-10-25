import json
from pathlib import Path

import pytest
from models import MarketTick, TickKind
from pydantic import ValidationError
from scrapers.skinport import parse_sale_feed_event

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _parse(payload: object, received_at_ms: int = 1_790_000_000_000):
    return parse_sale_feed_event(payload, received_at_ms)


def test_sold_event_from_captured_feed_parses_every_sale():
    """Real (sanitized) sold event: saleId is null, productId joins the listing to its outcome."""
    envelope = _load_fixture("skinport_sold_event.json")

    feed_event, ticks = parse_sale_feed_event(envelope["payload"], envelope["receivedAt"])

    assert feed_event.event_type == "sold"
    assert feed_event.received_at_ms == envelope["receivedAt"]
    assert feed_event.payload == envelope["payload"]
    assert len(ticks) == len(envelope["payload"]["sales"]) == 3

    rifle, case, knife = ticks
    assert all(tick.event_type == "sold" for tick in ticks)
    assert all(not tick.feeds_price_window for tick in ticks)
    assert all(tick.timestamp == envelope["receivedAt"] // 1000 for tick in ticks)

    assert rifle.market_hash_name == "AK-47 | Slate (Field-Tested)"
    assert rifle.price_cents == 397
    assert rifle.listing_id == "58903454"
    assert rifle.float_value == pytest.approx(0.3607417345046997)
    assert rifle.pattern == 415
    assert rifle.paint_index == 1035
    assert len(rifle.stickers) == 2
    assert rifle.listing_url == "https://skinport.com/item/ak-47-slate-field-tested"

    # Containers carry no wear, pattern, or finish.
    assert case.float_value is None
    assert case.pattern is None
    assert case.paint_index is None
    assert case.stickers == []

    # Doppler phases are folded into the name exactly as the REST path does.
    assert knife.market_hash_name == "★ Falchion Knife | Gamma Doppler (Phase 2) (Factory New)"


def test_listed_event_from_captured_feed_feeds_the_price_window():
    """Real (sanitized) listed event: same sale schema as sold, saleId also null."""
    envelope = _load_fixture("skinport_listed_event.json")

    feed_event, ticks = parse_sale_feed_event(envelope["payload"], envelope["receivedAt"])

    assert feed_event.event_type == "listed"
    assert feed_event.payload == envelope["payload"]
    knife, pistol = ticks
    assert all(tick.event_type == "listed" and tick.feeds_price_window for tick in ticks)

    assert knife.market_hash_name == "★ Karambit | Stained (Battle-Scarred)"
    assert knife.price_cents == 48500
    assert knife.listing_id == "60823173"
    assert (knife.pattern, knife.paint_index) == (552, 43)
    assert knife.float_value == pytest.approx(0.46042290329933167)
    assert knife.listing_url == "https://skinport.com/item/karambit-stained-battle-scarred"

    assert pistol.market_hash_name == "StatTrak™ Glock-18 | Vogue (Field-Tested)"
    assert pistol.listing_id == "58367553"


def test_sale_id_upgrades_the_deep_link_when_the_feed_provides_one():
    sale = {
        "productId": 61000001,
        "saleId": 41234567,
        "url": "awp-asiimov-field-tested",
        "marketHashName": "AWP | Asiimov (Field-Tested)",
        "salePrice": 9150,
        "wear": 0.25,
        "pattern": 7,
        "finish": 279,
        "stickers": [],
        "version": "default",
    }

    feed_event, (tick,) = _parse({"eventType": "listed", "sales": [sale]})

    assert feed_event.event_type == "listed"
    assert tick.event_type == "listed"
    assert tick.feeds_price_window
    assert tick.listing_id == "61000001"
    assert tick.listing_url == "https://skinport.com/item/awp-asiimov-field-tested/41234567"


def test_unknown_event_type_is_kept_raw_and_never_feeds_the_price_window():
    payload = {"eventType": "priceChanged", "sales": [{"marketHashName": "Item", "salePrice": 500}]}

    feed_event, (tick,) = _parse(payload)

    assert feed_event.event_type == "priceChanged"
    assert tick.kind == TickKind.OTHER_FEED_EVENT
    assert not tick.feeds_price_window


def test_sales_without_a_name_or_a_price_become_no_tick():
    payload = {"eventType": "listed", "sales": [{"marketHashName": "No Price"}, {"salePrice": 10000}]}

    feed_event, ticks = _parse(payload)

    assert feed_event.payload == payload
    assert ticks == []


def test_event_without_type_or_sales_is_still_recorded():
    feed_event, ticks = _parse({"sales": None})

    assert feed_event.event_type == "unknown"
    assert ticks == []


def test_malformed_sales_are_skipped_without_losing_the_event():
    payload = {
        "eventType": "sold",
        "sales": ["not-an-object", {"marketHashName": "Negative", "salePrice": -5}, {"marketHashName": "Ok", "salePrice": 1}],
    }

    feed_event, ticks = _parse(payload)

    assert feed_event.payload == payload
    assert [tick.market_hash_name for tick in ticks] == ["Ok"]


@pytest.mark.parametrize(
    ("payload", "received_at_ms"),
    [
        pytest.param([], 1, id="payload_not_object"),
        pytest.param(None, 1, id="no_payload"),
        pytest.param({"eventType": "sold"}, 0, id="zero_received_at"),
        pytest.param({"eventType": "sold"}, "soon", id="non_integer_received_at"),
        pytest.param({"eventType": "sold"}, True, id="boolean_received_at"),
    ],
)
def test_unusable_events_are_rejected(payload, received_at_ms):
    with pytest.raises(ValueError):
        parse_sale_feed_event(payload, received_at_ms)


def test_rest_snapshot_ticks_keep_the_legacy_batch_record_plus_their_receive_time():
    tick = MarketTick(
        venue="skinport", kind=TickKind.REST_SNAPSHOT, market_hash_name="Item", price_usd=1.5, timestamp=1_700_000_000
    )

    assert tick.feeds_price_window
    # A REST poll knows its time to the second only, so observed_at_ms is that second (#256).
    assert tick.to_batch_record() == {
        "market_hash_name": "Item",
        "price_cents": 150,
        "timestamp": 1_700_000_000,
        "observed_at_ms": 1_700_000_000_000,
    }


def test_feed_ticks_carry_the_millisecond_receive_time_of_their_event():
    feed_event, (tick,) = parse_sale_feed_event(
        {"eventType": "listed", "sales": [{"marketHashName": "Item", "salePrice": 150, "productId": 7}]}, 1_700_000_000_318
    )

    assert tick.timestamp == 1_700_000_000
    # The same clock as feed_events.received_at, so a label read from the tick matches the raw event (#256).
    assert tick.to_batch_record()["observed_at_ms"] == feed_event.received_at_ms == 1_700_000_000_318


def test_observed_at_ms_must_fall_inside_the_timestamp_second():
    with pytest.raises(ValidationError, match="not inside second"):
        MarketTick(
            venue="skinport",
            kind=TickKind.REST_SNAPSHOT,
            market_hash_name="Item",
            price_usd=1.0,
            timestamp=1_700_000_000,
            observed_at_ms=1_700_000_001_000,
        )


@pytest.mark.parametrize(
    ("event_type", "kind", "feeds_price_window"),
    [
        (None, TickKind.REST_SNAPSHOT, True),
        ("listed", TickKind.LISTED, True),
        ("sold", TickKind.SOLD, False),
        ("priceChanged", TickKind.OTHER_FEED_EVENT, False),
    ],
)
def test_tick_kind_decides_the_routing(event_type, kind, feeds_price_window):
    tick = MarketTick(venue="skinport", kind=kind, market_hash_name="Item", price_usd=1.0, event_type=event_type)

    assert tick.is_rest_snapshot is (kind == TickKind.REST_SNAPSHOT)
    assert tick.feeds_price_window is feeds_price_window


@pytest.mark.parametrize(
    ("kind", "event_type"),
    [
        # A feed parser that forgets the event type must not pass as a REST snapshot (#270).
        (TickKind.LISTED, None),
        (TickKind.REST_SNAPSHOT, "listed"),
        (TickKind.SOLD, "listed"),
        (TickKind.OTHER_FEED_EVENT, "sold"),
    ],
)
def test_tick_kind_must_match_the_event_type(kind, event_type):
    with pytest.raises(ValidationError, match="cannot carry event_type"):
        MarketTick(venue="skinport", kind=kind, market_hash_name="Item", price_usd=1.0, event_type=event_type)


def test_tick_kind_is_required():
    with pytest.raises(ValidationError, match="kind"):
        MarketTick(venue="skinport", market_hash_name="Item", price_usd=1.0)


def test_venue_cannot_hold_a_colon():
    # The venue is the first part of the price window key, so a colon would make keys ambiguous.
    with pytest.raises(ValidationError, match="venue"):
        MarketTick(venue="sky:port", kind=TickKind.REST_SNAPSHOT, market_hash_name="Item", price_usd=1.0)


def test_listing_tick_batch_record_carries_listing_fields():
    tick = MarketTick(
        venue="skinport",
        kind=TickKind.LISTED,
        market_hash_name="Item",
        price_usd=1.5,
        timestamp=1_700_000_000,
        float_value=0.1,
        stickers=[{"name": "Sticker"}],
        event_type="listed",
        listing_id="42",
        pattern=3,
        paint_index=44,
        listing_url="https://skinport.com/item/item/1",
    )

    assert tick.to_batch_record() == {
        "market_hash_name": "Item",
        "price_cents": 150,
        "timestamp": 1_700_000_000,
        "observed_at_ms": 1_700_000_000_000,
        "event_type": "listed",
        "listing_id": "42",
        "float_value": 0.1,
        "pattern": 3,
        "paint_index": 44,
        "listing_url": "https://skinport.com/item/item/1",
        "stickers": [{"name": "Sticker"}],
    }


def test_listing_tick_without_stickers_records_an_explicit_empty_list():
    tick = MarketTick(
        venue="skinport", kind=TickKind.SOLD, market_hash_name="Item", price_usd=1.0, timestamp=1_700_000_000, event_type="sold"
    )

    assert tick.to_batch_record()["stickers"] == []


def test_out_of_range_optional_fields_are_dropped_but_the_price_is_kept():
    sale = {
        "productId": 1,
        "url": "item",
        "marketHashName": "Item",
        "salePrice": 250,
        "wear": 1.2,
        "pattern": -1,
        "finish": True,
    }

    _feed_event, (tick,) = _parse({"eventType": "listed", "sales": [sale]})

    assert tick.price_cents == 250
    assert (tick.float_value, tick.pattern, tick.paint_index) == (None, None, None)


def test_overlong_event_type_is_clamped_and_kept_raw():
    event_type = "x" * 40

    feed_event, (tick,) = _parse({"eventType": event_type, "sales": [{"marketHashName": "Item", "salePrice": 1}]})

    assert feed_event.event_type == tick.event_type == "x" * 32
    assert feed_event.payload["eventType"] == event_type


def test_overlong_listing_url_is_dropped_rather_than_truncated():
    sale = {"marketHashName": "Item", "salePrice": 1, "url": "s" * 600}

    _feed_event, (tick,) = _parse({"eventType": "listed", "sales": [sale]})

    assert tick.listing_url is None


def test_edge_limits_match_the_backend_bulk_ingest_schema():
    """The backend rejects a whole batch (non-retryable 422) on one bad record, so the edge must never
    produce a record the backend would reject. Fails when either side's limits change alone."""
    import models
    import schemas

    def constraints(model, field_name: str) -> dict:
        found: dict = {}
        for item in model.model_fields[field_name].metadata:
            for attribute in ("gt", "ge", "le", "max_length"):
                if getattr(item, attribute, None) is not None:
                    found[attribute] = getattr(item, attribute)
        return found

    shared_tick_fields = ["float_value", "pattern", "paint_index", "event_type", "listing_id", "listing_url", "observed_at_ms"]
    for field_name in shared_tick_fields:
        edge = constraints(models.MarketTick, field_name)
        backend = constraints(schemas.BulkPriceTick, field_name)
        assert edge == backend, field_name

    assert (
        constraints(models.FeedEvent, "event_type")["max_length"]
        == constraints(schemas.BulkFeedEvent, "event_type")["max_length"]
    )
