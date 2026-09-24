import json
from pathlib import Path

import pytest
from models import MarketTick
from scrapers.skinport import parse_sale_feed_message

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _envelope(payload: dict, received_at_ms: int = 1_790_000_000_000) -> str:
    return json.dumps({"receivedAt": received_at_ms, "payload": payload})


def test_sold_event_from_captured_feed_parses_every_sale():
    """Real (sanitized) sold event: saleId is null, productId joins the listing to its outcome."""
    envelope = _load_fixture("skinport_sold_event.json")

    feed_event, ticks = parse_sale_feed_message(json.dumps(envelope))

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

    feed_event, ticks = parse_sale_feed_message(json.dumps(envelope))

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

    feed_event, (tick,) = parse_sale_feed_message(_envelope({"eventType": "listed", "sales": [sale]}))

    assert feed_event.event_type == "listed"
    assert tick.event_type == "listed"
    assert tick.feeds_price_window
    assert tick.listing_id == "61000001"
    assert tick.listing_url == "https://skinport.com/item/awp-asiimov-field-tested/41234567"


def test_unknown_event_type_is_kept_raw_and_never_feeds_the_price_window():
    payload = {"eventType": "priceChanged", "sales": [{"marketHashName": "Item", "salePrice": 500}]}

    feed_event, (tick,) = parse_sale_feed_message(_envelope(payload))

    assert feed_event.event_type == "priceChanged"
    assert not tick.feeds_price_window


def test_event_without_type_or_sales_is_still_recorded():
    feed_event, ticks = parse_sale_feed_message(_envelope({"sales": None}))

    assert feed_event.event_type == "unknown"
    assert ticks == []


def test_malformed_sales_are_skipped_without_losing_the_event():
    payload = {
        "eventType": "sold",
        "sales": ["not-an-object", {"marketHashName": "Negative", "salePrice": -5}, {"marketHashName": "Ok", "salePrice": 1}],
    }

    feed_event, ticks = parse_sale_feed_message(_envelope(payload))

    assert feed_event.payload == payload
    assert [tick.market_hash_name for tick in ticks] == ["Ok"]


@pytest.mark.parametrize(
    "message",
    [
        pytest.param("[]", id="not_an_object"),
        pytest.param(json.dumps({"receivedAt": 1, "payload": []}), id="payload_not_object"),
        pytest.param(json.dumps({"payload": {"eventType": "sold"}}), id="missing_received_at"),
        pytest.param(json.dumps({"receivedAt": "soon", "payload": {}}), id="non_integer_received_at"),
    ],
)
def test_unusable_envelopes_are_rejected(message):
    with pytest.raises(ValueError):
        parse_sale_feed_message(message)


def test_rest_snapshot_ticks_keep_the_legacy_batch_record():
    tick = MarketTick(venue="skinport", market_hash_name="Item", price_usd=1.5, timestamp=1_700_000_000)

    assert tick.feeds_price_window
    assert tick.to_batch_record() == {"market_hash_name": "Item", "price_cents": 150, "timestamp": 1_700_000_000}


def test_listing_tick_batch_record_carries_listing_fields():
    tick = MarketTick(
        venue="skinport",
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
        "event_type": "listed",
        "listing_id": "42",
        "float_value": 0.1,
        "pattern": 3,
        "paint_index": 44,
        "listing_url": "https://skinport.com/item/item/1",
        "stickers": [{"name": "Sticker"}],
    }


def test_listing_tick_without_stickers_records_an_explicit_empty_list():
    tick = MarketTick(venue="skinport", market_hash_name="Item", price_usd=1.0, timestamp=1_700_000_000, event_type="sold")

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

    _feed_event, (tick,) = parse_sale_feed_message(_envelope({"eventType": "listed", "sales": [sale]}))

    assert tick.price_cents == 250
    assert (tick.float_value, tick.pattern, tick.paint_index) == (None, None, None)


def test_overlong_event_type_is_clamped_and_kept_raw():
    event_type = "x" * 40

    feed_event, (tick,) = parse_sale_feed_message(
        _envelope({"eventType": event_type, "sales": [{"marketHashName": "Item", "salePrice": 1}]})
    )

    assert feed_event.event_type == tick.event_type == "x" * 32
    assert feed_event.payload["eventType"] == event_type


def test_overlong_listing_url_is_dropped_rather_than_truncated():
    sale = {"marketHashName": "Item", "salePrice": 1, "url": "s" * 600}

    _feed_event, (tick,) = parse_sale_feed_message(_envelope({"eventType": "listed", "sales": [sale]}))

    assert tick.listing_url is None


def test_edge_limits_match_the_backend_bulk_ingest_schema():
    """The backend rejects a whole batch (non-retryable 422) on one bad record, so the edge must never
    produce a record the backend would reject. Fails when either side's limits change alone."""
    import models
    import schemas

    def constraints(model, field_name: str) -> dict:
        found: dict = {}
        for item in model.model_fields[field_name].metadata:
            for attribute in ("ge", "le", "max_length"):
                if getattr(item, attribute, None) is not None:
                    found[attribute] = getattr(item, attribute)
        return found

    shared_tick_fields = ["float_value", "pattern", "paint_index", "event_type", "listing_id", "listing_url"]
    for field_name in shared_tick_fields:
        edge = constraints(models.MarketTick, field_name)
        backend = constraints(schemas.BulkPriceTick, field_name)
        assert edge == backend, field_name

    assert (
        constraints(models.FeedEvent, "event_type")["max_length"]
        == constraints(schemas.BulkFeedEvent, "event_type")["max_length"]
    )
