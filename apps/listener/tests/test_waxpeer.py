import json
from pathlib import Path
from unittest.mock import AsyncMock

import aiohttp
import pytest
from listener_telemetry import feed_connected, feed_events_filtered_total
from models import FeedEvent, MarketTick, TickKind
from scrapers.factory import ScraperFactory
from scrapers.waxpeer import (
    DEFAULT_RECEIVE_TIMEOUT_SECONDS,
    OUTCOME_ABOVE_RESALE_PRICE,
    OUTCOME_MALFORMED,
    OUTCOME_NO_BASELINE,
    OUTCOME_NOT_A_PRICE_CUT,
    OUTCOME_RECORDED,
    OUTCOME_UNTRACKED_REMOVAL,
    RAW_BATCH_EVENT_TYPE,
    FeedConnectionLost,
    RawEventBatch,
    RecordingFilter,
    ResalePrices,
    WaxpeerScraper,
    listing_tick,
    mills_to_cents,
    socketio_event,
    steam_market_hash_name,
    subscribe_frame,
    waxpeer_stickers,
)

FRAMES = (Path(__file__).parent / "fixtures" / "waxpeer_feed_frames.txt").read_text(encoding="utf-8").splitlines()
RECEIVED_AT_MS = 1_790_000_000_000

DOPPLER_NAME = "★ StatTrak™ Survival Knife | Doppler (Phase 1) (Factory New)"
GALIL_NAME = "Galil AR | Black Sand (Factory New)"
GLOCK_NAME = "StatTrak™ Glock-18 | Bullet Queen (Factory New)"
MAC10_NAME = "StatTrak™ MAC-10 | Sakkaku (Field-Tested)"


def _event_payload(name: str) -> dict:
    """The payload of the first fixture frame carrying the named event."""
    for frame in FRAMES:
        event = socketio_event(frame)
        if event is not None and event[0] == name:
            return event[1]
    raise AssertionError(f"no {name} frame in the fixture")


def _payload_for(item_name_fragment: str) -> dict:
    for frame in FRAMES:
        event = socketio_event(frame)
        if event is not None and isinstance(event[1], dict) and item_name_fragment in str(event[1].get("name")):
            return event[1]
    raise AssertionError(item_name_fragment)


def _prices(latest: dict[str, int], stickers: dict[str, int] | None = None) -> ResalePrices:
    prices = ResalePrices("skinport")
    prices.latest_price_cents = dict(latest)
    prices.sticker_prices_cents = dict(stickers or {})
    return prices


def _listing(item_id: str = "1", price: int = 10_000, name: str = "AK-47 | Redline (Field-Tested)", **extra) -> dict:
    return {"item_id": item_id, "name": name, "price": price, "game": "csgo", **extra}


# --- Parsing ---


@pytest.mark.parametrize(
    ("mills", "cents"),
    [
        (67_550, 6_755),
        (1_592, 160),
        (10, 1),
        (1, 1),
        ("1000", 100),
        (0, None),
        (-5, None),
        (None, None),
        (True, None),
        (12.5, None),
    ],
    ids=["exact", "rounds_up", "ten_mills", "one_mill", "digit_string", "zero", "negative", "missing", "bool", "float"],
)
def test_mills_to_cents_never_understates_a_price(mills, cents):
    assert mills_to_cents(mills) == cents


@pytest.mark.parametrize(
    ("name", "phase", "expected"),
    [
        ("★ Karambit | Doppler Phase 3 (Factory New)", "Phase 3", "★ Karambit | Doppler (Factory New)"),
        ("★ Flip Knife | Doppler Black Pearl (Factory New)", "Black Pearl", "★ Flip Knife | Doppler (Factory New)"),
        ("★ Karambit | Doppler Ruby", "Ruby", "★ Karambit | Doppler"),
        ("AK-47 | Redline (Field-Tested)", None, "AK-47 | Redline (Field-Tested)"),
        ("★ Karambit | Doppler (Factory New)", "Phase 2", "★ Karambit | Doppler (Factory New)"),
    ],
    ids=["inline_phase", "gem", "no_wear", "no_phase", "phase_not_in_name"],
)
def test_steam_market_hash_name_drops_the_inline_phase(name, phase, expected):
    assert steam_market_hash_name(name, phase) == expected


def test_waxpeer_stickers_match_skinport_sticker_names():
    stickers = waxpeer_stickers(["Sticker | Crown (Foil)", "Charm | Lil' Zen", "", 7])
    assert stickers == [{"name": "Crown (Foil)"}, {"name": "Charm | Lil' Zen"}]
    assert waxpeer_stickers(None) == []


def test_doppler_listing_becomes_a_versioned_listed_tick():
    tick = listing_tick(_payload_for("Survival Knife"), RECEIVED_AT_MS)

    assert tick is not None
    assert tick.venue == "waxpeer"
    assert tick.kind == TickKind.LISTED
    assert tick.event_type == "listed"
    assert tick.market_hash_name == DOPPLER_NAME
    assert tick.price_cents == 22_281  # 222,809 mills rounded up to the next cent
    assert tick.listing_id == "53862560251"
    assert tick.paint_index == 418
    assert tick.pattern is None  # the feed sends no paint seed
    assert tick.float_value == pytest.approx(0.004105276428163052)
    assert tick.stickers == []
    assert tick.timestamp == RECEIVED_AT_MS // 1000


def test_listing_with_stickers_and_trade_lock_is_parsed():
    tick = listing_tick(_payload_for("Bullet Queen"), RECEIVED_AT_MS)

    assert tick is not None
    assert tick.market_hash_name == GLOCK_NAME
    assert tick.stickers == [{"name": "donk (Champion) | Shanghai 2024"}, {"name": "Head In Hands"}]


@pytest.mark.parametrize(
    "payload",
    [None, [], {"name": "X", "price": 100}, {"item_id": "1", "price": 100}, {"item_id": "1", "name": "X", "price": 0}],
    ids=["none", "not_a_dict", "no_item_id", "no_name", "no_price"],
)
def test_unusable_listing_payloads_give_no_tick(payload):
    assert listing_tick(payload, RECEIVED_AT_MS) is None


def test_socketio_event_frames():
    assert socketio_event('42["new",{"item_id":"1"}]') == ("new", {"item_id": "1"})
    assert socketio_event('42["ping"]') == ("ping", None)
    assert socketio_event("2") is None
    assert socketio_event('42{"not":"a list"}') is None
    with pytest.raises(ValueError):
        socketio_event("42[broken")


def test_subscribe_frame():
    assert subscribe_frame("csgo") == '42["sub",{"name":"csgo"}]'


# --- Resale prices ---


class _HashRedis:
    """Just enough of the edge Redis for ResalePrices: hashes read with HGETALL."""

    def __init__(self, hashes: dict[str, dict[str, str]]) -> None:
        self.hashes = hashes
        self.reads: list[str] = []

    async def hgetall(self, key: str) -> dict[str, str]:
        self.reads.append(key)
        return dict(self.hashes.get(key, {}))

    async def aclose(self) -> None:
        return None


def _skinport_hashes(build_id: str = "4") -> dict[str, dict[str, str]]:
    return {
        "baseline_meta:skinport": {"build_id": build_id, "built_at": "2026-09-30T00:00:00", "item_count": "3"},
        "baselines:skinport": {
            GALIL_NAME: json.dumps({"latest_price_cents": 1500}),
            DOPPLER_NAME: json.dumps({"latest_price_cents": 30_000}),
            "Broken": "{not json",
            "No price": json.dumps({"rolling_30d_avg_cents": 100}),
        },
        "sticker_prices:skinport": {"Crown (Foil)": "20000", "Garbled": "x"},
    }


@pytest.mark.asyncio
async def test_resale_prices_load_a_build_once():
    cache = _HashRedis(_skinport_hashes())
    prices = ResalePrices("skinport")

    assert await prices.refresh(cache) is True
    assert prices.build_id == "4"
    assert prices.latest_price_cents == {GALIL_NAME: 1500, DOPPLER_NAME: 30_000}
    assert prices.sticker_prices_cents == {"Crown (Foil)": 20_000}

    cache.reads.clear()
    assert await prices.refresh(cache) is False
    assert cache.reads == ["baseline_meta:skinport"]  # an unchanged build is not read again

    cache.hashes = _skinport_hashes(build_id="5")
    assert await prices.refresh(cache) is True
    assert prices.build_id == "5"


@pytest.mark.asyncio
async def test_resale_prices_without_a_loaded_build_hold_nothing():
    prices = ResalePrices("skinport")
    assert await prices.refresh(_HashRedis({})) is False
    assert prices.latest_price_cents == {}


def _tick(price_cents: int, name: str = GALIL_NAME, stickers: list[dict] | None = None) -> MarketTick:
    return MarketTick(
        venue="waxpeer",
        kind=TickKind.LISTED,
        market_hash_name=name,
        price_usd=price_cents / 100,
        event_type="listed",
        listing_id="1",
        stickers=stickers or [],
    )


@pytest.mark.parametrize(
    ("price_cents", "stickers", "expected"),
    [
        (1499, [], OUTCOME_RECORDED),
        (1500, [], OUTCOME_ABOVE_RESALE_PRICE),  # at the resale price is not below it
        (2000, [], OUTCOME_ABOVE_RESALE_PRICE),
        # $500 of stickers: the DRE's sticker rule approves up to 3% ($15) above the resale price.
        (3000, [{"name": "Crown (Foil)"}] * 25, OUTCOME_RECORDED),
        (3001, [{"name": "Crown (Foil)"}] * 25, OUTCOME_ABOVE_RESALE_PRICE),
        # $100 of stickers is not more than the rule's minimum.
        (1501, [{"name": "Crown (Foil)"}] * 5, OUTCOME_ABOVE_RESALE_PRICE),
    ],
    ids=["below", "equal", "above", "sticker_premium_at_limit", "sticker_premium_over_limit", "stickers_at_minimum"],
)
def test_judge_records_what_could_resell_for_more(price_cents, stickers, expected):
    prices = _prices({GALIL_NAME: 1500}, {"Crown (Foil)": 2000})
    assert prices.judge(_tick(price_cents, stickers=stickers)) == expected


def test_judge_without_a_resale_baseline():
    assert _prices({}).judge(_tick(100)) == OUTCOME_NO_BASELINE


# --- Recording filter ---


def test_new_listing_below_resale_price_is_recorded_and_followed():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}))

    outcome, tick = recording_filter.listing("new", _listing(price=10_000), RECEIVED_AT_MS)

    assert outcome == OUTCOME_RECORDED
    assert tick is not None and tick.price_cents == 1000
    assert recording_filter.tracked_count == 1


def test_new_listing_above_resale_price_is_dropped():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}))

    assert recording_filter.listing("new", _listing(price=20_000), RECEIVED_AT_MS) == (OUTCOME_ABOVE_RESALE_PRICE, None)
    assert recording_filter.tracked_count == 0


def test_followed_listing_records_only_price_cuts():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}))
    recording_filter.listing("new", _listing(price=12_000), RECEIVED_AT_MS)

    assert recording_filter.listing("update", _listing(price=13_000), RECEIVED_AT_MS)[0] == OUTCOME_NOT_A_PRICE_CUT
    assert recording_filter.listing("update", _listing(price=13_000), RECEIVED_AT_MS)[0] == OUTCOME_NOT_A_PRICE_CUT
    # A cut is compared with the latest price (1300), not the recorded one (1200).
    outcome, tick = recording_filter.listing("update", _listing(price=12_500), RECEIVED_AT_MS)
    assert outcome == OUTCOME_RECORDED
    assert tick is not None and tick.price_cents == 1250


def test_followed_listing_cut_to_a_price_still_above_resale_is_not_recorded():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}, {"Crown (Foil)": 2000}))
    stickers = ["Sticker | Crown (Foil)"] * 25
    # Recorded through the sticker exception, at 1510 (above the resale price).
    assert (
        recording_filter.listing("new", _listing(price=15_100, sticker_names=stickers), RECEIVED_AT_MS)[0] == OUTCOME_RECORDED
    )
    # Cut to 1505 with the stickers gone from the payload: a cut, but no longer worth recording.
    assert recording_filter.listing("update", _listing(price=15_050), RECEIVED_AT_MS) == (OUTCOME_ABOVE_RESALE_PRICE, None)
    assert recording_filter.tracked_count == 1  # still followed, so its removal is recorded


def test_first_sighting_through_an_update_is_judged_like_a_new_listing():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}))

    outcome, tick = recording_filter.listing("update", _listing(price=9_000), RECEIVED_AT_MS)

    assert outcome == OUTCOME_RECORDED
    assert tick is not None


def test_relisting_that_no_longer_qualifies_is_unfollowed():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}))
    recording_filter.listing("new", _listing(price=10_000), RECEIVED_AT_MS)

    assert recording_filter.listing("new", _listing(price=20_000), RECEIVED_AT_MS)[0] == OUTCOME_ABOVE_RESALE_PRICE
    assert recording_filter.tracked_count == 0


def test_removal_of_a_followed_listing_is_recorded_under_its_item():
    recording_filter = RecordingFilter(_prices({DOPPLER_NAME: 30_000}))
    recording_filter.listing("new", _payload_for("Survival Knife"), RECEIVED_AT_MS)

    # A removal carries the name with the phase written in, and here no price.
    outcome, tick = recording_filter.removal(
        {"item_id": "53862560251", "name": "★ StatTrak™ Survival Knife | Doppler Phase 1 (Factory New)"}, RECEIVED_AT_MS
    )

    assert outcome == OUTCOME_RECORDED
    assert tick is not None
    assert tick.kind == TickKind.OTHER_FEED_EVENT
    assert tick.event_type == "removed"
    assert tick.market_hash_name == DOPPLER_NAME
    assert tick.price_cents == 22_281  # the followed listing's price
    assert not tick.feeds_price_window
    assert recording_filter.tracked_count == 0


def test_removal_of_an_unfollowed_listing_is_dropped():
    recording_filter = RecordingFilter(_prices({}))
    assert recording_filter.removal(_event_payload("removed"), RECEIVED_AT_MS) == (OUTCOME_UNTRACKED_REMOVAL, None)
    assert recording_filter.removal({"name": "no id"}, RECEIVED_AT_MS) == (OUTCOME_MALFORMED, None)


def test_malformed_listing_is_counted_as_malformed():
    recording_filter = RecordingFilter(_prices({}))
    assert recording_filter.listing("new", {"item_id": "1"}, RECEIVED_AT_MS) == (OUTCOME_MALFORMED, None)


def test_followed_listings_are_capped_least_recent_first():
    recording_filter = RecordingFilter(_prices({"AK-47 | Redline (Field-Tested)": 1500}), max_tracked=2)
    for item_id in ("1", "2", "3"):
        recording_filter.listing("new", _listing(item_id=item_id, price=10_000), RECEIVED_AT_MS)

    assert recording_filter.tracked_count == 2
    assert recording_filter.removal({"item_id": "1"}, RECEIVED_AT_MS)[0] == OUTCOME_UNTRACKED_REMOVAL
    assert recording_filter.removal({"item_id": "3"}, RECEIVED_AT_MS)[0] == OUTCOME_RECORDED


# --- Raw payload batches ---


def test_raw_batch_is_handed_on_when_full():
    raw_batch = RawEventBatch(max_events=2, max_seconds=10)
    raw_batch.add("new", {"item_id": "1"}, RECEIVED_AT_MS)
    assert raw_batch.take_if_due(RECEIVED_AT_MS) is None
    raw_batch.add("removed", {"item_id": "1"}, RECEIVED_AT_MS + 5)

    feed_event = raw_batch.take_if_due(RECEIVED_AT_MS + 5)

    assert isinstance(feed_event, FeedEvent)
    assert feed_event.event_type == RAW_BATCH_EVENT_TYPE
    assert feed_event.received_at_ms == RECEIVED_AT_MS
    assert feed_event.payload == {
        "venue": "waxpeer",
        "channel": "csgo",
        "events": [
            {"event": "new", "received_at_ms": RECEIVED_AT_MS, "data": {"item_id": "1"}},
            {"event": "removed", "received_at_ms": RECEIVED_AT_MS + 5, "data": {"item_id": "1"}},
        ],
    }
    assert raw_batch.take() is None


def test_raw_batch_is_handed_on_when_old_enough():
    raw_batch = RawEventBatch(max_events=500, max_seconds=10)
    assert raw_batch.take_if_due(RECEIVED_AT_MS) is None
    raw_batch.add("new", {"item_id": "1"}, RECEIVED_AT_MS)

    assert raw_batch.take_if_due(RECEIVED_AT_MS + 9_999) is None
    assert raw_batch.take_if_due(RECEIVED_AT_MS + 10_000) is not None


# --- Scraper and protocol ---


class _Message:
    def __init__(self, data: str | None = None, type_: aiohttp.WSMsgType = aiohttp.WSMsgType.TEXT) -> None:
        self.type = type_
        self.data = data


class _FakeWebSocket:
    def __init__(self, messages: list[_Message]) -> None:
        self.messages = list(messages)
        self.sent: list[str] = []
        self.timeouts: list[float | None] = []

    async def receive(self, timeout: float | None = None) -> _Message:
        self.timeouts.append(timeout)
        if not self.messages:
            return _Message(type_=aiohttp.WSMsgType.CLOSED)
        return self.messages.pop(0)

    async def send_str(self, data: str) -> None:
        self.sent.append(data)

    def exception(self) -> Exception:
        return RuntimeError("boom")

    async def __aenter__(self) -> "_FakeWebSocket":
        return self

    async def __aexit__(self, *exc) -> None:
        return None


class _FakeSession:
    def __init__(self, ws: _FakeWebSocket) -> None:
        self.ws = ws
        self.headers: dict[str, str] | None = None
        self.closed = False

    def ws_connect(self, url: str, headers: dict[str, str], heartbeat: float | None) -> _FakeWebSocket:
        self.url = url
        self.headers = headers
        return self.ws

    async def close(self) -> None:
        self.closed = True


def _scraper(monkeypatch, messages: list[_Message], hashes: dict | None = None, api_key: str | None = "test-key"):
    if api_key is None:
        monkeypatch.delenv("WAXPEER_API_KEY", raising=False)
    else:
        monkeypatch.setenv("WAXPEER_API_KEY", api_key)
    scraper = WaxpeerScraper()
    ws = _FakeWebSocket(messages)
    session = _FakeSession(ws)
    cache = _HashRedis(_skinport_hashes() if hashes is None else hashes)
    monkeypatch.setattr(scraper, "_open_session", lambda: session)
    monkeypatch.setattr(scraper, "_open_cache", lambda: cache)
    return scraper, ws, session


async def _drain(scraper: WaxpeerScraper) -> list[MarketTick | FeedEvent]:
    items: list[MarketTick | FeedEvent] = []
    with pytest.raises(FeedConnectionLost):
        async for item in scraper.listen_websocket_stream():
            items.append(item)
    return items


@pytest.mark.asyncio
async def test_feed_session_speaks_engine_io_and_records_what_qualifies(monkeypatch):
    scraper, ws, session = _scraper(monkeypatch, [_Message(frame) for frame in FRAMES])
    recorded_before = feed_events_filtered_total.labels(event="new", outcome=OUTCOME_RECORDED)._value.get()

    items = await _drain(scraper)

    assert session.headers is not None and session.headers["authorization"] == "test-key"
    # Open -> connect to the default namespace; namespace connected -> subscribe; ping -> pong.
    assert ws.sent == ["40", subscribe_frame("csgo"), "3"]
    # Until the open packet arrives the default timeout applies, then ping interval + ping timeout.
    assert ws.timeouts[0] == DEFAULT_RECEIVE_TIMEOUT_SECONDS
    assert ws.timeouts[-1] == pytest.approx(45.0)

    ticks = [item for item in items if isinstance(item, MarketTick)]
    batches = [item for item in items if isinstance(item, FeedEvent)]
    # The Doppler (22,281 < 30,000) is recorded; the Galil (13,403 mills = 1,341 cents < 1,500) too.
    # The Glock and MAC-10 have no Skinport baseline here, and the removal is of an unfollowed listing.
    assert [tick.market_hash_name for tick in ticks] == [DOPPLER_NAME, GALIL_NAME]
    # The raw payloads still pending when the connection closed are handed on in one batch.
    assert len(batches) == 1
    assert [event["event"] for event in batches[0].payload["events"]] == ["new", "new"]
    assert batches[0].payload["events"][0]["data"]["item_id"] == "53862560251"
    recorded_after = feed_events_filtered_total.labels(event="new", outcome=OUTCOME_RECORDED)._value.get()
    assert recorded_after - recorded_before == 2
    assert session.closed
    assert feed_connected._value.get() == 0


@pytest.mark.asyncio
async def test_feed_without_resale_baselines_records_nothing(monkeypatch):
    scraper, _, _ = _scraper(monkeypatch, [_Message(frame) for frame in FRAMES], hashes={})

    items = await _drain(scraper)

    assert items == []


@pytest.mark.asyncio
async def test_feed_connects_without_a_key(monkeypatch):
    scraper, _, session = _scraper(monkeypatch, [], api_key=None)

    await _drain(scraper)

    assert session.headers is not None and "authorization" not in session.headers


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("frame", "reason"),
    [('44{"message":"Not authorized"}', "refused"), ("41", "ended"), ("1", "ended")],
    ids=["connect_error", "socket_disconnect", "engine_close"],
)
async def test_server_ending_the_session_raises(monkeypatch, frame, reason):
    scraper, _, _ = _scraper(monkeypatch, [_Message(frame)])

    with pytest.raises(FeedConnectionLost, match=reason):
        async for _ in scraper.listen_websocket_stream():
            pass


@pytest.mark.asyncio
async def test_websocket_error_raises(monkeypatch):
    scraper, _, _ = _scraper(monkeypatch, [_Message(type_=aiohttp.WSMsgType.ERROR)])

    with pytest.raises(FeedConnectionLost, match="boom"):
        async for _ in scraper.listen_websocket_stream():
            pass


@pytest.mark.asyncio
async def test_silent_server_times_out(monkeypatch):
    scraper, ws, _ = _scraper(monkeypatch, [])
    ws.receive = AsyncMock(side_effect=TimeoutError())

    with pytest.raises(FeedConnectionLost, match="TimeoutError"):
        async for _ in scraper.listen_websocket_stream():
            pass


@pytest.mark.asyncio
async def test_binary_and_unparsable_frames_are_skipped(monkeypatch):
    malformed_before = feed_events_filtered_total.labels(event="unparsed", outcome=OUTCOME_MALFORMED)._value.get()
    scraper, _, _ = _scraper(monkeypatch, [_Message(b"\x00", aiohttp.WSMsgType.BINARY), _Message("42[broken")])

    assert await _drain(scraper) == []
    malformed_after = feed_events_filtered_total.labels(event="unparsed", outcome=OUTCOME_MALFORMED)._value.get()
    assert malformed_after - malformed_before == 1


def test_receive_timeout_falls_back_on_a_bad_open_packet():
    assert WaxpeerScraper._receive_timeout("0{broken") == DEFAULT_RECEIVE_TIMEOUT_SECONDS
    assert WaxpeerScraper._receive_timeout('0{"pingInterval":25000,"pingTimeout":5000}') == 30.0


@pytest.mark.asyncio
async def test_waxpeer_has_no_rest_poll():
    scraper = WaxpeerScraper()
    assert scraper.polls_rest is False
    assert [tick async for tick in scraper.poll_market_stream()] == []


def test_factory_builds_the_waxpeer_scraper():
    assert isinstance(ScraperFactory.get_scraper("waxpeer"), WaxpeerScraper)
