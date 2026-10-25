import base64
import dataclasses
import json
from datetime import UTC, datetime
from pathlib import Path

import aiohttp
import pytest
from listener_telemetry import (
    dmarket_catch_up_seconds,
    dmarket_newest_listing_age_seconds,
    dmarket_rate_limited_total,
    dmarket_requests_total,
    feed_connected,
    feed_events_filtered_total,
)
from models import FeedEvent, MarketTick, TickKind
from scrapers import dmarket
from scrapers.base import FeedConnectionLost
from scrapers.dmarket import (
    EVENT_CATCH_UP,
    EVENT_LIVE,
    LIVE_OVERLAP_MS,
    OUTCOME_PERSON_TO_PERSON,
    RESUME_KEY,
    RESUME_OVERLAP_MS,
    CatchUpGap,
    DMarketScraper,
    ListingPoller,
    ResumePoint,
    listing_tick,
    listing_url,
    parse_resume_document,
    raw_listing,
)
from scrapers.listing_filter import (
    OUTCOME_ABOVE_RESALE_PRICE,
    OUTCOME_MALFORMED,
    OUTCOME_NO_BASELINE,
    OUTCOME_RECORDED,
    RawEventBatch,
)
from shared_utils import edge_baseline_meta_key, edge_baselines_key, edge_sticker_prices_key
from shared_utils.dmarket import DMarketAPIError, DMarketKeys, OffersPage, offers_cursor, parse_offer

FIXTURES = Path(__file__).parent / "fixtures"
REDLINE = "AK-47 | Redline (Field-Tested)"
# 2026-10-25T13:42:58Z, the time of the newest listing in the recorded page.
T0_MS = 1_792_935_778_000
RECEIVED_AT_MS = T0_MS + 4_000
KEYS = DMarketKeys.from_hex("ab" * 32, "01" * 64)


def _iso(created_ms: int) -> str:
    return datetime.fromtimestamp(created_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _entry(
    offer_id: str,
    created_ms: int,
    price_cents: int = 1000,
    title: str = REDLINE,
    offer_type: str = "dmarket",
    stickers: tuple[str, ...] = (),
) -> dict:
    return {
        "offerId": offer_id,
        "createdAt": _iso(created_ms),
        "priceCents": str(price_cents),
        "offerType": offer_type,
        "discountPercent": 4,
        "locked": False,
        "attributes": {
            "id": f"asset-{offer_id}",
            "title": title,
            "tradable": True,
            "tradeLockDays": 0,
            "botId": "76561190000000003",
            "owner": "00000000-0000-4000-8000-000000000001",
            "steamAssetId": "10000000004",
            "cs2": {
                "float": "0.25",
                "paintSeed": 12,
                "paintIndex": 282,
                "phase": "",
                "stickers": [{"name": name, "wear": ""} for name in stickers],
                "inspectInGameUri": "steam://run/730//+csgo_econ_action_preview%20SANITIZED",
            },
        },
    }


class FakeMarket:
    """Serves listings newest first and by offer ID within a second, with DMarket's keyset cursor."""

    def __init__(self, entries: list[dict], page_size: int = 3, fail_after: int | None = None) -> None:
        self.page_size = page_size
        self.fail_after = fail_after
        self.cursors: list[str | None] = []
        self.entries: list[dict] = []
        self.add(*entries)

    def add(self, *entries: dict) -> None:
        self.entries = sorted([*self.entries, *entries], key=lambda entry: (-self._created(entry), entry["offerId"]))

    @staticmethod
    def _created(entry: dict) -> int:
        offer = parse_offer(entry)
        assert offer is not None
        return offer.created_at_ms

    async def offers(self, cursor: str | None = None) -> OffersPage:
        self.cursors.append(cursor)
        if self.fail_after is not None and len(self.cursors) > self.fail_after:
            raise DMarketAPIError(502, "bad gateway")
        start = 0
        if cursor is not None:
            position = json.loads(base64.b64decode(cursor))
            created_ms, offer_id = position
            start = len(self.entries)
            for index, entry in enumerate(self.entries):
                created = self._created(entry)
                if created < created_ms or (created == created_ms and entry["offerId"] > offer_id):
                    start = index
                    break
        page = self.entries[start : start + self.page_size]
        more = start + self.page_size < len(self.entries)
        next_cursor = offers_cursor(self._created(page[-1]), page[-1]["offerId"]) if page and more else None
        return OffersPage(entries=page, cursor=next_cursor)


class Clock:
    def __init__(self, now_ms: int = RECEIVED_AT_MS) -> None:
        self.now_ms = now_ms

    def __call__(self) -> int:
        return self.now_ms


def _ids(read) -> list[str]:
    return [offer.offer_id for offer, _entry, _received in read]


def _listings(count: int, newest_ms: int, prefix: str = "n", step_ms: int = 1_000) -> list[dict]:
    """`count` listings one `step_ms` apart, the newest at `newest_ms`."""
    return [_entry(f"{prefix}{index:03d}", newest_ms - index * step_ms) for index in range(count)]


# --- Ticks and raw payloads ---


def test_bot_listing_becomes_a_listed_tick_observed_when_its_page_arrived():
    entries = json.loads((FIXTURES / "dmarket_offers_page.json").read_text(encoding="utf-8"))["items"]
    offer = parse_offer(entries[0])
    assert offer is not None

    tick = listing_tick(offer, RECEIVED_AT_MS)

    assert tick is not None
    assert (tick.venue, tick.kind, tick.event_type) == ("dmarket", TickKind.LISTED, "listed")
    assert tick.market_hash_name == "★ Specialist Gloves | Big Swell (Field-Tested)"
    assert tick.price_cents == 21345
    assert (tick.timestamp, tick.observed_at_ms) == (RECEIVED_AT_MS // 1000, RECEIVED_AT_MS)
    assert (tick.listing_id, tick.float_value, tick.pattern, tick.paint_index) == (offer.offer_id, 0.23134745657444, 873, 1437)
    assert tick.listing_url == (
        "https://dmarket.com/ingame-items/item-list/csgo-skins?title="
        "%E2%98%85%20Specialist%20Gloves%20%7C%20Big%20Swell%20%28Field-Tested%29"
    )


def test_stickers_are_named_like_skinport_stickers():
    offer = parse_offer(_entry("s", T0_MS, stickers=("Crown (Foil)",)))
    assert offer is not None
    tick = listing_tick(offer, RECEIVED_AT_MS)
    assert tick is not None and tick.stickers == [{"name": "Crown (Foil)"}]


def test_too_long_names_get_no_link_and_too_long_ids_no_tick():
    assert listing_url("x" * 600) is None
    offer = parse_offer(_entry("i" * 65, T0_MS))
    assert offer is not None and listing_tick(offer, RECEIVED_AT_MS) is None


def test_a_listing_the_tick_model_refuses_gets_no_tick():
    offer = parse_offer(_entry("w", T0_MS))
    assert offer is not None
    assert listing_tick(dataclasses.replace(offer, float_value=1.5), RECEIVED_AT_MS) is None


def test_raw_listing_without_item_details_keeps_its_attributes():
    entry = _entry("a", T0_MS)
    del entry["attributes"]["cs2"]
    assert raw_listing(entry)["attributes"] == {"id": "asset-a", "title": REDLINE, "tradable": True, "tradeLockDays": 0}


def test_raw_listing_keeps_what_describes_it_and_drops_account_ids_and_links():
    raw = raw_listing(_entry("a", T0_MS))

    assert raw["offerId"] == "a" and raw["priceCents"] == "1000" and raw["offerType"] == "dmarket"
    assert raw["attributes"]["title"] == REDLINE and raw["attributes"]["id"] == "asset-a"
    assert raw["attributes"]["cs2"]["paintSeed"] == 12
    for dropped in ("botId", "owner", "steamAssetId"):
        assert dropped not in raw["attributes"]
    assert "inspectInGameUri" not in raw["attributes"]["cs2"]
    assert raw_listing({"offerId": "b", "attributes": "?"}) == {"offerId": "b"}


# --- Live poll ---


@pytest.mark.asyncio
async def test_first_poll_without_a_resume_point_reads_only_the_newest_page():
    market = FakeMarket(_listings(7, T0_MS))
    poller = ListingPoller()

    read = await poller.live_poll(market.offers, Clock())

    # Oldest first, so the price window sees them in order.
    assert _ids(read) == ["n002", "n001", "n000"]
    assert market.cursors == [None]
    assert poller.newest_ms == T0_MS
    assert not poller.gaps


@pytest.mark.asyncio
async def test_later_polls_read_down_to_the_previous_newest_listing_and_skip_what_they_read():
    market = FakeMarket(_listings(3, T0_MS))
    poller = ListingPoller()
    await poller.live_poll(market.offers, Clock())

    market.add(*_listings(4, T0_MS + 8_000, prefix="m", step_ms=2_000))
    read = await poller.live_poll(market.offers, Clock())

    # Four new listings (8 s down to 2 s after T0); the overlap pages back to T0 - 10 s and reads nothing twice.
    assert _ids(read) == ["m003", "m002", "m001", "m000"]
    assert len(market.cursors) == 1 + 3
    assert poller.newest_ms == T0_MS + 8_000

    assert await poller.live_poll(market.offers, Clock()) == []


@pytest.mark.asyncio
async def test_a_listing_that_shows_up_late_within_the_overlap_is_still_read():
    market = FakeMarket(_listings(3, T0_MS))
    poller = ListingPoller()
    await poller.live_poll(market.offers, Clock())

    # Created 3 s before the newest listing already read, but visible only now.
    market.add(_entry("late", T0_MS - 3_000))
    read = await poller.live_poll(market.offers, Clock())

    assert _ids(read) == ["late"]
    market.add(_entry("too-late", T0_MS - LIVE_OVERLAP_MS - 5_000))
    assert await poller.live_poll(market.offers, Clock()) == []


@pytest.mark.asyncio
async def test_a_poll_that_falls_behind_leaves_the_rest_to_catching_up(monkeypatch):
    monkeypatch.setattr(dmarket, "LIVE_MAX_PAGES", 2)
    market = FakeMarket(_listings(2, T0_MS))
    poller = ListingPoller()
    await poller.live_poll(market.offers, Clock())

    # 12 listings arrived while the connection was down: two pages now, the rest later.
    market.add(*_listings(12, T0_MS + 12_000, prefix="m"))
    live = await poller.live_poll(market.offers, Clock())
    assert _ids(live) == ["m005", "m004", "m003", "m002", "m001", "m000"]
    assert len(poller.gaps) == 1
    assert poller.gaps[0].until_ms == T0_MS - LIVE_OVERLAP_MS
    assert poller.catch_up_remaining_ms() == 6_000 + LIVE_OVERLAP_MS + 1_000

    caught_up = []
    while poller.gaps:
        caught_up += _ids(await poller.catch_up_page(market.offers, Clock()))

    assert caught_up == ["m006", "m007", "m008", "m009", "m010", "m011"]
    assert poller.catch_up_remaining_ms() == 0
    assert await poller.catch_up_page(market.offers, Clock()) == []


@pytest.mark.asyncio
async def test_a_poll_that_fails_part_way_is_read_again_in_full():
    market = FakeMarket(_listings(2, T0_MS), page_size=2)
    poller = ListingPoller()
    await poller.live_poll(market.offers, Clock())

    # Six listings arrived; the first page of the next poll arrives, the second fails.
    market.add(*_listings(6, T0_MS + 6_000, prefix="m"))
    market.fail_after = 2
    with pytest.raises(DMarketAPIError):
        await poller.live_poll(market.offers, Clock())

    # Nothing moved on, so the resume point does not claim the listings that were never handed on.
    assert poller.newest_ms == T0_MS
    assert parse_resume_document(poller.resume_document()) == ResumePoint(newest_ms=T0_MS)

    market.fail_after = None
    read = await poller.live_poll(market.offers, Clock())
    assert _ids(read) == ["m005", "m004", "m003", "m002", "m001", "m000"]
    assert poller.newest_ms == T0_MS + 6_000


@pytest.mark.asyncio
async def test_catching_up_stops_at_the_lower_end_of_its_gap():
    # Older listings are still for sale below the gap; catching up must not read on into them.
    market = FakeMarket([*_listings(5, T0_MS), *_listings(4, T0_MS - 60_000, prefix="old")], page_size=2)
    poller = ListingPoller()
    poller.gaps.append(CatchUpGap(cursor=offers_cursor(T0_MS + 1_000), until_ms=T0_MS - 30_000))

    read = []
    while poller.gaps:
        read += _ids(await poller.catch_up_page(market.offers, Clock()))

    assert read == ["n000", "n001", "n002", "n003", "n004"]
    assert len(market.cursors) == 3


@pytest.mark.asyncio
async def test_an_empty_market_leaves_the_poller_where_it_was():
    poller = ListingPoller()

    assert await poller.live_poll(FakeMarket([]).offers, Clock()) == []
    assert poller.newest_ms is None and not poller.started
    assert json.loads(poller.resume_document())["newest_ms"] is None


@pytest.mark.asyncio
async def test_catching_up_hands_on_an_unparsable_entry_without_an_offer():
    market = FakeMarket(_listings(2, T0_MS), page_size=5)
    market.entries.insert(1, {"offerId": "broken"})
    poller = ListingPoller()
    poller.gaps.append(CatchUpGap(cursor=offers_cursor(T0_MS + 1_000), until_ms=T0_MS - 30_000))

    read = await poller.catch_up_page(market.offers, Clock())

    assert [offer.offer_id if offer else None for offer, _entry, _received in read] == ["n000", None, "n001"]


def test_the_listings_read_are_remembered_up_to_a_limit():
    poller = ListingPoller(max_seen=2)
    offers = [parse_offer(_entry(f"o{index}", T0_MS)) for index in range(3)]
    assert all(offer is not None and poller._take_unseen(offer) for offer in offers)

    # The oldest is forgotten, the newest two are still known.
    assert offers[0] is not None and poller._take_unseen(offers[0])
    assert offers[2] is not None and not poller._take_unseen(offers[2])


@pytest.mark.asyncio
async def test_unparsable_entries_are_handed_on_once_per_poll_without_an_offer():
    market = FakeMarket(_listings(2, T0_MS))
    market.entries.insert(1, {"offerId": "broken"})
    market.page_size = 5

    read = await ListingPoller().live_poll(market.offers, Clock())

    assert [offer.offer_id if offer else None for offer, _entry, _received in read] == [None, "n001", "n000"]


# --- Resume point ---


@pytest.mark.asyncio
async def test_resume_reads_back_to_two_minutes_before_the_resume_point_and_skips_recorded_listings():
    # The PC was off for 10 minutes: 6 listings 2 minutes apart were created in that time.
    downtime = _listings(6, T0_MS, prefix="d", step_ms=120_000)
    before = [_entry("recorded-before", T0_MS - 600_000 - 30_000), _entry("judged-before", T0_MS - 600_000 - 60_000)]
    older = [_entry("long-ago", T0_MS - 600_000 - RESUME_OVERLAP_MS - 1_000)]
    market = FakeMarket([*downtime, *before, *older], page_size=2)
    poller = ListingPoller()
    poller.resume_from(
        ResumePoint(newest_ms=T0_MS - 600_000, recorded_ids=frozenset({"recorded-before"})), T0_MS, 48 * 3_600_000
    )

    read = await poller.live_poll(market.offers, Clock())
    while poller.gaps:
        read += await poller.catch_up_page(market.offers, Clock())

    # Listings within the 2 minute overlap are read again unless they were recorded; older ones are not.
    assert sorted(_ids(read)) == sorted(["d000", "d001", "d002", "d003", "d004", "d005", "judged-before"])


@pytest.mark.asyncio
async def test_a_gap_left_after_a_resume_keeps_skipping_what_was_recorded_before_it(monkeypatch):
    monkeypatch.setattr(dmarket, "LIVE_MAX_PAGES", 1)
    market = FakeMarket(_listings(6, T0_MS, step_ms=60_000), page_size=2)
    poller = ListingPoller()
    poller.resume_from(ResumePoint(newest_ms=T0_MS - 300_000, recorded_ids=frozenset({"n005"})), T0_MS, 3_600_000)

    await poller.live_poll(market.offers, Clock())
    point = parse_resume_document(poller.resume_document())

    # Another restart now still skips the listing recorded before the first one.
    assert point is not None and len(point.gaps) == 1
    assert point.gaps[0].skip_ids == frozenset({"n005"})
    assert point.gaps[0].until_ms == T0_MS - 300_000 - RESUME_OVERLAP_MS


def test_catching_up_reaches_back_at_most_the_configured_hours():
    poller = ListingPoller()
    gap = CatchUpGap(cursor=offers_cursor(T0_MS - 3_600_000), until_ms=T0_MS - 90 * 3_600_000)
    poller.resume_from(ResumePoint(newest_ms=T0_MS - 72 * 3_600_000, gaps=[gap]), T0_MS, 48 * 3_600_000)

    assert poller._next_floor_ms == T0_MS - 48 * 3_600_000
    assert poller.gaps[0].until_ms == T0_MS - 48 * 3_600_000


def test_gaps_already_read_are_not_resumed():
    poller = ListingPoller()
    done = CatchUpGap(cursor=offers_cursor(T0_MS - 60_000), until_ms=T0_MS - 60_000)
    poller.resume_from(ResumePoint(newest_ms=T0_MS, gaps=[done]), T0_MS, 3_600_000)
    assert poller.gaps == []


@pytest.mark.asyncio
async def test_resume_document_round_trips_through_the_edge_redis():
    market = FakeMarket(_listings(3, T0_MS))
    poller = ListingPoller()
    read = await poller.live_poll(market.offers, Clock())
    for offer, _entry_payload, _received in read:
        assert offer is not None
        poller.note_recorded(offer)
    stale = parse_offer(_entry("stale", T0_MS - RESUME_OVERLAP_MS - 1))
    assert stale is not None
    poller.note_recorded(stale)
    poller.gaps.append(CatchUpGap(cursor=offers_cursor(T0_MS - 60_000), until_ms=T0_MS - 600_000, skip_ids=frozenset({"x"})))

    point = parse_resume_document(poller.resume_document())

    assert point is not None
    assert point.newest_ms == T0_MS
    # Only the recorded listings near the newest one are kept.
    assert point.recorded_ids == frozenset({"n000", "n001", "n002"})
    assert point.gaps == [CatchUpGap(cursor=offers_cursor(T0_MS - 60_000), until_ms=T0_MS - 600_000, skip_ids=frozenset({"x"}))]


@pytest.mark.parametrize(
    "text",
    [None, "not json", "[]", json.dumps({"version": 99, "newest_ms": 1}), json.dumps({"version": 1, "newest_ms": "x"})],
    ids=["missing", "not_json", "not_object", "other_version", "bad_time"],
)
def test_unreadable_resume_documents_are_ignored(text):
    assert parse_resume_document(text) is None


def test_unreadable_gaps_are_dropped_from_a_resume_document():
    document = json.dumps({"version": 1, "newest_ms": T0_MS, "recorded": {}, "gaps": [{"cursor": 5}, "x"]})
    point = parse_resume_document(document)
    assert point is not None and point.gaps == []


def test_a_gap_with_an_unreadable_cursor_has_nothing_left():
    assert CatchUpGap(cursor="?", until_ms=0).remaining_ms() == 0


# --- Scraper ---


class FakeCache:
    def __init__(self, hashes: dict[str, dict[str, str]] | None = None, fail: bool = False) -> None:
        self.hashes = hashes or {}
        self.values: dict[str, str] = {}
        self.fail = fail
        self.closed = False

    async def hgetall(self, key):
        self._check()
        return dict(self.hashes.get(key, {}))

    async def get(self, key):
        self._check()
        return self.values.get(key)

    async def set(self, key, value):
        self._check()
        self.values[key] = value

    async def aclose(self):
        self.closed = True

    def _check(self):
        if self.fail:
            from redis.exceptions import ConnectionError as RedisConnectionError

            raise RedisConnectionError("edge Redis is down")


def _dmarket_baselines(prices: dict[str, int], stickers: dict[str, int] | None = None) -> dict[str, dict[str, str]]:
    return {
        edge_baseline_meta_key("dmarket"): {"build_id": "16"},
        edge_baselines_key("dmarket"): {name: json.dumps({"latest_price_cents": price}) for name, price in prices.items()},
        edge_sticker_prices_key("dmarket"): {name: str(price) for name, price in (stickers or {}).items()},
    }


class FakeBackendResponse:
    def __init__(self, status: int, payload) -> None:
        self.status = status
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        return False

    def raise_for_status(self):
        if self.status >= 400:
            raise aiohttp.ClientError(f"HTTP {self.status}")

    async def json(self):
        return self.payload


class FakeSession:
    def __init__(self, backend: FakeBackendResponse | None = None) -> None:
        self.backend = backend or FakeBackendResponse(404, {"detail": "none"})
        self.requests: list[tuple[str, dict]] = []
        self.closed = False

    def get(self, url, params=None, headers=None):
        self.requests.append((url, params or {}))
        return self.backend

    async def close(self):
        self.closed = True


class StopPolling(Exception):
    pass


def _scraper(market, cache, session=None):
    """A scraper reading from the fake market, cache and backend, which stops when it would sleep."""

    async def stop(_seconds):
        raise StopPolling

    scraper = DMarketScraper(keys=KEYS, backend_base_url="http://backend:8080", sleep=stop)
    scraper._open_cache = lambda: cache
    scraper._open_session = lambda: session or FakeSession()
    scraper._client = lambda _session: market
    return scraper


async def _collect(scraper: DMarketScraper) -> tuple[list[MarketTick], list[FeedEvent], BaseException | None]:
    ticks: list[MarketTick] = []
    events: list[FeedEvent] = []
    ended: BaseException | None = None
    try:
        async for item in scraper.listen_websocket_stream():
            if isinstance(item, FeedEvent):
                events.append(item)
            else:
                ticks.append(item)
    except (FeedConnectionLost, StopPolling) as err:
        ended = err
    return ticks, events, ended


def _filtered(event: str, outcome: str) -> float:
    return feed_events_filtered_total.labels(event=event, outcome=outcome)._value.get()


@pytest.mark.asyncio
async def test_scraper_records_only_listings_below_the_dmarket_price_and_hands_on_their_raw_payloads(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 0)
    market = FakeMarket(
        [
            _entry("cheap", T0_MS, price_cents=900),
            _entry("dear", T0_MS - 1_000, price_cents=1_100),
            _entry("unknown", T0_MS - 2_000, title="Revolution Case"),
            _entry("p2p", T0_MS - 3_000, price_cents=900, offer_type="p2p"),
            _entry("stickered", T0_MS - 4_000, price_cents=1_100, stickers=("Crown (Foil)",)),
        ],
        page_size=10,
        fail_after=1,
    )
    cache = FakeCache(_dmarket_baselines({REDLINE: 1_000}, {"Crown (Foil)": 90_000}))
    before = {
        outcome: _filtered(EVENT_LIVE, outcome)
        for outcome in (OUTCOME_RECORDED, OUTCOME_ABOVE_RESALE_PRICE, OUTCOME_NO_BASELINE, OUTCOME_PERSON_TO_PERSON)
    }
    failed_before = dmarket_requests_total.labels(poll="live", outcome="failed")._value.get()

    ticks, events, ended = await _collect(_scraper(market, cache))

    assert [tick.listing_id for tick in ticks] == ["stickered", "cheap"]
    assert isinstance(ended, FeedConnectionLost) and "bad gateway" in str(ended)
    # The raw payloads still pending were handed on before the poll stopped.
    assert len(events) == 1
    assert [event["data"]["offerId"] for event in events[0].payload["events"]] == ["stickered", "cheap"]
    assert events[0].payload["venue"] == "dmarket"
    assert {event["event"] for event in events[0].payload["events"]} == {EVENT_LIVE}
    assert _filtered(EVENT_LIVE, OUTCOME_RECORDED) - before[OUTCOME_RECORDED] == 2
    assert _filtered(EVENT_LIVE, OUTCOME_ABOVE_RESALE_PRICE) - before[OUTCOME_ABOVE_RESALE_PRICE] == 1
    assert _filtered(EVENT_LIVE, OUTCOME_NO_BASELINE) - before[OUTCOME_NO_BASELINE] == 1
    assert _filtered(EVENT_LIVE, OUTCOME_PERSON_TO_PERSON) - before[OUTCOME_PERSON_TO_PERSON] == 1
    assert dmarket_requests_total.labels(poll="live", outcome="failed")._value.get() - failed_before == 1
    assert feed_connected._value.get() == 0
    assert cache.closed

    # The resume point names the newest listing read and the recorded ones near it.
    point = parse_resume_document(cache.values[RESUME_KEY])
    assert point is not None and point.newest_ms == T0_MS
    assert point.recorded_ids == frozenset({"cheap", "stickered"})


@pytest.mark.asyncio
async def test_scraper_counts_malformed_listings(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 0)
    market = FakeMarket([_entry("a", T0_MS)], page_size=10, fail_after=1)
    market.entries.append({"offerId": "broken"})
    before = _filtered(EVENT_LIVE, OUTCOME_MALFORMED)

    await _collect(_scraper(market, FakeCache(_dmarket_baselines({REDLINE: 2_000}))))

    assert _filtered(EVENT_LIVE, OUTCOME_MALFORMED) - before == 1


@pytest.mark.asyncio
async def test_scraper_resumes_from_the_edge_redis_and_catches_up_between_live_polls(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 3_600)
    monkeypatch.setattr(dmarket, "LIVE_MAX_PAGES", 1)
    now_ms = int(datetime.now(UTC).timestamp()) * 1000
    # The listener was restarted 5 minutes after it last read a listing; 9 cheap listings arrived since.
    resume_ms = now_ms - 300_000
    market = FakeMarket(
        [_entry(f"c{index}", now_ms - index * 30_000, price_cents=500) for index in range(9)]
        + [_entry("recorded-before", resume_ms - 20_000, price_cents=500)],
        page_size=2,
    )
    cache = FakeCache(_dmarket_baselines({REDLINE: 1_000}))
    cache.values[RESUME_KEY] = json.dumps(
        {"version": 1, "newest_ms": resume_ms, "recorded": {"recorded-before": resume_ms - 20_000}, "gaps": []}
    )
    session = FakeSession()

    ticks, _events, ended = await _collect(_scraper(market, cache, session))

    # The live poll read the newest page; catching up read the rest, down to 2 minutes before the resume point.
    assert isinstance(ended, StopPolling)
    assert sorted(tick.listing_id for tick in ticks) == sorted(f"c{index}" for index in range(9))
    assert session.requests == []  # The edge Redis had the resume point, so the backend was not asked.
    assert dmarket_catch_up_seconds._value.get() == 0
    assert 0 <= dmarket_newest_listing_age_seconds._value.get() < 60
    assert _filtered(EVENT_CATCH_UP, OUTCOME_RECORDED) >= 7


@pytest.mark.asyncio
async def test_scraper_asks_the_backend_when_the_edge_redis_is_empty(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 3_600)
    now_ms = int(datetime.now(UTC).timestamp()) * 1000
    market = FakeMarket(
        [_entry("new", now_ms, price_cents=500), _entry("recorded-before", now_ms - 60_000, price_cents=500)], page_size=10
    )
    session = FakeSession(
        FakeBackendResponse(200, {"venue": "dmarket", "observed_at_ms": now_ms - 30_000, "listing_ids": ["recorded-before"]})
    )

    ticks, _events, _ended = await _collect(_scraper(market, FakeCache(_dmarket_baselines({REDLINE: 1_000})), session))

    assert [tick.listing_id for tick in ticks] == ["new"]
    url, params = session.requests[0]
    assert url == "http://backend:8080/api/v1/listings/dmarket/newest"
    assert params == {"window_seconds": "120"}


@pytest.mark.asyncio
async def test_scraper_starts_from_the_newest_listings_without_any_resume_point(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 3_600)
    market = FakeMarket(_listings(5, T0_MS), page_size=2)
    cache = FakeCache(_dmarket_baselines({REDLINE: 1_000}))
    scraper = _scraper(market, cache, FakeSession(FakeBackendResponse(500, {})))

    ticks, _events, ended = await _collect(scraper)

    # Neither the edge Redis nor the backend knew where reading stopped; none of the listings is below the price.
    assert ticks == [] and isinstance(ended, StopPolling)
    assert market.cursors == [None]
    assert scraper.poller.started


@pytest.mark.asyncio
async def test_a_resume_point_the_edge_redis_cannot_store_is_only_logged():
    scraper = DMarketScraper(keys=KEYS, backend_base_url="http://backend:8080")
    scraper.poller.newest_ms = T0_MS
    cache = FakeCache(fail=True)

    await scraper._save_resume_point(cache)

    assert cache.values == {}


@pytest.mark.asyncio
async def test_nothing_is_read_while_the_edge_redis_holds_no_baselines(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 0)
    market = FakeMarket(_listings(5, T0_MS), page_size=2)
    sleeps: list[float] = []

    async def stop_on_second_wait(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 1:
            raise StopPolling

    scraper = _scraper(market, FakeCache(fail=True), FakeSession(FakeBackendResponse(500, {})))
    scraper._sleep = stop_on_second_wait

    ticks, _events, ended = await _collect(scraper)

    # Every listing read now would be dropped as having no baseline, so the poll waits instead of reading.
    assert ticks == [] and isinstance(ended, StopPolling)
    assert market.cursors == []
    assert sleeps[0] == pytest.approx(dmarket.RESALE_PRICES_RETRY_SECONDS, abs=1)


@pytest.mark.asyncio
async def test_catching_up_after_downtime_waits_for_the_baselines_to_load(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 3_600)
    monkeypatch.setattr(dmarket, "LIVE_MAX_PAGES", 1)
    monkeypatch.setattr(dmarket, "RESALE_PRICES_RETRY_SECONDS", 0)
    now_ms = int(datetime.now(UTC).timestamp()) * 1000
    # The PC was off: the edge Redis is empty, the backend knows where reading stopped, and the baseline loader
    # stores the DMarket build only after the listener has started.
    market = FakeMarket([_entry(f"c{index}", now_ms - index * 30_000, price_cents=500) for index in range(6)], page_size=2)
    session = FakeSession(FakeBackendResponse(200, {"observed_at_ms": now_ms - 200_000, "listing_ids": []}))
    cache = FakeCache()
    reads_while_waiting: list[int] = []
    waits = 0

    async def baselines_arrive(_seconds):
        nonlocal waits
        waits += 1
        if waits == 1:
            reads_while_waiting.append(len(market.cursors))
            cache.hashes = _dmarket_baselines({REDLINE: 1_000})
            return
        raise StopPolling

    scraper = _scraper(market, cache, session)
    scraper._sleep = baselines_arrive

    ticks, _events, _ended = await _collect(scraper)

    assert reads_while_waiting == [0]
    # Once the prices are there, the live poll and catching up record every cheap listing of the downtime.
    assert sorted(tick.listing_id for tick in ticks) == sorted(f"c{index}" for index in range(6))


@pytest.mark.asyncio
async def test_the_client_counts_429s_and_keeps_to_the_listener_share():
    scraper = DMarketScraper(keys=KEYS, backend_base_url="http://backend:8080")
    async with aiohttp.ClientSession() as session:
        client = scraper._client(session)
    before = dmarket_rate_limited_total._value.get()

    assert client._on_rate_limited is not None
    client._on_rate_limited()

    assert dmarket_rate_limited_total._value.get() - before == 1
    assert client._interval == pytest.approx(1 / dmarket.MAX_REQUESTS_PER_SECOND)


@pytest.mark.asyncio
async def test_a_reconnect_keeps_reading_where_the_poll_stopped(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 0)
    market = FakeMarket([_entry("a", T0_MS, price_cents=500)], page_size=10, fail_after=1)
    cache = FakeCache(_dmarket_baselines({REDLINE: 1_000}))
    session = FakeSession()
    scraper = _scraper(market, cache, session)
    await _collect(scraper)

    market.add(_entry("b", T0_MS + 1_000, price_cents=500))
    market.fail_after = 3
    ticks, _events, _ended = await _collect(scraper)

    # In memory state survives the reconnect: the resume point is not read again and "a" is not recorded twice.
    assert [tick.listing_id for tick in ticks] == ["b"]
    assert len(session.requests) == 1


@pytest.mark.asyncio
async def test_a_failure_before_anything_was_read_stores_no_resume_point(monkeypatch):
    market = FakeMarket(_listings(2, T0_MS), fail_after=0)
    cache = FakeCache(_dmarket_baselines({REDLINE: 1_000}))

    ticks, events, ended = await _collect(_scraper(market, cache))

    assert (ticks, events) == ([], [])
    assert isinstance(ended, FeedConnectionLost)
    assert RESUME_KEY not in cache.values


@pytest.mark.parametrize(
    "document",
    [{"observed_at_ms": "soon", "listing_ids": []}, {"observed_at_ms": T0_MS, "listing_ids": "a"}, ["not", "an", "object"]],
    ids=["time_not_a_number", "ids_not_a_list", "not_an_object"],
)
@pytest.mark.asyncio
async def test_an_unreadable_backend_answer_is_no_resume_point(document):
    scraper = DMarketScraper(keys=KEYS, backend_base_url="http://backend:8080")
    assert await scraper._backend_resume_point(FakeSession(FakeBackendResponse(200, document))) is None


@pytest.mark.asyncio
async def test_raw_listings_are_handed_on_while_polling_once_a_batch_is_due(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(dmarket, "RawEventBatch", lambda venue: RawEventBatch(venue, max_events=1))
    market = FakeMarket([_entry("a", T0_MS, price_cents=500)], page_size=10, fail_after=1)

    _ticks, events, _ended = await _collect(_scraper(market, FakeCache(_dmarket_baselines({REDLINE: 1_000}))))

    assert [event.payload["events"][0]["data"]["offerId"] for event in events] == ["a"]


@pytest.mark.asyncio
async def test_between_live_polls_the_scraper_sleeps_when_there_is_nothing_to_catch_up(monkeypatch):
    monkeypatch.setattr(dmarket, "POLL_INTERVAL_SECONDS", 3_600)
    sleeps: list[float] = []

    async def sleep_once(seconds):
        sleeps.append(seconds)
        if len(sleeps) > 1:
            raise StopPolling

    # An empty market: no listing read yet, so there is no newest listing age to report either.
    scraper = _scraper(FakeMarket([]), FakeCache(_dmarket_baselines({REDLINE: 1_000})))
    scraper._sleep = sleep_once

    await _collect(scraper)

    assert len(sleeps) == 2 and 0 < sleeps[0] <= 3_600


@pytest.mark.asyncio
async def test_polling_that_ends_without_an_error_still_reports_the_lost_connection():
    async def no_polls(_client, _cache, _raw_batch):
        return
        yield

    scraper = _scraper(FakeMarket([]), FakeCache())
    scraper._poll = no_polls

    _ticks, _events, ended = await _collect(scraper)

    assert isinstance(ended, FeedConnectionLost) and "polling stopped" in str(ended)


@pytest.mark.asyncio
async def test_the_scraper_has_no_separate_rest_poll_and_opens_its_own_session_and_cache():
    scraper = DMarketScraper(keys=KEYS, backend_base_url="http://backend:8080")

    assert [tick async for tick in scraper.poll_market_stream()] == []
    session = scraper._open_session()
    assert session.timeout.total == dmarket.REQUEST_TIMEOUT_SECONDS
    await session.close()
    cache = scraper._open_cache()
    await cache.aclose()
