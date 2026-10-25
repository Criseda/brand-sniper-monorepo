"""
DMarket listings for the listener (#339).

DMarket has no live feed, so the listener polls `GET /marketplace-api/v2/offers` for the newest bot held
listings every DMARKET_POLL_INTERVAL_SECONDS and reads every listing created since the previous poll. Bot held
listings arrive at 5 to 9 a second, far more than the database should hold, and DMarket publishes its own
sales, so a listing is judged against DMarket's own latest baseline price: only listings that could be bought
for less are recorded and scored (`ResalePrices.judge`, as for Waxpeer). Everything else is counted in
`listener_feed_events_filtered_total` and dropped.

The edge runs on a PC that is often off. While the listener runs it keeps a resume point in the edge Redis:
the newest listing time it has read, the listings it recorded near it, and any stretch of listings it has not
read back through yet. The edge Redis is empty after the PC was off, so the listener then asks the backend for
the newest DMarket listing it recorded. From the resume point it reads back through the listings created while
it was off, next to the live poll, by building a cursor at the time it has to start from. Measurements and
reasoning: docs/dmarket_api.md.
"""

import asyncio
import json
import os
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import aiohttp
from listener_telemetry import (
    dmarket_catch_up_seconds,
    dmarket_newest_listing_age_seconds,
    dmarket_rate_limited_total,
    dmarket_requests_total,
    export_dmarket_metrics,
    export_feed_metrics,
    feed_connected,
    feed_events_filtered_total,
    feed_reconnects_total,
    feed_resale_prices,
)
from models import LISTED_EVENT_TYPE, MAX_LISTING_ID_LENGTH, MAX_LISTING_URL_LENGTH, FeedEvent, MarketTick, TickKind
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from scrapers.base import BaseScraper, FeedConnectionLost, edge_redis_from_env
from scrapers.listing_filter import OUTCOME_MALFORMED, OUTCOME_RECORDED, RawEventBatch, ResalePrices
from shared_utils import backend_api_headers, get_logger, resale_venue_for
from shared_utils.dmarket import (
    OFFER_TYPE_BOT,
    DMarketAPIError,
    DMarketClient,
    DMarketKeys,
    DMarketOffer,
    OffersPage,
    cursor_created_at_ms,
    parse_offer,
)

logger = get_logger("listener.dmarket")

VENUE = "dmarket"
# DMarket has no page per listing. The market filtered to the item's exact name lists it with the item's other
# listings, the way a Skinport tick links to the item page (checked on dmarket.com on 2026-10-25).
MARKET_SEARCH_URL = "https://dmarket.com/ingame-items/item-list/csgo-skins?title="

POLL_INTERVAL_SECONDS = float(os.getenv("DMARKET_POLL_INTERVAL_SECONDS", "1.5"))
# The listener's share of the account's 30 requests a second; the baseline builder takes another 10.
MAX_REQUESTS_PER_SECOND = float(os.getenv("DMARKET_LISTENER_MAX_REQUESTS_PER_SECOND", "10"))
# The furthest back catching up reads after downtime. Listings older than this that are still for sale are
# unlikely to be underpriced.
MAX_CATCH_UP_HOURS = float(os.getenv("DMARKET_MAX_CATCH_UP_HOURS", "48"))

# A live poll reads this far below the newest listing it read before, in case a listing shows up late (none did
# in 80 polls on 2026-10-25). The first page reaches 12 to 35 seconds back, so one page usually covers the poll.
LIVE_OVERLAP_MS = 5_000
# A live poll reads at most this many pages; anything older is left to catching up, so a poll never waits on
# a long read back.
LIVE_MAX_PAGES = 10
# After a restart, reading starts this far below the resume point. It covers clock drift between the PC and
# DMarket, and the backend's resume point, which is a receive time rather than a creation time.
RESUME_OVERLAP_MS = 120_000
# Listings read recently, so one seen again by the overlap or by catching up is not judged twice.
SEEN_LISTINGS_MAX = 200_000
RESUME_KEY = "dmarket:resume"
RESUME_VERSION = 1

# `event` label of listener_feed_events_filtered_total: a listing read by the live poll, or while catching up.
EVENT_LIVE = "listed"
EVENT_CATCH_UP = "catch_up"
# A person to person listing; the API cannot buy them (docs/dmarket_api.md), so they are not recorded.
OUTCOME_PERSON_TO_PERSON = "person_to_person"

# How often the scraper checks the edge Redis for a newer DMarket baseline build, and how often while it holds
# none yet (the baseline loader fills the edge Redis as the listener starts).
RESALE_PRICES_REFRESH_SECONDS = 60
RESALE_PRICES_RETRY_SECONDS = 5
REQUEST_TIMEOUT_SECONDS = 15

# Fields of a listing kept in its raw feed_events payload. The rest is images, inspect links, and the
# account and Steam asset IDs of DMarket's bots, which describe nothing about the listing and triple its size.
RAW_LISTING_FIELDS = ("offerId", "createdAt", "priceCents", "offerType", "discountPercent", "locked")
RAW_ATTRIBUTE_FIELDS = (
    "id",
    "title",
    "tradable",
    "tradeLockDays",
    "unlockDate",
    "settlementTime",
    "revertPendingUntil",
    "tradeProtectionRemoved",
    "status",
)
RAW_CS2_DROPPED_FIELDS = ("inspectInGameUri", "inspectInGameTemplateUri")


def listing_url(title: str) -> str | None:
    """Link to DMarket's market filtered to the item; None when too long to store."""
    url = MARKET_SEARCH_URL + quote(title, safe="")
    return url if len(url) <= MAX_LISTING_URL_LENGTH else None


def listing_tick(offer: DMarketOffer, received_at_ms: int) -> MarketTick | None:
    """A listed tick for a DMarket listing, observed when its page arrived; None when it does not validate."""
    if len(offer.offer_id) > MAX_LISTING_ID_LENGTH:
        return None
    try:
        return MarketTick(
            venue=VENUE,
            kind=TickKind.LISTED,
            market_hash_name=offer.market_hash_name,
            price_usd=offer.price_cents / 100,
            timestamp=received_at_ms // 1000,
            observed_at_ms=received_at_ms,
            float_value=offer.float_value,
            stickers=[{"name": name} for name in offer.sticker_names],
            event_type=LISTED_EVENT_TYPE,
            listing_id=offer.offer_id,
            pattern=offer.paint_seed,
            paint_index=offer.paint_index,
            listing_url=listing_url(offer.title),
        )
    except (ValidationError, ValueError, TypeError) as err:
        logger.warning("[DMARKET] Skipping malformed listing '%s': %s", offer.market_hash_name, err)
        return None


def raw_listing(entry: dict[str, Any]) -> dict[str, Any]:
    """The listing as recorded in feed_events: the fields that describe it (RAW_LISTING_FIELDS)."""
    raw = {name: entry[name] for name in RAW_LISTING_FIELDS if name in entry}
    attributes = entry.get("attributes")
    if isinstance(attributes, dict):
        kept = {name: attributes[name] for name in RAW_ATTRIBUTE_FIELDS if name in attributes}
        cs2 = attributes.get("cs2")
        if isinstance(cs2, dict):
            kept["cs2"] = {name: value for name, value in cs2.items() if name not in RAW_CS2_DROPPED_FIELDS}
        raw["attributes"] = kept
    return raw


@dataclass(slots=True)
class CatchUpGap:
    """
    Listings not read yet: those from `cursor` down to `until_ms` (creation times in Unix milliseconds).

    `skip_ids` are listings recorded before the restart near `until_ms`, where reading overlaps what was read
    before; they are not recorded again.
    """

    cursor: str
    until_ms: int
    skip_ids: frozenset[str] = frozenset()

    def remaining_ms(self) -> int:
        position = cursor_created_at_ms(self.cursor)
        return max(0, position - self.until_ms) if position is not None else 0

    def to_json(self) -> dict[str, Any]:
        return {"cursor": self.cursor, "until_ms": self.until_ms, "skip_ids": sorted(self.skip_ids)}

    @classmethod
    def from_json(cls, document: Any) -> "CatchUpGap | None":
        if not isinstance(document, dict):
            return None
        cursor, until_ms, skip_ids = document.get("cursor"), document.get("until_ms"), document.get("skip_ids", [])
        if not isinstance(cursor, str) or not isinstance(until_ms, int) or not isinstance(skip_ids, list):
            return None
        return cls(cursor=cursor, until_ms=until_ms, skip_ids=frozenset(str(listing_id) for listing_id in skip_ids))


@dataclass(slots=True)
class ResumePoint:
    """
    Where reading stopped: the newest listing time read, and the listings recorded near it.

    From the edge Redis it is the newest creation time the live poll read, plus the stretches it had not read
    back through yet. From the backend it is the receive time of the newest recorded listing, which is never
    earlier than its creation time.
    """

    newest_ms: int
    recorded_ids: frozenset[str] = frozenset()
    gaps: list[CatchUpGap] = field(default_factory=list)


def parse_resume_document(text: str | bytes | None) -> ResumePoint | None:
    """The resume point stored in the edge Redis; None when there is none or it is unreadable."""
    if text is None:
        return None
    try:
        document = json.loads(text)
    except ValueError:
        return None
    if not isinstance(document, dict) or document.get("version") != RESUME_VERSION:
        return None
    newest_ms = document.get("newest_ms")
    recorded = document.get("recorded", {})
    gaps = document.get("gaps", [])
    if not isinstance(newest_ms, int) or not isinstance(recorded, dict) or not isinstance(gaps, list):
        return None
    restored = [gap for gap in (CatchUpGap.from_json(entry) for entry in gaps) if gap is not None]
    return ResumePoint(newest_ms=newest_ms, recorded_ids=frozenset(str(key) for key in recorded), gaps=restored)


class ListingPoller:
    """
    Reads DMarket listings newest first: a live poll of the newest pages, and pages back through any stretch
    of listings not read yet (`gaps`). Each listing is read once, so each is judged once.

    It only reads and remembers; the scraper judges what it returns and keeps the resume point.
    """

    def __init__(self, max_seen: int = SEEN_LISTINGS_MAX) -> None:
        self.newest_ms: int | None = None  # Newest creation time the live poll has read
        self.gaps: list[CatchUpGap] = []
        self._next_floor_ms: int | None = None  # The live poll reads down to this time
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._max_seen = max_seen
        # Listings recorded near the newest one, by creation time, kept for the resume point.
        self._recorded: dict[str, int] = {}

    @property
    def started(self) -> bool:
        return self._next_floor_ms is not None or self.newest_ms is not None

    def resume_from(self, point: ResumePoint, now_ms: int, max_catch_up_ms: int) -> None:
        """Start reading where an earlier run stopped, at most `max_catch_up_ms` back."""
        oldest_allowed_ms = now_ms - max_catch_up_ms
        self._next_floor_ms = max(point.newest_ms - RESUME_OVERLAP_MS, oldest_allowed_ms)
        for listing_id in point.recorded_ids:
            self._remember(listing_id)
            # Kept with the resume time, so a gap the first poll leaves carries them past another restart.
            self._recorded[listing_id] = point.newest_ms
        for gap in point.gaps:
            if gap.until_ms < oldest_allowed_ms:
                gap = CatchUpGap(cursor=gap.cursor, until_ms=oldest_allowed_ms, skip_ids=gap.skip_ids)
            if gap.remaining_ms() > 0:
                self.gaps.append(gap)

    def _remember(self, listing_id: str) -> None:
        self._seen[listing_id] = None
        self._seen.move_to_end(listing_id)
        while len(self._seen) > self._max_seen:
            self._seen.popitem(last=False)

    def _take_unseen(self, offer: DMarketOffer, skip_ids: frozenset[str] = frozenset()) -> bool:
        """True the first time a listing is read; it is remembered from then on."""
        if offer.offer_id in self._seen or offer.offer_id in skip_ids:
            return False
        self._remember(offer.offer_id)
        return True

    def note_recorded(self, offer: DMarketOffer) -> None:
        """A listing that was recorded. Those near the newest listing go into the resume point."""
        self._recorded[offer.offer_id] = offer.created_at_ms

    async def live_poll(
        self, fetch: Callable[[str | None], Awaitable[OffersPage]], clock_ms: Callable[[], int]
    ) -> list[tuple[DMarketOffer | None, dict[str, Any], int]]:
        """
        Reads the newest listings down to the previous poll's newest one, less LIVE_OVERLAP_MS, and returns
        those not read before, oldest first, each with its raw entry and the time its page arrived. An entry
        that does not parse is returned with no offer. Without a floor (nothing read yet, no resume point)
        only the first page is read.
        """
        floor_ms = self._next_floor_ms
        read: list[tuple[DMarketOffer | None, dict[str, Any], int]] = []
        cursor: str | None = None
        for _page in range(LIVE_MAX_PAGES):
            page = await fetch(cursor)
            received_at_ms = clock_ms()
            oldest_ms: int | None = None
            for entry in page.entries:
                offer = parse_offer(entry)
                if offer is not None:
                    oldest_ms = offer.created_at_ms if oldest_ms is None else min(oldest_ms, offer.created_at_ms)
                    if self.newest_ms is None or offer.created_at_ms > self.newest_ms:
                        self.newest_ms = offer.created_at_ms
                    if floor_ms is not None and offer.created_at_ms < floor_ms:
                        continue
                    if not self._take_unseen(offer):
                        continue
                read.append((offer, entry if isinstance(entry, dict) else {}, received_at_ms))
            if floor_ms is None or page.cursor is None or oldest_ms is None or oldest_ms < floor_ms:
                break
            cursor = page.cursor
        else:
            # The poll fell behind (after a reconnect): catching up reads from the last page to the floor. Every
            # page that did not end the loop had a floor and a cursor.
            assert cursor is not None and floor_ms is not None
            skip_ids = frozenset(listing_id for listing_id, created in self._recorded.items() if created >= floor_ms)
            self.gaps.append(CatchUpGap(cursor=cursor, until_ms=floor_ms, skip_ids=skip_ids))

        if self.newest_ms is not None:
            self._next_floor_ms = self.newest_ms - LIVE_OVERLAP_MS
        read.sort(key=lambda item: item[0].created_at_ms if item[0] is not None else 0)
        return read

    async def catch_up_page(
        self, fetch: Callable[[str | None], Awaitable[OffersPage]], clock_ms: Callable[[], int]
    ) -> list[tuple[DMarketOffer | None, dict[str, Any], int]]:
        """Reads the next page of the oldest open gap and returns the listings on it not read before."""
        if not self.gaps:
            return []
        gap = self.gaps[0]
        page = await fetch(gap.cursor)
        received_at_ms = clock_ms()
        read: list[tuple[DMarketOffer | None, dict[str, Any], int]] = []
        reached_end = page.cursor is None or not page.entries
        for entry in page.entries:
            offer = parse_offer(entry)
            if offer is not None:
                if offer.created_at_ms < gap.until_ms:
                    reached_end = True
                    continue
                if not self._take_unseen(offer, gap.skip_ids):
                    continue
            read.append((offer, entry if isinstance(entry, dict) else {}, received_at_ms))
        if reached_end or page.cursor is None:
            self.gaps.pop(0)
        else:
            gap.cursor = page.cursor
        return read

    def catch_up_remaining_ms(self) -> int:
        return sum(gap.remaining_ms() for gap in self.gaps)

    def resume_document(self) -> str:
        """The resume point for the edge Redis: the newest time read, the recorded listings near it, and the gaps."""
        if self.newest_ms is not None:
            oldest_kept_ms = self.newest_ms - RESUME_OVERLAP_MS
            self._recorded = {
                listing_id: created for listing_id, created in self._recorded.items() if created >= oldest_kept_ms
            }
        return json.dumps(
            {
                "version": RESUME_VERSION,
                "newest_ms": self.newest_ms,
                "recorded": self._recorded,
                "gaps": [gap.to_json() for gap in self.gaps],
            },
            separators=(",", ":"),
        )


class DMarketScraper(BaseScraper):
    """
    Records DMarket bot held listings by polling the Trading API (https://docs.dmarket.com/v1/swagger.html).
    Observed behaviour and measurements: docs/dmarket_api.md.
    """

    polls_rest = False

    def __init__(
        self,
        keys: DMarketKeys | None = None,
        backend_base_url: str | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        super().__init__(venue=VENUE)
        self._sleep = sleep
        export_feed_metrics()
        export_dmarket_metrics()
        # Fails at startup, not on the first poll, when the keys are missing.
        self._keys = keys or DMarketKeys.from_env()
        # The backend address, built from the environment as main.py builds it.
        self._backend_base_url = backend_base_url or (
            f"http://{os.getenv('COMPUTE_NODE_IP', 'localhost')}:{os.getenv('COMPUTE_NODE_PORT', '8080')}"
        )
        self.resale_prices = ResalePrices(resale_venue_for(VENUE))
        self.poller = ListingPoller()

    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """The listing poll runs as the live feed (`listen_websocket_stream`); there is no separate REST poll."""
        return
        yield  # pragma: no cover

    def _open_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS))

    def _open_cache(self) -> Redis:
        return edge_redis_from_env()

    def _client(self, session: aiohttp.ClientSession) -> DMarketClient:
        return DMarketClient(self._keys, session, MAX_REQUESTS_PER_SECOND, on_rate_limited=dmarket_rate_limited_total.inc)

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        """
        Polls DMarket's listings and yields a MarketTick per recorded listing plus a FeedEvent per group of raw
        listings. Raises FeedConnectionLost when a request fails, after handing on the raw listings still
        pending; the listener's producer loop starts it again, and reading resumes where it stopped.
        """
        cache = self._open_cache()
        session = self._open_session()
        client = self._client(session)
        raw_batch = RawEventBatch(VENUE)
        try:
            try:
                if not self.poller.started:
                    await self._resume(cache, session)
                async for item in self._poll(client, cache, raw_batch):
                    yield item
                reason = "polling stopped"
            except (DMarketAPIError, aiohttp.ClientError, TimeoutError) as err:
                reason = str(err) or type(err).__name__
            pending = raw_batch.take()
            if pending is not None:
                yield pending
            await self._save_resume_point(cache)
            raise FeedConnectionLost(f"DMarket polling stopped: {reason}")
        finally:
            feed_connected.set(0)
            feed_reconnects_total.inc()
            await session.close()
            await cache.aclose()

    async def _resume(self, cache: Redis, session: aiohttp.ClientSession) -> None:
        """Finds where an earlier run stopped: the edge Redis first, else the backend."""
        point: ResumePoint | None = None
        source = "edge Redis"
        try:
            point = parse_resume_document(await cache.get(RESUME_KEY))
        except RedisError as err:
            logger.warning("[DMARKET] Could not read the resume point from the edge Redis: %s", err)
        if point is None:
            source = "backend"
            point = await self._backend_resume_point(session)
        if point is None:
            logger.info("[DMARKET] No resume point; reading from the newest listings on.")
            return
        now_ms = int(time.time() * 1000)
        self.poller.resume_from(point, now_ms, int(MAX_CATCH_UP_HOURS * 3_600_000))
        logger.info(
            "[DMARKET] Resuming from the %s: newest listing read %.0f minutes ago, %d recorded listings near it, %d gaps.",
            source,
            (now_ms - point.newest_ms) / 60_000,
            len(point.recorded_ids),
            len(point.gaps),
        )

    async def _backend_resume_point(self, session: aiohttp.ClientSession) -> ResumePoint | None:
        """The newest DMarket listing the backend recorded, and the listings recorded just before it."""
        url = f"{self._backend_base_url}/api/v1/listings/{VENUE}/newest"
        params = {"window_seconds": str(RESUME_OVERLAP_MS // 1000)}
        try:
            async with session.get(url, params=params, headers=backend_api_headers()) as response:
                if response.status == 404:
                    return None
                response.raise_for_status()
                document = await response.json()
        except (aiohttp.ClientError, TimeoutError, ValueError) as err:
            logger.warning("[DMARKET] Could not ask the backend where reading stopped: %s", err)
            return None
        observed_at_ms = document.get("observed_at_ms") if isinstance(document, dict) else None
        listing_ids = document.get("listing_ids") if isinstance(document, dict) else None
        if not isinstance(observed_at_ms, int) or not isinstance(listing_ids, list):
            return None
        return ResumePoint(newest_ms=observed_at_ms, recorded_ids=frozenset(str(listing_id) for listing_id in listing_ids))

    async def _save_resume_point(self, cache: Redis) -> None:
        if self.poller.newest_ms is None:
            return
        try:
            await cache.set(RESUME_KEY, self.poller.resume_document())
        except RedisError as err:
            logger.warning("[DMARKET] Could not store the resume point in the edge Redis: %s", err)

    async def _refresh_resale_prices(self, cache: Redis) -> None:
        try:
            loaded = await self.resale_prices.refresh(cache)
        except RedisError as err:
            logger.warning("[DMARKET] Could not read %s baselines from the edge Redis: %s", self.resale_prices.venue, err)
            return
        if loaded:
            logger.info(
                "[DMARKET] Judging listings against %s build %s: %d item prices, %d sticker prices.",
                self.resale_prices.venue,
                self.resale_prices.build_id,
                len(self.resale_prices.latest_price_cents),
                len(self.resale_prices.sticker_prices_cents),
            )
        feed_resale_prices.set(len(self.resale_prices.latest_price_cents))

    async def _poll(
        self, client: DMarketClient, cache: Redis, raw_batch: RawEventBatch
    ) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        """Live polls every POLL_INTERVAL_SECONDS, and pages of catching up in between, for as long as requests succeed."""

        def fetcher(poll: str) -> Callable[[str | None], Awaitable[OffersPage]]:
            async def fetch(cursor: str | None) -> OffersPage:
                try:
                    page = await client.offers(cursor)
                except Exception:
                    dmarket_requests_total.labels(poll=poll, outcome="failed").inc()
                    raise
                dmarket_requests_total.labels(poll=poll, outcome="ok").inc()
                return page

            return fetch

        def clock_ms() -> int:
            return int(time.time() * 1000)

        fetch_live, fetch_catch_up = fetcher("live"), fetcher(EVENT_CATCH_UP)
        next_live_at = 0.0
        next_refresh_at = 0.0
        while True:
            now = time.monotonic()
            if now >= next_refresh_at:
                await self._refresh_resale_prices(cache)
                loaded = bool(self.resale_prices.latest_price_cents)
                next_refresh_at = now + (RESALE_PRICES_REFRESH_SECONDS if loaded else RESALE_PRICES_RETRY_SECONDS)

            if now >= next_live_at:
                next_live_at = now + POLL_INTERVAL_SECONDS
                read = await self.poller.live_poll(fetch_live, clock_ms)
                event = EVENT_LIVE
                feed_connected.set(1)
            elif self.poller.gaps:
                read = await self.poller.catch_up_page(fetch_catch_up, clock_ms)
                event = EVENT_CATCH_UP
            else:
                await self._sleep(next_live_at - now)
                continue

            for offer, entry, received_at_ms in read:
                tick = self._judge(offer, received_at_ms, event)
                if tick is not None and offer is not None:
                    self.poller.note_recorded(offer)
                    raw_batch.add(event, raw_listing(entry), received_at_ms)
                    yield tick

            now_ms = clock_ms()
            due = raw_batch.take_if_due(now_ms)
            if due is not None:
                yield due
            if self.poller.newest_ms is not None:
                dmarket_newest_listing_age_seconds.set(max(0, now_ms - self.poller.newest_ms) / 1000)
            dmarket_catch_up_seconds.set(self.poller.catch_up_remaining_ms() / 1000)
            await self._save_resume_point(cache)

    def _judge(self, offer: DMarketOffer | None, received_at_ms: int, event: str) -> MarketTick | None:
        """The tick to record for a listing read, or None; counts what the filter did with it."""
        tick: MarketTick | None = None
        if offer is None:
            outcome = OUTCOME_MALFORMED
        elif offer.offer_type != OFFER_TYPE_BOT:
            outcome = OUTCOME_PERSON_TO_PERSON
        else:
            tick = listing_tick(offer, received_at_ms)
            outcome = OUTCOME_MALFORMED if tick is None else self.resale_prices.judge(tick)
        feed_events_filtered_total.labels(event=event, outcome=outcome).inc()
        return tick if outcome == OUTCOME_RECORDED else None
