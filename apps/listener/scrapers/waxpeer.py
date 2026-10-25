"""
Waxpeer live listing feed for the listener (#261).

Waxpeer publishes every listing change on a public Socket.IO feed (https://docs.waxpeer.com/socketio). The
listener speaks the protocol itself over an aiohttp websocket: Engine.IO v4 frames with Socket.IO packets
inside them. No Node.js sidecar and no browser cookies are needed.

The feed sends about 430 events a second, mostly repricing bots moving prices up and down, which is far more
than the database can hold. Waxpeer also publishes no sales, so its listings are judged against the resale
venue (Skinport, see `shared_utils.resale_venue_for`). The scraper therefore records and scores only listings
that could be bought for less than the item's resale price: a new listing below it, or a price cut that lands
below it. It then follows those listings, so their later cuts and their removal are recorded too. Everything
else is counted in `listener_feed_events_filtered_total` and dropped. Measurements and reasoning:
docs/waxpeer_feed.md.
"""

import json
import os
import re
import time
from collections import OrderedDict
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any

import aiohttp
from listener_telemetry import (
    export_feed_metrics,
    feed_connected,
    feed_events_filtered_total,
    feed_reconnects_total,
    feed_resale_prices,
    feed_tracked_listings,
)
from models import LISTED_EVENT_TYPE, MAX_LISTING_URL_LENGTH, FeedEvent, MarketTick, TickKind
from pydantic import ValidationError
from redis.asyncio import Redis
from redis.exceptions import RedisError
from scrapers.base import (
    BaseScraper,
    FeedConnectionLost,
    edge_redis_from_env,
    optional_non_negative_int,
    optional_wear,
)
from scrapers.listing_filter import (
    OUTCOME_ABOVE_RESALE_PRICE,
    OUTCOME_MALFORMED,
    OUTCOME_NO_BASELINE,
    OUTCOME_RECORDED,
    RawEventBatch,
    ResalePrices,
)
from shared_utils import applied_sticker_name, build_versioned_name, get_logger, resale_venue_for

logger = get_logger("listener.waxpeer")

VENUE = "waxpeer"
SOCKET_URL = "wss://waxpeer.com/socket.io/?EIO=4&transport=websocket"
GAME_CHANNEL = "csgo"
# A listing's page is `<WEBSITE_URL>/<slug of the item name>/item?id=<item_id>`. The ID picks the listing; the
# slug only makes the link readable (checked on waxpeer.com on 2026-10-21).
WEBSITE_URL = "https://waxpeer.com"
API_KEY_ENV = "WAXPEER_API_KEY"

# Waxpeer prices are integer thousandths of a dollar (1000 = $1).
MILLS_PER_CENT = 10

# Feed events. `new` and `update` carry a listing that can be bought now; `removed` means the listing left
# the market, whether it sold, was delisted, or its seller went offline (docs/waxpeer_feed.md).
NEW_EVENT = "new"
UPDATE_EVENT = "update"
REMOVED_EVENT = "removed"
REMOVED_EVENT_TYPE = "removed"

# What the recording filter did with a feed event (the `outcome` label of listener_feed_events_filtered_total):
# the outcomes of scrapers/listing_filter.py, plus two only a feed that reports price changes and removals has.
OUTCOME_NOT_A_PRICE_CUT = "not_a_price_cut"  # A followed listing whose price rose or stayed the same
OUTCOME_UNTRACKED_REMOVAL = "untracked_removal"  # Removal of a listing that was never recorded
FEED_OUTCOMES = (
    OUTCOME_RECORDED,
    OUTCOME_NO_BASELINE,
    OUTCOME_ABOVE_RESALE_PRICE,
    OUTCOME_NOT_A_PRICE_CUT,
    OUTCOME_UNTRACKED_REMOVAL,
    OUTCOME_MALFORMED,
)

# Recorded listings the filter follows for later price cuts and removal (LRU). The feed records roughly
# 11 listings a second, so this covers several hours; an evicted listing is simply judged afresh.
TRACKED_LISTINGS_MAX = int(os.getenv("WAXPEER_TRACKED_LISTINGS_MAX", "200000"))

# How often the scraper checks the edge Redis for a newer resale venue baseline build, and how often while it
# holds none yet (the baseline loader fills the edge Redis while the feed connects).
RESALE_PRICES_REFRESH_SECONDS = 60
RESALE_PRICES_RETRY_SECONDS = 5
# Receive timeout before the server's open packet states its ping interval and timeout.
DEFAULT_RECEIVE_TIMEOUT_SECONDS = 60

# Engine.IO v4 packet types, and the Socket.IO packets carried in Engine.IO messages ("4" + Socket.IO type).
ENGINE_OPEN = "0"
ENGINE_CLOSE = "1"
ENGINE_PING = "2"
ENGINE_PONG = "3"
SOCKET_CONNECT = "40"
SOCKET_DISCONNECT = "41"
SOCKET_EVENT = "42"
SOCKET_CONNECT_ERROR = "44"
SUBSCRIBED_EVENT = "subscribed"


def mills_to_cents(mills: object) -> int | None:
    """A Waxpeer price in whole cents, rounded up so a buy price is never understated; None when unusable."""
    if isinstance(mills, str) and mills.isdigit():
        mills = int(mills)
    if isinstance(mills, bool) or not isinstance(mills, int) or mills <= 0:
        return None
    return -(-mills // MILLS_PER_CENT)


def steam_market_hash_name(name: str, phase: str | None) -> str:
    """
    The Steam market hash name of a Waxpeer listing.

    Waxpeer writes a Doppler phase into the name ("Doppler Phase 3 (Factory New)"), where Steam's name has
    none ("Doppler (Factory New)"). The phase is put back by `build_versioned_name`, as for Skinport.
    """
    if not phase:
        return name
    inline = f" {phase} ("
    if inline in name:
        return name.replace(inline, " (", 1)
    suffix = f" {phase}"
    return name.removesuffix(suffix)


def listing_url(name: str, item_id: object) -> str | None:
    """Link that opens the listing on waxpeer.com, built from its Waxpeer name and item ID; None when too long."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    url = f"{WEBSITE_URL}/{slug}/item?id={item_id}"
    # A truncated URL would be a broken link; drop it instead.
    return url if len(url) <= MAX_LISTING_URL_LENGTH else None


def waxpeer_stickers(sticker_names: object) -> list[dict[str, str]]:
    """
    Applied stickers as the listener's tick stickers.

    Waxpeer names each applied sticker by its item name ("Sticker | Crown (Foil)"). A Skinport listing names it
    without the prefix, and sticker prices are keyed that way, so the prefix is dropped. Charms and patches
    keep their full name; they have no sticker price.
    """
    if not isinstance(sticker_names, list):
        return []
    stickers = []
    for sticker_name in sticker_names:
        if isinstance(sticker_name, str) and sticker_name:
            stickers.append({"name": applied_sticker_name(sticker_name) or sticker_name})
    return stickers


def listing_tick(payload: object, received_at_ms: int) -> MarketTick | None:
    """A listed tick from a `new` or `update` payload; None when it lacks a name, a listing ID, or a valid price."""
    if not isinstance(payload, dict):
        return None
    name = payload.get("name")
    item_id = payload.get("item_id")
    price_cents = mills_to_cents(payload.get("price"))
    if not isinstance(name, str) or not name or item_id is None or price_cents is None:
        return None
    phase = payload.get("phase") or None
    phase = phase if isinstance(phase, str) else None
    try:
        return MarketTick(
            venue=VENUE,
            kind=TickKind.LISTED,
            market_hash_name=build_versioned_name(steam_market_hash_name(name, phase), phase),
            price_usd=price_cents / 100,
            timestamp=received_at_ms // 1000,
            observed_at_ms=received_at_ms,
            float_value=optional_wear(payload.get("float")),
            stickers=waxpeer_stickers(payload.get("sticker_names")),
            event_type=LISTED_EVENT_TYPE,
            listing_id=str(item_id),
            # The feed sends the finish (`paint_index`) but no paint seed.
            paint_index=optional_non_negative_int(payload.get("paint_index")),
            listing_url=listing_url(name, item_id),
        )
    except (ValidationError, ValueError, TypeError) as err:
        logger.warning("[WAXPEER] Skipping malformed listing '%s': %s", name, err)
        return None


def socketio_event(frame: str) -> tuple[str, Any] | None:
    """The name and first argument of a Socket.IO event frame (`42["new", {...}]`); None for anything else."""
    if not frame.startswith(SOCKET_EVENT):
        return None
    packet = json.loads(frame[len(SOCKET_EVENT) :])
    if not isinstance(packet, list) or not packet or not isinstance(packet[0], str):
        return None
    return packet[0], packet[1] if len(packet) > 1 else None


def subscribe_frame(channel: str) -> str:
    return SOCKET_EVENT + json.dumps(["sub", {"name": channel}], separators=(",", ":"))


@dataclass(frozen=True, slots=True)
class TrackedListing:
    """A recorded listing the filter follows: its item and its latest price on the feed."""

    market_hash_name: str
    price_cents: int


class RecordingFilter:
    """
    Decides which feed events become ticks.

    - `new`, or an `update` of a listing the filter does not follow: recorded when the resale venue judges it
      worth recording (`ResalePrices.judge`), and followed from then on.
    - `update` of a followed listing: recorded only when it cuts the price and the new price is still worth
      recording. A rise is not a new opportunity.
    - `removed` of a followed listing: recorded (it ends the listing's time on the market) and unfollowed.
      Other removals are dropped.
    """

    def __init__(self, resale_prices: ResalePrices, max_tracked: int = TRACKED_LISTINGS_MAX) -> None:
        self.resale_prices = resale_prices
        self.max_tracked = max_tracked
        self._tracked: OrderedDict[str, TrackedListing] = OrderedDict()

    @property
    def tracked_count(self) -> int:
        return len(self._tracked)

    def _track(self, listing_id: str, tick: MarketTick) -> None:
        self._tracked[listing_id] = TrackedListing(tick.market_hash_name, tick.price_cents)
        self._tracked.move_to_end(listing_id)
        while len(self._tracked) > self.max_tracked:
            self._tracked.popitem(last=False)

    def listing(self, event: str, payload: object, received_at_ms: int) -> tuple[str, MarketTick | None]:
        tick = listing_tick(payload, received_at_ms)
        if tick is None or tick.listing_id is None:
            return OUTCOME_MALFORMED, None
        listing_id = tick.listing_id
        followed = self._tracked.get(listing_id)
        if followed is not None and event == UPDATE_EVENT:
            # Follow the price either way, so the next update is compared with the latest one.
            self._track(listing_id, tick)
            if tick.price_cents >= followed.price_cents:
                return OUTCOME_NOT_A_PRICE_CUT, None
            outcome = self.resale_prices.judge(tick)
            return outcome, tick if outcome == OUTCOME_RECORDED else None

        # A new listing, a relisting under the same ID, or an update of a listing seen for the first time.
        outcome = self.resale_prices.judge(tick)
        if outcome != OUTCOME_RECORDED:
            # A relisting that no longer qualifies is not followed any more.
            self._tracked.pop(listing_id, None)
            return outcome, None
        self._track(listing_id, tick)
        return outcome, tick

    def removal(self, payload: object, received_at_ms: int) -> tuple[str, MarketTick | None]:
        if not isinstance(payload, dict) or payload.get("item_id") is None:
            return OUTCOME_MALFORMED, None
        listing_id = str(payload["item_id"])
        followed = self._tracked.pop(listing_id, None)
        if followed is None:
            return OUTCOME_UNTRACKED_REMOVAL, None
        # A removal may carry only the item ID, name and price, and its name has the phase written in, so
        # the item and a missing price come from the followed listing.
        price_cents = mills_to_cents(payload.get("price")) or followed.price_cents
        try:
            tick = MarketTick(
                venue=VENUE,
                kind=TickKind.OTHER_FEED_EVENT,
                market_hash_name=followed.market_hash_name,
                price_usd=price_cents / 100,
                timestamp=received_at_ms // 1000,
                observed_at_ms=received_at_ms,
                event_type=REMOVED_EVENT_TYPE,
                listing_id=listing_id,
            )
        except (ValidationError, ValueError, TypeError):
            return OUTCOME_MALFORMED, None
        return OUTCOME_RECORDED, tick


class WaxpeerScraper(BaseScraper):
    """
    Records Waxpeer listings from its public Socket.IO feed. There is no REST poll: the feed reports every new
    listing, price change and removal. Docs: https://docs.waxpeer.com/socketio. Observed behaviour and
    measurements: docs/waxpeer_feed.md.
    """

    polls_rest = False

    def __init__(self) -> None:
        super().__init__(venue=VENUE)
        export_feed_metrics()
        self._api_key = os.getenv(API_KEY_ENV) or None
        self.resale_prices = ResalePrices(resale_venue_for(VENUE))
        self.recording_filter = RecordingFilter(self.resale_prices)

    async def poll_market_stream(self) -> AsyncGenerator[MarketTick, None]:
        """Waxpeer has no REST poll; the live feed carries everything."""
        return
        yield  # pragma: no cover

    def _headers(self) -> dict[str, str]:
        headers = {"User-Agent": "BrandSniperEdgeTelemetry/1.0"}
        if self._api_key:
            # The documented way to authenticate the feed: the API key as the raw `authorization` header.
            headers["authorization"] = self._api_key
        return headers

    def _open_session(self) -> aiohttp.ClientSession:
        return aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=15))

    def _open_cache(self) -> Redis:
        return edge_redis_from_env()

    async def listen_websocket_stream(self) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        """
        Connects to the feed, subscribes to CS2 listings, and yields a MarketTick per recorded event plus a
        FeedEvent per group of raw payloads. Raises FeedConnectionLost when the connection ends, after
        handing on the raw payloads still pending.
        """
        if not self._api_key:
            logger.warning("[WAXPEER] WAXPEER_API_KEY is not set; connecting to the public feed without it.")
        cache = self._open_cache()
        session = self._open_session()
        raw_batch = RawEventBatch(VENUE, {"channel": GAME_CHANNEL})
        try:
            try:
                async with session.ws_connect(SOCKET_URL, headers=self._headers(), heartbeat=None) as ws:
                    async for item in self._read_feed(ws, cache, raw_batch):
                        yield item
                reason = "the server closed the connection"
            except (aiohttp.ClientError, TimeoutError, FeedConnectionLost) as err:
                reason = str(err) or type(err).__name__
            pending = raw_batch.take()
            if pending is not None:
                yield pending
            raise FeedConnectionLost(f"Waxpeer feed connection ended: {reason}")
        finally:
            feed_connected.set(0)
            feed_reconnects_total.inc()
            await session.close()
            await cache.aclose()

    async def _refresh_resale_prices(self, cache: Redis) -> None:
        try:
            loaded = await self.resale_prices.refresh(cache)
        except RedisError as err:
            logger.warning("[WAXPEER] Could not read %s baselines from the edge Redis: %s", self.resale_prices.venue, err)
            return
        if loaded:
            logger.info(
                "[WAXPEER] Judging listings against %s build %s: %d item prices, %d sticker prices.",
                self.resale_prices.venue,
                self.resale_prices.build_id,
                len(self.resale_prices.latest_price_cents),
                len(self.resale_prices.sticker_prices_cents),
            )
        feed_resale_prices.set(len(self.resale_prices.latest_price_cents))

    async def _read_feed(
        self, ws: aiohttp.ClientWebSocketResponse, cache: Redis, raw_batch: RawEventBatch
    ) -> AsyncGenerator[MarketTick | FeedEvent, None]:
        receive_timeout: float = DEFAULT_RECEIVE_TIMEOUT_SECONDS
        next_refresh_at = 0.0
        while True:
            # A server that stops pinging for longer than its ping interval plus timeout has gone away.
            message = await ws.receive(timeout=receive_timeout)
            if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                return
            if message.type == aiohttp.WSMsgType.ERROR:
                raise FeedConnectionLost(f"websocket error: {ws.exception()}")
            if message.type != aiohttp.WSMsgType.TEXT:
                continue

            now = time.time()
            received_at_ms = int(now * 1000)
            due = raw_batch.take_if_due(received_at_ms)
            if due is not None:
                feed_tracked_listings.set(self.recording_filter.tracked_count)
                yield due

            frame: str = message.data
            if frame == ENGINE_PING:
                await ws.send_str(ENGINE_PONG)
                continue
            if frame.startswith(SOCKET_EVENT):
                event = self._parse_event(frame)
                if event is None:
                    continue
                name, data = event
                if name == SUBSCRIBED_EVENT:
                    feed_connected.set(1)
                    logger.info("[WAXPEER] Subscribed to the %s listing feed.", GAME_CHANNEL)
                    continue
                if now >= next_refresh_at:
                    await self._refresh_resale_prices(cache)
                    loaded = bool(self.resale_prices.latest_price_cents)
                    next_refresh_at = now + (RESALE_PRICES_REFRESH_SECONDS if loaded else RESALE_PRICES_RETRY_SECONDS)
                tick = self._filter_event(name, data, received_at_ms)
                if tick is not None:
                    raw_batch.add(name, data, received_at_ms)
                    yield tick
                continue
            if frame.startswith(SOCKET_CONNECT):
                await ws.send_str(subscribe_frame(GAME_CHANNEL))
                continue
            if frame.startswith(ENGINE_OPEN):
                receive_timeout = self._receive_timeout(frame)
                await ws.send_str(SOCKET_CONNECT)
                continue
            if frame.startswith(SOCKET_CONNECT_ERROR):
                raise FeedConnectionLost(f"the server refused the connection: {frame[len(SOCKET_CONNECT_ERROR) :]}")
            if frame.startswith(SOCKET_DISCONNECT) or frame == ENGINE_CLOSE:
                raise FeedConnectionLost("the server ended the session")

    @staticmethod
    def _receive_timeout(open_frame: str) -> float:
        """Seconds without a frame after which the connection is dead: the ping interval plus the ping timeout."""
        try:
            handshake = json.loads(open_frame[len(ENGINE_OPEN) :])
            return (int(handshake["pingInterval"]) + int(handshake["pingTimeout"])) / 1000
        except (ValueError, KeyError, TypeError):
            return DEFAULT_RECEIVE_TIMEOUT_SECONDS

    @staticmethod
    def _parse_event(frame: str) -> tuple[str, Any] | None:
        try:
            return socketio_event(frame)
        except ValueError:
            feed_events_filtered_total.labels(event="unparsed", outcome=OUTCOME_MALFORMED).inc()
            return None

    def _filter_event(self, name: str, data: Any, received_at_ms: int) -> MarketTick | None:
        if name in (NEW_EVENT, UPDATE_EVENT):
            outcome, tick = self.recording_filter.listing(name, data, received_at_ms)
        elif name == REMOVED_EVENT:
            outcome, tick = self.recording_filter.removal(data, received_at_ms)
        else:
            return None
        feed_events_filtered_total.labels(event=name, outcome=outcome).inc()
        return tick
