"""
DMarket Trading API: request signing, response parsing, and a rate limited client (#339).

What the live API returns, and where it differs from DMarket's reference, is in docs/dmarket_api.md. In short:

- Every request is signed with Ed25519 over method + decoded route path + query string as sent + body +
  Unix seconds, and carries the signature in three headers.
- The account may make 30 requests a second across all endpoints. A 429 carries `Retry-After: 1`. Each
  caller here keeps to its own share (`max_requests_per_second`), so the listener and the baseline builder
  fit next to each other.
- Prices come as cents in a string on listings ("21345") and as dollars in a string on sales ("24.56", or
  "1550" when the cents are zero). Both are parsed with Decimal, never through a float.
- The seller fee is 10% unless the title is on the reduced list (`customized-fees`), which renews every
  few days and caps each reduced fee at a maximum price.
- Listings page newest first through a cursor that is base64 JSON of a creation time in milliseconds and
  an offer ID. A cursor built from any time starts the page at that time, and paging has no depth limit.
"""

import asyncio
import base64
import binascii
import json
import os
import re
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from yarl import URL

from .item_classifier import build_versioned_name
from .pnl import BASIS_POINTS, DMARKET_FEES

API_URL = "https://api.dmarket.com"
CS2_GAME_ID = "a8db"
OFFERS_PATH = "/marketplace-api/v2/offers"
LAST_SALES_PATH = "/trade-aggregator/v1/last-sales"
CUSTOMIZED_FEES_PATH = "/exchange/v1/customized-fees"

SIGNATURE_PREFIX = "dmar ed25519 "
LAST_SALES_PAGE_SIZE = 20  # The most `last-sales` returns per request
FEES_PAGE_SIZE = 100
OFFERS_PAGE_SIZE = 100  # The most `offers` returns per request

# `offerType` of a listing held by a DMarket bot, the only kind the API can buy. A person to person
# listing is `p2p`.
OFFER_TYPE_BOT = "dmarket"

SALE_TYPE_OFFER = "offer"  # Someone bought a listing
SALE_TYPE_TARGET = "target"  # A seller sold into a buy order
_SALE_TYPES = {"Offer": SALE_TYPE_OFFER, "Target": SALE_TYPE_TARGET}

# A 429 is retried after Retry-After, doubling on each refusal in a row up to this many seconds.
MAX_BACKOFF_SECONDS = 30.0
DEFAULT_MAX_RETRIES = 8

_CENTS = Decimal(100)


class DMarketAPIError(RuntimeError):
    """A DMarket request failed with an HTTP status other than 429, or kept being refused."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"DMarket answered HTTP {status}: {message}")
        self.status = status


@dataclass(frozen=True)
class DMarketKeys:
    """The account's API key pair. Only the public key is ever sent."""

    public_key: str
    private_key: Ed25519PrivateKey

    @classmethod
    def from_hex(cls, public_key: str, secret_key: str) -> "DMarketKeys":
        """Keys as DMarket hands them out: the secret key is the 32 byte seed followed by the public key, in hex."""
        try:
            secret = bytes.fromhex(secret_key.strip())
        except ValueError as error:
            raise ValueError("DMARKET_SECRET_KEY is not hex") from error
        if len(secret) not in (32, 64):
            raise ValueError(f"DMARKET_SECRET_KEY must be 32 or 64 bytes of hex, got {len(secret)} bytes")
        return cls(public_key=public_key.strip().lower(), private_key=Ed25519PrivateKey.from_private_bytes(secret[:32]))

    @classmethod
    def from_env(cls) -> "DMarketKeys":
        """Keys from DMARKET_PUBLIC_KEY and DMARKET_SECRET_KEY. Raises ValueError when either is missing."""
        public_key = os.getenv("DMARKET_PUBLIC_KEY", "")
        secret_key = os.getenv("DMARKET_SECRET_KEY", "")
        if not public_key or not secret_key:
            raise ValueError("DMARKET_PUBLIC_KEY and DMARKET_SECRET_KEY must both be set")
        return cls.from_hex(public_key, secret_key)


def encode_query(params: Mapping[str, Any] | None) -> str:
    """The query string as sent and signed, with a leading '?' (empty without parameters)."""
    if not params:
        return ""
    return "?" + urlencode(params, quote_via=quote, doseq=True)


def encode_path(path: str) -> str:
    """The route path as sent: every segment percent encoded (a title in the path keeps its spaces decoded when signed)."""
    return "/".join(quote(segment, safe="") for segment in path.split("/"))


def signed_headers(keys: DMarketKeys, method: str, path: str, query: str, body: str, timestamp: int) -> dict[str, str]:
    """The three authentication headers for one request. `path` is decoded and `query` exactly as sent."""
    unsigned = f"{method.upper()}{path}{query}{body}{timestamp}"
    signature = keys.private_key.sign(unsigned.encode("utf-8")).hex()
    return {
        "X-Api-Key": keys.public_key,
        "X-Sign-Date": str(timestamp),
        "X-Request-Sign": SIGNATURE_PREFIX + signature,
    }


def _decimal(value: object) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    try:
        number = Decimal(str(value).strip())
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def dollars_to_cents(value: object) -> int | None:
    """A sale price ("24.56", "1550") in cents, to the nearest cent; None when unusable or not positive."""
    number = _decimal(value)
    if number is None or number <= 0:
        return None
    return int((number * _CENTS).to_integral_value(rounding=ROUND_HALF_UP))


def cents_to_int(value: object) -> int | None:
    """A listing price in cents ("21345") as an int; None when unusable or not positive."""
    number = _decimal(value)
    if number is None or number <= 0 or number != number.to_integral_value():
        return None
    return int(number)


def phase_version(slug: object) -> str | None:
    """
    The version a Doppler phase or gem slug names, as Skinport writes it: "phase-2" is "Phase 2",
    "black-pearl" is "Black Pearl". None for an empty or missing slug.
    """
    if not isinstance(slug, str) or not slug.strip():
        return None
    return " ".join(word.capitalize() for word in slug.strip().split("-"))


@dataclass(frozen=True, slots=True)
class DMarketSale:
    """One sale from `last-sales`."""

    title: str  # The title as queried, without the phase
    market_hash_name: str  # Versioned name (phase included), as ticks carry it
    sold_at: datetime  # Naive UTC, whole seconds
    price_cents: int
    sale_type: str  # SALE_TYPE_OFFER or SALE_TYPE_TARGET
    float_value: float | None
    paint_seed: int | None
    phase: str | None


def _optional_float(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if 0 <= value <= 1 else None


def _optional_seed(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def parse_sale(title: str, entry: object) -> DMarketSale | None:
    """One `last-sales` entry; None when it has no usable price, time or sale type."""
    if not isinstance(entry, Mapping):
        return None
    price_cents = dollars_to_cents(entry.get("price"))
    sale_type = _SALE_TYPES.get(str(entry.get("txOperationType")))
    date = _decimal(entry.get("date"))
    if price_cents is None or sale_type is None or date is None or date <= 0:
        return None
    attributes = entry.get("offerAttributes")
    if not isinstance(attributes, Mapping):
        attributes = {}
    phase = phase_version(attributes.get("phaseTitle"))
    return DMarketSale(
        title=title,
        market_hash_name=build_versioned_name(title, phase),
        sold_at=datetime.fromtimestamp(int(date), UTC).replace(tzinfo=None),
        price_cents=price_cents,
        sale_type=sale_type,
        float_value=_optional_float(attributes.get("floatValue")),
        paint_seed=_optional_seed(attributes.get("paintSeed")),
        phase=phase,
    )


def parse_sales(title: str, payload: object) -> list[DMarketSale]:
    """Every usable sale in a `last-sales` response, newest first as DMarket sends them."""
    entries = payload.get("sales") if isinstance(payload, Mapping) else None
    if not isinstance(entries, list):
        return []
    sales = (parse_sale(title, entry) for entry in entries)
    return [sale for sale in sales if sale is not None]


def sale_keys(sales: Iterable[DMarketSale]) -> list[str]:
    """
    A key per sale that tells it apart from the title's other sales.

    Several sales of a cheap item often share one second, price and (empty) pattern, and each is a real
    sale, so identical sales are numbered in the order DMarket lists them. The numbering is stable as long
    as every sale of that second is read together, which the builder ensures by reading past the second.
    """
    seen: Counter[str] = Counter()
    keys = []
    for sale in sales:
        float_part = "" if sale.float_value is None else repr(sale.float_value)
        seed_part = "" if sale.paint_seed is None else str(sale.paint_seed)
        base = (
            f"{int(sale.sold_at.replace(tzinfo=UTC).timestamp())}:{sale.price_cents}:{sale.sale_type}:"
            f"{float_part}:{seed_part}:{sale.phase or ''}"
        )
        keys.append(f"{base}#{seen[base]}")
        seen[base] += 1
    return keys


# An RFC 3339 time as listings carry it: whole seconds for bot held listings, nanoseconds for person to person ones.
_RFC3339 = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$")


def created_at_ms(value: object) -> int | None:
    """A listing's `createdAt` in Unix milliseconds (finer digits dropped); None when unusable."""
    if not isinstance(value, str):
        return None
    match = _RFC3339.match(value.strip())
    if match is None:
        return None
    whole, fraction, zone = match.groups()
    try:
        moment = datetime.fromisoformat(whole + ("+00:00" if zone == "Z" else zone))
    except ValueError:
        return None
    milliseconds = int((fraction or "0")[:3].ljust(3, "0"))
    return int(moment.timestamp()) * 1000 + milliseconds


@dataclass(frozen=True, slots=True)
class DMarketOffer:
    """One listing from `offers`."""

    offer_id: str
    created_at_ms: int
    price_cents: int
    offer_type: str  # OFFER_TYPE_BOT or "p2p"
    title: str  # Market hash name without the phase
    market_hash_name: str  # Versioned name (phase included), as ticks carry it
    float_value: float | None
    paint_seed: int | None
    paint_index: int | None
    phase: str | None
    sticker_names: tuple[str, ...]  # Applied stickers, named the way Skinport names them


def _optional_wear_text(value: object) -> float | None:
    """A float given as a decimal string ("0.2313"); None when missing, out of range, or "0" (an item without wear)."""
    number = _decimal(value)
    if number is None or not 0 < number <= 1:
        return None
    return float(number)


def parse_offer(entry: object) -> DMarketOffer | None:
    """One `offers` entry; None when it has no offer ID, creation time, price or title."""
    if not isinstance(entry, Mapping):
        return None
    offer_id = entry.get("offerId")
    created = created_at_ms(entry.get("createdAt"))
    price_cents = cents_to_int(entry.get("priceCents"))
    attributes = entry.get("attributes")
    if not isinstance(attributes, Mapping):
        return None
    title = attributes.get("title")
    if not isinstance(offer_id, str) or not offer_id or created is None or price_cents is None:
        return None
    if not isinstance(title, str) or not title:
        return None
    cs2 = attributes.get("cs2")
    if not isinstance(cs2, Mapping):
        cs2 = {}
    phase = phase_version(cs2.get("phase"))
    stickers = cs2.get("stickers")
    sticker_names = tuple(
        sticker["name"]
        for sticker in (stickers if isinstance(stickers, list) else [])
        if isinstance(sticker, Mapping) and isinstance(sticker.get("name"), str) and sticker["name"]
    )
    offer_type = entry.get("offerType")
    float_value = _optional_wear_text(cs2.get("float"))
    # Items without wear (cases, stickers) send 0 for the pattern and the finish too.
    paint_index = _optional_seed(cs2.get("paintIndex"))
    return DMarketOffer(
        offer_id=offer_id,
        created_at_ms=created,
        price_cents=price_cents,
        offer_type=offer_type if isinstance(offer_type, str) else "",
        title=title,
        market_hash_name=build_versioned_name(title, phase),
        float_value=float_value,
        paint_seed=_optional_seed(cs2.get("paintSeed")) if float_value is not None else None,
        paint_index=paint_index or None,
        phase=phase,
        sticker_names=sticker_names,
    )


@dataclass(frozen=True, slots=True)
class OffersPage:
    """One page of `offers`: the raw entries, newest first, and the cursor of the next page (None at the end)."""

    entries: list[Any]
    cursor: str | None


def parse_offers_page(payload: object) -> OffersPage:
    entries = payload.get("items") if isinstance(payload, Mapping) else None
    cursor = payload.get("cursor") if isinstance(payload, Mapping) else None
    return OffersPage(
        entries=entries if isinstance(entries, list) else [],
        cursor=cursor if isinstance(cursor, str) and cursor else None,
    )


def offers_cursor(created_at_ms: int, offer_id: str = "") -> str:
    """
    A cursor that starts a page of listings, newest first, at `created_at_ms`.

    DMarket's own cursors are base64 JSON of the last listing's creation time in milliseconds and its offer
    ID. One built from a time alone starts at that time (checked live on 2026-10-25), which lets catching up
    jump straight to where it stopped.
    """
    return base64.b64encode(json.dumps([created_at_ms, offer_id], separators=(",", ":")).encode()).decode()


def cursor_created_at_ms(cursor: str) -> int | None:
    """
    The creation time a cursor points at; None for a cursor that is not DMarket's base64 JSON. The URL safe
    alphabet (`-` and `_`) is read too, in case DMarket ever sends it.
    """
    try:
        position = json.loads(base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True))
    except (binascii.Error, ValueError):
        return None
    if not isinstance(position, list) or not position:
        return None
    created = position[0]
    if isinstance(created, bool) or not isinstance(created, int):
        return None
    return created


def fraction_to_bps(value: object) -> int | None:
    """A fee fraction ("0.02") in basis points; None when unusable or outside [0, 1)."""
    number = _decimal(value)
    if number is None or not 0 <= number < 1:
        return None
    return int((number * BASIS_POINTS).to_integral_value())


@dataclass(frozen=True, slots=True)
class ReducedFee:
    """A title's reduced seller fee, up to a maximum price (no cap when `max_price_cents` is None)."""

    fee_bps: int
    max_price_cents: int | None


@dataclass(frozen=True)
class FeeSchedule:
    """DMarket's seller fees for one game: the default, and the titles with a reduced fee."""

    default_fee_bps: int
    reduced: Mapping[str, ReducedFee]

    def fee_bps(self, title: str, price_cents: int) -> int:
        """The seller fee of `title` at `price_cents`. A price above the reduced fee's cap pays the default."""
        reduced = self.reduced.get(title)
        if reduced is None:
            return self.default_fee_bps
        if reduced.max_price_cents is not None and price_cents > reduced.max_price_cents:
            return self.default_fee_bps
        return reduced.fee_bps


def parse_fee_page(payload: object) -> tuple[int | None, dict[str, ReducedFee]]:
    """The default fee (None when absent) and the reduced fees on one `customized-fees` page."""
    if not isinstance(payload, Mapping):
        return None, {}
    default = payload.get("defaultFee")
    default_bps = fraction_to_bps(default.get("fraction")) if isinstance(default, Mapping) else None
    reduced: dict[str, ReducedFee] = {}
    entries = payload.get("reducedFees")
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, Mapping):
            continue
        title = entry.get("title")
        fee_bps = fraction_to_bps(entry.get("fraction"))
        if not isinstance(title, str) or not title or fee_bps is None:
            continue
        max_price = entry.get("maxPrice")
        has_cap = isinstance(max_price, int) and not isinstance(max_price, bool) and max_price > 0
        reduced[title] = ReducedFee(fee_bps=fee_bps, max_price_cents=max_price if has_cap else None)
    return default_bps, reduced


def _entries(payload: object, key: str) -> int:
    entries = payload.get(key) if isinstance(payload, Mapping) else None
    return len(entries) if isinstance(entries, list) else 0


def retry_after_seconds(headers: Mapping[str, str]) -> float | None:
    """Seconds from a Retry-After header given in seconds; None when absent or unusable."""
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


class DMarketClient:
    """
    Signed GET requests to the DMarket API, paced to `max_requests_per_second`.

    Requests start at least 1 / `max_requests_per_second` apart. When a response says no requests are left
    this second, the next one waits a second. A 429 is retried after Retry-After, doubling the wait on each
    refusal in a row up to MAX_BACKOFF_SECONDS, and fails after `max_retries` refusals.
    """

    def __init__(
        self,
        keys: DMarketKeys,
        session: aiohttp.ClientSession,
        max_requests_per_second: float = 10.0,
        *,
        base_url: str = API_URL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        on_rate_limited: Callable[[], None] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if max_requests_per_second <= 0:
            raise ValueError("max_requests_per_second must be positive")
        self._keys = keys
        self._session = session
        self._interval = 1.0 / max_requests_per_second
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries
        self._on_rate_limited = on_rate_limited
        self._monotonic = monotonic
        self._wall_clock = wall_clock
        self._sleep = sleep
        self._next_request_at = 0.0
        self._lock = asyncio.Lock()

    async def _wait_for_turn(self) -> None:
        async with self._lock:
            now = self._monotonic()
            if self._next_request_at > now:
                await self._sleep(self._next_request_at - now)
                now = self._next_request_at
            self._next_request_at = now + self._interval

    def _pause_after(self, headers: Mapping[str, str]) -> None:
        """Hold the next request back for a second when this response used the last request of the second."""
        remaining = headers.get("X-RateLimit-Remaining-Second")
        if remaining is not None and remaining.strip() == "0":
            self._next_request_at = max(self._next_request_at, self._monotonic() + 1.0)

    async def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        """The decoded JSON of a signed GET request. Raises DMarketAPIError on any failure but a retried 429."""
        query = encode_query(params)
        url = URL(self._base_url + encode_path(path) + query, encoded=True)
        backoff = 1.0
        for _attempt in range(self._max_retries + 1):
            await self._wait_for_turn()
            headers = signed_headers(self._keys, "GET", path, query, "", int(self._wall_clock()))
            headers["Accept"] = "application/json"
            async with self._session.get(url, headers=headers) as response:
                self._pause_after(response.headers)
                if response.status == 429:
                    if self._on_rate_limited is not None:
                        self._on_rate_limited()
                    wait = max(retry_after_seconds(response.headers) or 0.0, backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)
                    await self._sleep(min(wait, MAX_BACKOFF_SECONDS))
                    continue
                if response.status != 200:
                    raise DMarketAPIError(response.status, (await response.text())[:300])
                try:
                    return await response.json(content_type=None)
                except ValueError as err:
                    # A 200 that is not JSON (a maintenance page from a proxy) fails like any other answer.
                    raise DMarketAPIError(response.status, f"the answer is not JSON: {err}") from err
        raise DMarketAPIError(429, f"still rate limited after {self._max_retries} retries")

    async def last_sales(self, title: str, offset: int = 0, game_id: str = CS2_GAME_ID) -> tuple[list[DMarketSale], int]:
        """One page of a title's sales, newest first, and how many entries the page held before parsing."""
        payload = await self.get(
            LAST_SALES_PATH, {"gameId": game_id, "title": title, "limit": LAST_SALES_PAGE_SIZE, "offset": offset}
        )
        return parse_sales(title, payload), _entries(payload, "sales")

    async def offers(self, cursor: str | None = None, game_id: str = CS2_GAME_ID) -> OffersPage:
        """One page of bot held listings, newest first, starting at `cursor` (the newest listings without one)."""
        params: dict[str, Any] = {
            "gameId": game_id,
            "limit": OFFERS_PAGE_SIZE,
            "orderBy": "createdAt",
            "orderDir": "desc",
            "currency": "USD",
        }
        if cursor is not None:
            params["cursor"] = cursor
        return parse_offers_page(await self.get(OFFERS_PATH, params))

    async def fee_schedule(self, game_id: str = CS2_GAME_ID) -> FeeSchedule:
        """The default seller fee and every reduced fee, read page by page."""
        default_bps: int | None = None
        reduced: dict[str, ReducedFee] = {}
        offset = 0
        while True:
            payload = await self.get(CUSTOMIZED_FEES_PATH, {"gameId": game_id, "limit": FEES_PAGE_SIZE, "offset": offset})
            page_default, page_reduced = parse_fee_page(payload)
            default_bps = default_bps if page_default is None else page_default
            reduced.update(page_reduced)
            if _entries(payload, "reducedFees") < FEES_PAGE_SIZE:
                break
            offset += FEES_PAGE_SIZE
        if default_bps is None:
            # Never price below DMarket's documented worst case when the default is missing.
            default_bps = DMARKET_FEES.seller_fee_bps(0)
        return FeeSchedule(default_fee_bps=default_bps, reduced=reduced)
