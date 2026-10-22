"""
Trade alerts (#18): a message on Discord and/or Telegram for every listing the Edge DRE approves, with a link
that opens it. There is no automated buying, so the alert is how a person acts on an approval.

Sending never slows the decision path. `AlertDispatcher.submit` returns at once: each channel has its own
bounded queue and a single worker, and an alert that finds its channel's queue full is dropped and counted.
Each worker keeps its channel under a message rate and waits out the `retry_after` of a provider's 429. An alert
failure is logged and counted; it never reaches tick processing.

Every attempt is recorded in the backend's `trade_alerts` table (listing, venue, channel, outcome, latency), so
alerts can later be compared with what the market did (`listing_outcomes`).

Webhook URLs and bot tokens are secrets (a Telegram token is part of the request URL), so no log line holds a
request URL or the message of a request exception: failures are logged by HTTP status or exception type only.
"""

import abc
import asyncio
import html
import os
import re
import time
from collections import Counter, OrderedDict, deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Any, ClassVar, Self
from urllib.parse import quote

import aiohttp
from listener_telemetry import alert_latency_seconds, alert_records_total, alerts_deduplicated_total, alerts_total
from models import MarketTick
from shared_utils import backend_api_headers, get_logger, purchase_keys, resale_venue_for, utc_now_naive
from task_supervisor import BoundedTaskPool

logger = get_logger("listener.alerts")

# What decided to alert: the Z score plus the Edge DRE. A model decision source arrives with shadow scoring (#236).
DECISION_SOURCE_RULES = "rules"

OUTCOME_SENT = "sent"
OUTCOME_RATE_LIMITED = "rate_limited"  # The provider kept answering 429, or the wait for the channel was too long
OUTCOME_FAILED = "failed"  # Any other HTTP error, a timeout, or a connection error
OUTCOME_DROPPED = "dropped"  # The channel's queue was full, so nothing was sent
ALERT_OUTCOMES = (OUTCOME_SENT, OUTCOME_RATE_LIMITED, OUTCOME_FAILED, OUTCOME_DROPPED)

# Alerts remembered per process to skip a repeat of the same listing; empty again after a restart.
ALERTED_MEMORY_SIZE = 10_000
# Wait after a 429 that names no retry time.
DEFAULT_RETRY_AFTER_SECONDS = 5.0
# Distinct stickers named in an alert; the rest are counted.
STICKERS_SHOWN = 5
# Discord message flag that stops the link from unfolding into a preview card.
DISCORD_SUPPRESS_EMBEDS = 1 << 2
TELEGRAM_API_URL = "https://api.telegram.org"

VENUE_NAMES = {"skinport": "Skinport", "waxpeer": "Waxpeer"}
# A REST snapshot names no listing, so its alert links to the venue's market filtered to the item.
MARKET_SEARCH_URLS = {"skinport": "https://skinport.com/market?search={name}"}

_DISCORD_MARKDOWN = re.compile(r"([\\*_~`|>#\[\]()])")


@dataclass(frozen=True, slots=True)
class TradeAlert:
    """Everything an alert says about one approved listing."""

    venue: str
    market_hash_name: str
    price_cents: int
    # Fee aware estimate, the same one the paper trade records; None without a baseline price to resell at.
    estimated_net_margin_cents: int | None
    resale_venue: str
    decision_source: str
    approval_reason: str  # The DRE rule that approved it
    z_source: str  # local, hybrid, or macro
    z_score: float
    is_rest_snapshot: bool
    listing_id: str | None
    listing_url: str | None
    float_value: float | None
    pattern: int | None
    sticker_names: tuple[str, ...]
    received_at: int  # Unix time the tick was received
    received_monotonic: float  # time.monotonic() when the tick was built, for the latency

    @classmethod
    def from_tick(
        cls,
        tick: MarketTick,
        *,
        estimated_net_margin_cents: int | None,
        approval_reason: str,
        z_source: str,
        z_score: float,
    ) -> Self:
        sticker_names = tuple(str(sticker["name"]) for sticker in tick.stickers if sticker.get("name"))
        return cls(
            venue=tick.venue,
            market_hash_name=tick.market_hash_name,
            price_cents=tick.price_cents,
            estimated_net_margin_cents=estimated_net_margin_cents,
            resale_venue=resale_venue_for(tick.venue),
            decision_source=DECISION_SOURCE_RULES,
            approval_reason=approval_reason,
            z_source=z_source,
            z_score=z_score,
            is_rest_snapshot=tick.is_rest_snapshot,
            listing_id=tick.listing_id,
            listing_url=tick.listing_url,
            float_value=tick.float_value,
            pattern=tick.pattern,
            sticker_names=sticker_names,
            received_at=tick.timestamp,
            received_monotonic=tick.received_monotonic,
        )


@dataclass(frozen=True, slots=True)
class AlertLink:
    label: str
    url: str


def venue_name(venue: str) -> str:
    return VENUE_NAMES.get(venue, venue)


def usd(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) / 100:,.2f}"


def sticker_summary(sticker_names: Sequence[str]) -> str | None:
    """`3: 2x Crown (Foil), Titan (Holo)`; repeated stickers are counted, and only the first few are named."""
    if not sticker_names:
        return None
    named = [f"{count}x {name}" if count > 1 else name for name, count in Counter(sticker_names).items()]
    shown = ", ".join(named[:STICKERS_SHOWN])
    hidden = len(named) - STICKERS_SHOWN
    if hidden > 0:
        shown += f" and {hidden} more"
    return f"{len(sticker_names)}: {shown}"


def alert_link(alert: TradeAlert) -> AlertLink | None:
    """The listing's own link, or for a REST snapshot a market search for the item; None when there is neither."""
    if alert.listing_url:
        return AlertLink("Open listing", alert.listing_url)
    search_url = MARKET_SEARCH_URLS.get(alert.venue)
    if search_url is None:
        return None
    return AlertLink(f"Search {venue_name(alert.venue)}", search_url.format(name=quote(alert.market_hash_name, safe="")))


def alert_lines(alert: TradeAlert) -> list[str]:
    """The alert's text as plain lines, without the link."""
    offer = "lowest ask" if alert.is_rest_snapshot else "listing"
    lines = [
        f"Buy signal: {venue_name(alert.venue)} {offer}",
        alert.market_hash_name,
        f"Price: {usd(alert.price_cents)}",
    ]

    margin = alert.estimated_net_margin_cents
    if margin is None:
        lines.append("Est. net margin: unknown (no resale price in the baseline)")
    else:
        margin_percent = margin / alert.price_cents * 100
        lines.append(
            f"Est. net margin: {usd(margin)} ({margin_percent:.1f}% of price, after fees, "
            f"resold on {venue_name(alert.resale_venue)})"
        )

    details = []
    if alert.float_value is not None:
        details.append(f"Float: {alert.float_value:.6f}")
    if alert.pattern is not None:
        details.append(f"Pattern: {alert.pattern}")
    if details:
        lines.append(" | ".join(details))

    stickers = sticker_summary(alert.sticker_names)
    if stickers is not None:
        lines.append(f"Stickers {stickers}")

    lines.append(f"Decision: {alert.decision_source}, rule {alert.approval_reason}, Z {alert.z_score:.2f} ({alert.z_source})")
    return lines


def discord_escape(text: str) -> str:
    return _DISCORD_MARKDOWN.sub(r"\\\1", text)


def format_discord(alert: TradeAlert) -> str:
    lines = [discord_escape(line) for line in alert_lines(alert)]
    link = alert_link(alert)
    if link is not None:
        lines.append(f"[{link.label}]({link.url})")
    return "\n".join(lines)


def format_telegram(alert: TradeAlert) -> str:
    """Text for Telegram's HTML parse mode."""
    lines = [html.escape(line, quote=False) for line in alert_lines(alert)]
    link = alert_link(alert)
    if link is not None:
        lines.append(f'<a href="{html.escape(link.url)}">{html.escape(link.label)}</a>')
    return "\n".join(lines)


class AlertChannel(abc.ABC):
    """One place alerts go. A channel only builds requests and reads replies; the dispatcher sends them."""

    name: ClassVar[str]

    @abc.abstractmethod
    def request(self, alert: TradeAlert) -> tuple[str, dict[str, Any]]:
        """URL and JSON body of the request that posts the alert. The URL holds a secret: never log it."""

    @abc.abstractmethod
    def accepted(self, status: int, body: Any) -> bool:
        """True when the provider took the message."""

    @abc.abstractmethod
    def retry_after_seconds(self, body: Any, headers: Mapping[str, str]) -> float | None:
        """How long a 429 reply asks the sender to wait; None when it does not say."""


class DiscordWebhookChannel(AlertChannel):
    """Posts to a Discord channel webhook (https://discord.com/developers/docs/resources/webhook#execute-webhook)."""

    name = "discord"

    def __init__(self, webhook_url: str) -> None:
        self._webhook_url = webhook_url

    def request(self, alert: TradeAlert) -> tuple[str, dict[str, Any]]:
        body = {
            "content": format_discord(alert),
            # Item and sticker names are not allowed to ping anyone.
            "allowed_mentions": {"parse": []},
            "flags": DISCORD_SUPPRESS_EMBEDS,
        }
        return self._webhook_url, body

    def accepted(self, status: int, body: Any) -> bool:
        return 200 <= status < 300

    def retry_after_seconds(self, body: Any, headers: Mapping[str, str]) -> float | None:
        # The body gives seconds with a fraction (https://discord.com/developers/docs/topics/rate-limits).
        if isinstance(body, dict):
            seconds = _seconds(body.get("retry_after"))
            if seconds is not None:
                return seconds
        return _seconds(headers.get("Retry-After"))


class TelegramBotChannel(AlertChannel):
    """Sends with a Telegram bot to one chat (https://core.telegram.org/bots/api#sendmessage)."""

    name = "telegram"

    def __init__(self, bot_token: str, chat_id: str, *, api_url: str = TELEGRAM_API_URL) -> None:
        self._send_url = f"{api_url.rstrip('/')}/bot{bot_token}/sendMessage"
        self._chat_id = chat_id

    def request(self, alert: TradeAlert) -> tuple[str, dict[str, Any]]:
        body = {
            "chat_id": self._chat_id,
            "text": format_telegram(alert),
            "parse_mode": "HTML",
            "link_preview_options": {"is_disabled": True},
        }
        return self._send_url, body

    def accepted(self, status: int, body: Any) -> bool:
        return 200 <= status < 300 and isinstance(body, dict) and body.get("ok") is True

    def retry_after_seconds(self, body: Any, headers: Mapping[str, str]) -> float | None:
        # Whole seconds in `parameters.retry_after` (https://core.telegram.org/bots/api#responseparameters).
        if isinstance(body, dict) and isinstance(body.get("parameters"), dict):
            seconds = _seconds(body["parameters"].get("retry_after"))
            if seconds is not None:
                return seconds
        return _seconds(headers.get("Retry-After"))


def _seconds(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


class RateLimiter:
    """
    At most `max_per_minute` sends in any 60 seconds, plus any pause a provider's 429 asked for.

    Not safe for concurrent use: each channel has a single worker, and only that worker takes slots.
    """

    WINDOW_SECONDS = 60.0

    def __init__(
        self,
        max_per_minute: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.max_per_minute = max_per_minute
        self._clock = clock
        self._sleep = sleep
        self._sent: deque[float] = deque()
        self._paused_until = 0.0

    def pause(self, seconds: float) -> None:
        self._paused_until = max(self._paused_until, self._clock() + seconds)

    def wait_seconds(self) -> float:
        """How long until the next send is allowed."""
        now = self._clock()
        while self._sent and self._sent[0] <= now - self.WINDOW_SECONDS:
            self._sent.popleft()
        wait = max(0.0, self._paused_until - now)
        if len(self._sent) >= self.max_per_minute:
            wait = max(wait, self._sent[0] + self.WINDOW_SECONDS - now)
        return wait

    async def acquire(self, max_wait_seconds: float) -> bool:
        """Waits for the next send slot and takes it; False, without waiting, when it is further away than allowed."""
        while (wait := self.wait_seconds()) > 0:
            if wait > max_wait_seconds:
                return False
            await self._sleep(wait)
        self._sent.append(self._clock())
        return True


def _env_flag(name: str) -> bool:
    """A channel switch: on unless set to false, 0, or no."""
    return os.getenv(name, "true").strip().lower() not in ("false", "0", "no")


def _positive_env(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return value


class AlertDispatcher:
    """Sends trade alerts to every configured channel, each through its own bounded queue and single worker."""

    def __init__(
        self,
        channels: Sequence[AlertChannel],
        *,
        record_url: str | None = None,
        record_headers: Mapping[str, str] | None = None,
        max_per_minute: int = 20,
        queue_size: int = 32,
        max_wait_seconds: float = 60.0,
        max_attempts: int = 3,
        shutdown_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.channels = list(channels)
        self._record_url = record_url
        self._record_headers = dict(record_headers or {})
        self._queue_size = queue_size
        self._max_wait_seconds = max_wait_seconds
        self._max_attempts = max_attempts
        self._shutdown_timeout = shutdown_timeout
        self._limiters = {channel.name: RateLimiter(max_per_minute, clock=clock, sleep=sleep) for channel in self.channels}
        self._pools: dict[str, BoundedTaskPool] = {}
        self._alerted: OrderedDict[str, None] = OrderedDict()
        self._session: aiohttp.ClientSession | None = None

    @classmethod
    def from_env(cls, backend_url: str, *, shutdown_timeout: float) -> Self:
        """
        Channels from the environment. Discord is on when DISCORD_WEBHOOK_URL is set, Telegram when TELEGRAM_BOT_TOKEN
        and TELEGRAM_CHAT_ID are; DISCORD_ALERTS_ENABLED or TELEGRAM_ALERTS_ENABLED set to false turns one off.
        """
        channels: list[AlertChannel] = []
        webhook_url = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
        if webhook_url and _env_flag("DISCORD_ALERTS_ENABLED"):
            channels.append(DiscordWebhookChannel(webhook_url))

        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
        if bot_token and _env_flag("TELEGRAM_ALERTS_ENABLED"):
            if chat_id:
                channels.append(TelegramBotChannel(bot_token, chat_id))
            else:
                logger.warning("[ALERT] TELEGRAM_BOT_TOKEN is set without TELEGRAM_CHAT_ID, so Telegram alerts are off.")

        return cls(
            channels,
            record_url=f"{backend_url.rstrip('/')}/api/v1/ingest/alert",
            record_headers=backend_api_headers() if channels else None,
            max_per_minute=int(_positive_env("ALERT_MAX_PER_MINUTE", 20)),
            queue_size=int(_positive_env("ALERT_QUEUE_SIZE", 32)),
            max_wait_seconds=_positive_env("ALERT_MAX_WAIT_SECONDS", 60),
            max_attempts=int(_positive_env("ALERT_MAX_ATTEMPTS", 3)),
            shutdown_timeout=shutdown_timeout,
        )

    async def __aenter__(self) -> Self:
        if self.channels:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10, connect=3))
        for channel in self.channels:
            pool = BoundedTaskPool(
                f"alert_{channel.name}",
                workers=1,
                queue_size=self._queue_size,
                shutdown_timeout=self._shutdown_timeout,
            )
            await pool.__aenter__()
            self._pools[channel.name] = pool
            for outcome in ALERT_OUTCOMES:
                alerts_total.labels(channel=channel.name, outcome=outcome)
        names = ", ".join(channel.name for channel in self.channels) or "none configured"
        logger.info("[ALERT] Trade alert channels: %s", names)
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.close()

    async def close(self) -> None:
        """Sends what is queued (up to the shutdown timeout), then closes the HTTP session."""
        try:
            for pool in self._pools.values():
                await pool.close()
        finally:
            if self._session is not None:
                await self._session.close()
                self._session = None

    def submit(self, alert: TradeAlert) -> None:
        """
        Queues the alert on every channel without waiting.

        A listing is alerted once per process, like a paper trade (`executor.ExecutionService`): a repeat is
        skipped, and so is the REST snapshot that shows an alerted listing as the item's lowest ask that day.
        """
        if not self._pools:
            return
        key, snapshot_key = purchase_keys(
            alert.venue, alert.market_hash_name, alert.price_cents, alert.listing_id, utc_now_naive().date()
        )
        if key in self._alerted:
            alerts_deduplicated_total.inc()
            logger.info(
                "[ALERT] Skipped repeat | Venue: %s | Item: %s | Listing: %s",
                alert.venue,
                alert.market_hash_name,
                alert.listing_id or "REST snapshot",
            )
            return
        self._remember(key)
        if snapshot_key != key:
            self._remember(snapshot_key)

        for channel in self.channels:
            if not self._pools[channel.name].try_submit(partial(self._deliver, channel, alert)):
                alerts_total.labels(channel=channel.name, outcome=OUTCOME_DROPPED).inc()
                logger.warning(
                    "[ALERT] Dropped, the %s queue is full | Venue: %s | Item: %s | Listing: %s",
                    channel.name,
                    alert.venue,
                    alert.market_hash_name,
                    alert.listing_id or "REST snapshot",
                )

    def _remember(self, key: str) -> None:
        self._alerted[key] = None
        while len(self._alerted) > ALERTED_MEMORY_SIZE:
            self._alerted.popitem(last=False)

    async def _deliver(self, channel: AlertChannel, alert: TradeAlert) -> None:
        outcome, http_status = await self._send(channel, alert)
        latency_seconds = time.monotonic() - alert.received_monotonic
        alerts_total.labels(channel=channel.name, outcome=outcome).inc()
        if outcome == OUTCOME_SENT:
            alert_latency_seconds.labels(channel=channel.name).observe(latency_seconds)
            logger.info(
                "[ALERT] Sent | Channel: %s | Venue: %s | Item: %s | Price: %s | Listing: %s | Latency: %.2fs",
                channel.name,
                alert.venue,
                alert.market_hash_name,
                usd(alert.price_cents),
                alert.listing_id or "REST snapshot",
                latency_seconds,
            )
        else:
            logger.warning(
                "[ALERT] Not sent (%s) | Channel: %s | HTTP: %s | Venue: %s | Item: %s | Listing: %s",
                outcome,
                channel.name,
                http_status if http_status is not None else "none",
                alert.venue,
                alert.market_hash_name,
                alert.listing_id or "REST snapshot",
            )
        await self._record(channel, alert, outcome, latency_seconds)

    async def _send(self, channel: AlertChannel, alert: TradeAlert) -> tuple[str, int | None]:
        """Posts the alert, waiting for the channel's rate limit and retrying after a 429. Returns (outcome, last status)."""
        session = self._require_session()
        url, body = channel.request(alert)
        limiter = self._limiters[channel.name]
        status: int | None = None
        for _attempt in range(self._max_attempts):
            if not await limiter.acquire(self._max_wait_seconds):
                return OUTCOME_RATE_LIMITED, status
            try:
                async with session.post(url, json=body) as response:
                    status = response.status
                    reply = await _json_or_none(response)
                    if status == 429:
                        retry_after = channel.retry_after_seconds(reply, response.headers)
                        limiter.pause(retry_after if retry_after is not None else DEFAULT_RETRY_AFTER_SECONDS)
                        continue
                    outcome = OUTCOME_SENT if channel.accepted(status, reply) else OUTCOME_FAILED
                    return outcome, status
            except (TimeoutError, aiohttp.ClientError) as err:
                # The exception text can hold the request URL, and with it the webhook or bot token.
                logger.warning("[ALERT] %s request failed: %s", channel.name, type(err).__name__)
                return OUTCOME_FAILED, None
        return OUTCOME_RATE_LIMITED, status

    async def _record(self, channel: AlertChannel, alert: TradeAlert, outcome: str, latency_seconds: float) -> None:
        """Stores the attempt in the backend's trade_alerts table. A failure is logged and counted, never retried."""
        if self._record_url is None:
            return
        payload = {
            "venue": alert.venue,
            "listing_id": alert.listing_id,
            "market_hash_name": alert.market_hash_name,
            "price_cents": alert.price_cents,
            "estimated_net_margin_cents": alert.estimated_net_margin_cents,
            "decision_source": alert.decision_source,
            "approval_reason": alert.approval_reason,
            "z_source": alert.z_source,
            "z_score": round(alert.z_score, 4),
            "channel": channel.name,
            "outcome": outcome,
            "received_at": alert.received_at,
            "alerted_at_ms": int(time.time() * 1000),
            "latency_ms": round(latency_seconds * 1000),
        }
        session = self._require_session()
        try:
            async with session.post(self._record_url, json=payload, headers=self._record_headers) as response:
                if response.status != 201:
                    alert_records_total.labels(outcome="rejected").inc()
                    logger.warning("[ALERT] Backend rejected the alert record with HTTP %d", response.status)
                    return
        except (TimeoutError, aiohttp.ClientError) as err:
            alert_records_total.labels(outcome="error").inc()
            logger.warning("[ALERT] Could not record the alert in the backend: %s", type(err).__name__)
            return
        alert_records_total.labels(outcome="success").inc()

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise RuntimeError("AlertDispatcher is not running; use it as an async context manager")
        return self._session


async def _json_or_none(response: aiohttp.ClientResponse) -> Any:
    try:
        return await response.json(content_type=None)
    except ValueError:
        return None
