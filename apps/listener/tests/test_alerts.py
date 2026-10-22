import asyncio
import logging
from dataclasses import dataclass, field, replace
from typing import Any

import alerts as alerts_module
import pytest
import pytest_asyncio
from aiohttp import web
from alerts import (
    OUTCOME_DROPPED,
    OUTCOME_FAILED,
    OUTCOME_RATE_LIMITED,
    OUTCOME_SENT,
    AlertDispatcher,
    DiscordWebhookChannel,
    RateLimiter,
    TelegramBotChannel,
    TradeAlert,
    alert_link,
    format_discord,
    format_telegram,
    sticker_summary,
)
from listener_telemetry import alert_latency_seconds, alert_records_total, alerts_deduplicated_total, alerts_total
from models import MarketTick, TickKind
from scrapers.waxpeer import listing_tick

BOT_TOKEN = "123456:secret-bot-token"
WEBHOOK_SECRET = "secret-webhook-token"
RECORD_HEADERS = {"X-Backend-Api-Key": "listener-test-key-that-is-at-least-32-characters"}


def _skinport_listing(**fields: Any) -> MarketTick:
    values: dict[str, Any] = {
        "venue": "skinport",
        "kind": TickKind.LISTED,
        "market_hash_name": "★ Karambit | Doppler (Phase 2) (Factory New)",
        "price_usd": 1234.56,
        "timestamp": 1_790_000_000,
        "event_type": "listed",
        "listing_id": "58903454",
        "float_value": 0.0123456789,
        "pattern": 412,
        "stickers": [{"name": "Crown (Foil)"}, {"name": "Crown (Foil)"}, {"name": "Titan (Holo) | Katowice 2014"}],
        "listing_url": "https://skinport.com/item/karambit-doppler-factory-new-phase-2",
    }
    values.update(fields)
    return MarketTick(**values)


def _snapshot(price_usd: float = 10.0, name: str = "AK-47 | Slate (Field-Tested)") -> MarketTick:
    return MarketTick(
        venue="skinport", kind=TickKind.REST_SNAPSHOT, market_hash_name=name, price_usd=price_usd, timestamp=1_790_000_000
    )


def _alert(tick: MarketTick | None = None, margin: int | None = 15_000, **fields: Any) -> TradeAlert:
    alert = TradeAlert.from_tick(
        tick or _skinport_listing(),
        estimated_net_margin_cents=margin,
        approval_reason="support_floor",
        z_source="macro",
        z_score=-3.214,
    )
    return replace(alert, **fields)


# --- Formatting -------------------------------------------------------------------------------------------------------


def test_skinport_listing_alert_carries_everything_needed_to_buy():
    text = format_telegram(_alert())

    assert text.splitlines() == [
        "Buy signal: Skinport listing",
        "★ Karambit | Doppler (Phase 2) (Factory New)",
        "Price: $1,234.56",
        "Est. net margin: $150.00 (12.2% of price, after fees, resold on Skinport)",
        "Float: 0.012346 | Pattern: 412",
        "Stickers 3: 2x Crown (Foil), Titan (Holo) | Katowice 2014",
        "Decision: rules, rule support_floor, Z -3.21 (macro)",
        '<a href="https://skinport.com/item/karambit-doppler-factory-new-phase-2">Open listing</a>',
    ]


def test_discord_alert_escapes_markdown_and_masks_the_link():
    tick = _skinport_listing(market_hash_name="Sticker | s1mple_ (Gold) *rare*", stickers=[], pattern=None, float_value=None)

    text = format_discord(_alert(tick))

    lines = text.splitlines()
    assert lines[1] == r"Sticker \| s1mple\_ \(Gold\) \*rare\*"
    assert lines[-1] == "[Open listing](https://skinport.com/item/karambit-doppler-factory-new-phase-2)"
    assert not any(line.startswith("Float") or line.startswith("Stickers") for line in lines)


def test_telegram_alert_escapes_html():
    tick = _skinport_listing(market_hash_name="Sticker | <b>Bold</b> & Co", listing_url="https://example.test/?a=1&b=2")

    text = format_telegram(_alert(tick))

    assert "Sticker | &lt;b&gt;Bold&lt;/b&gt; &amp; Co" in text
    assert '<a href="https://example.test/?a=1&amp;b=2">Open listing</a>' in text


def test_waxpeer_alert_links_the_listing_and_is_priced_for_resale_on_skinport():
    payload = {
        "name": "★ StatTrak™ Survival Knife | Doppler Phase 1 (Factory New)",
        "item_id": "54185950016",
        "price": 175_220,
        "phase": "Phase 1",
        "float": 0.0105,
        "sticker_names": None,
    }
    tick = listing_tick(payload, 1_790_000_000_000)
    assert tick is not None

    alert = _alert(tick, margin=-1_250)
    lines = format_telegram(alert).splitlines()

    assert lines[0] == "Buy signal: Waxpeer listing"
    assert lines[3] == "Est. net margin: -$12.50 (-7.1% of price, after fees, resold on Skinport)"
    assert lines[4] == "Float: 0.010500"  # Waxpeer sends no paint seed
    assert alert_link(alert) is not None
    assert lines[-1] == (
        '<a href="https://waxpeer.com/stattrak-survival-knife-doppler-phase-1-factory-new/item?id=54185950016">Open listing</a>'
    )


def test_rest_snapshot_alert_links_a_market_search_and_shows_a_missing_estimate():
    alert = _alert(_snapshot(name="AK-47 | Slate (Field-Tested)"), margin=None)

    lines = format_discord(alert).splitlines()

    assert lines[0] == "Buy signal: Skinport lowest ask"
    assert lines[3] == r"Est. net margin: unknown \(no resale price in the baseline\)"
    assert lines[-1] == "[Search Skinport](https://skinport.com/market?search=AK-47%20%7C%20Slate%20%28Field-Tested%29)"


def test_alert_without_any_link_says_nothing_about_one():
    alert = _alert(_snapshot(), venue="unlisted-venue")

    assert alert_link(alert) is None
    assert "Open listing" not in format_telegram(alert)
    assert format_discord(alert).splitlines()[-1].startswith("Decision:")


def test_sticker_summary_counts_repeats_and_names_only_the_first_few():
    assert sticker_summary([]) is None
    names = ["A", "A", "B", "C", "D", "E", "F", "G"]
    assert sticker_summary(names) == "8: 2x A, B, C, D, E and 2 more"


# --- Rate limiting -----------------------------------------------------------------------------------------------------


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.mark.asyncio
async def test_rate_limiter_allows_a_number_of_sends_per_minute():
    clock = FakeClock()
    limiter = RateLimiter(2, clock=clock, sleep=clock.sleep)

    assert await limiter.acquire(max_wait_seconds=60)
    clock.now += 10
    assert await limiter.acquire(max_wait_seconds=60)
    assert await limiter.acquire(max_wait_seconds=60)

    # The third send waited until the first left the 60 second window.
    assert clock.slept == [50.0]
    assert clock.now == 1060.0


@pytest.mark.asyncio
async def test_rate_limiter_gives_up_without_waiting_when_the_wait_is_too_long():
    clock = FakeClock()
    limiter = RateLimiter(5, clock=clock, sleep=clock.sleep)

    limiter.pause(120)
    assert not await limiter.acquire(max_wait_seconds=60)
    assert clock.slept == []

    clock.now += 100
    assert await limiter.acquire(max_wait_seconds=60)
    assert clock.slept == [20.0]


@pytest.mark.parametrize(
    ("body", "headers", "expected"),
    [
        ({"retry_after": 1.5}, {}, 1.5),
        ({"retry_after": "soon"}, {"Retry-After": "3"}, 3.0),
        ({"message": "You are being rate limited."}, {}, None),
        (None, {"Retry-After": "2"}, 2.0),
        ({"retry_after": True}, {"Retry-After": "-1"}, None),
    ],
    ids=["body", "unusable_body_falls_back_to_header", "neither", "header_only", "bool_and_negative"],
)
def test_discord_retry_after_reads_the_body_then_the_header(body, headers, expected):
    assert DiscordWebhookChannel("https://discord.test/webhooks/1/x").retry_after_seconds(body, headers) == expected


@pytest.mark.parametrize(
    ("body", "headers", "expected"),
    [
        ({"ok": False, "parameters": {"retry_after": 7}}, {}, 7.0),
        ({"ok": False, "parameters": {"migrate_to_chat_id": 1}}, {"Retry-After": "4"}, 4.0),
        ({"ok": False}, {"Retry-After": "5"}, 5.0),
        ({"ok": False, "parameters": {"retry_after": [1]}}, {}, None),
    ],
    ids=["parameters", "parameters_without_retry_after", "no_parameters", "unusable"],
)
def test_telegram_retry_after_reads_parameters_then_the_header(body, headers, expected):
    assert TelegramBotChannel(BOT_TOKEN, "1").retry_after_seconds(body, headers) == expected


# --- Dispatcher against fake providers --------------------------------------------------------------------------------


@dataclass
class FakeProviders:
    """Discord, Telegram and the backend's alert ingest on one local server; replies are scripted per path."""

    url: str = ""
    requests: list[tuple[str, dict[str, Any], dict[str, str]]] = field(default_factory=list)
    replies: dict[str, list[web.Response]] = field(default_factory=dict)
    hold: asyncio.Event | None = None

    def paths(self, prefix: str) -> list[dict[str, Any]]:
        return [body for path, body, _ in self.requests if path.startswith(prefix)]

    async def handle(self, request: web.Request) -> web.Response:
        self.requests.append((request.path, await request.json(), dict(request.headers)))
        if self.hold is not None:
            await self.hold.wait()
        for prefix, scripted in self.replies.items():
            if request.path.startswith(prefix) and scripted:
                return scripted.pop(0)
        if request.path.startswith("/webhooks"):
            return web.Response(status=204)
        if request.path.startswith("/bot"):
            return web.json_response({"ok": True, "result": {"message_id": 1}})
        return web.json_response({"status": "SUCCESS"}, status=201)

    def discord(self) -> DiscordWebhookChannel:
        return DiscordWebhookChannel(f"{self.url}/webhooks/1/{WEBHOOK_SECRET}")

    def telegram(self) -> TelegramBotChannel:
        return TelegramBotChannel(BOT_TOKEN, "-100200300", api_url=self.url)

    def dispatcher(self, channels: list, **options: Any) -> AlertDispatcher:
        options.setdefault("record_url", f"{self.url}/api/v1/ingest/alert")
        options.setdefault("record_headers", RECORD_HEADERS)
        return AlertDispatcher(channels, **options)


@pytest_asyncio.fixture
async def providers():
    fake = FakeProviders()
    app = web.Application()
    app.router.add_post("/{tail:.*}", fake.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    fake.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
    yield fake
    if fake.hold is not None:
        fake.hold.set()
    await runner.cleanup()


def _count(channel: str, outcome: str) -> float:
    return alerts_total.labels(channel=channel, outcome=outcome)._value.get()


def _latency_count(channel: str) -> float:
    for metric in alert_latency_seconds.collect():
        for sample in metric.samples:
            if sample.name.endswith("_count") and sample.labels.get("channel") == channel:
                return sample.value
    return 0.0


@pytest.mark.asyncio
async def test_alert_reaches_both_channels_and_every_attempt_is_recorded(providers):
    sent_before = _count("discord", OUTCOME_SENT), _count("telegram", OUTCOME_SENT)
    latency_before = _latency_count("discord")

    async with providers.dispatcher([providers.discord(), providers.telegram()]) as dispatcher:
        dispatcher.submit(_alert())

    discord_body = providers.paths("/webhooks")[0]
    assert discord_body["allowed_mentions"] == {"parse": []}
    assert "Open listing" in discord_body["content"]
    telegram_body = providers.paths(f"/bot{BOT_TOKEN}/sendMessage")[0]
    assert telegram_body["chat_id"] == "-100200300"
    assert telegram_body["parse_mode"] == "HTML"

    records = providers.paths("/api/v1/ingest/alert")
    assert sorted(record["channel"] for record in records) == ["discord", "telegram"]
    record = records[0]
    assert record["venue"] == "skinport"
    assert record["listing_id"] == "58903454"
    assert record["price_cents"] == 123_456
    assert record["estimated_net_margin_cents"] == 15_000
    assert record["decision_source"] == "rules"
    assert record["approval_reason"] == "support_floor"
    assert record["z_source"] == "macro"
    assert record["outcome"] == OUTCOME_SENT
    assert record["received_at"] == 1_790_000_000
    assert record["latency_ms"] >= 0

    # The backend key goes to the backend only, never to a provider.
    for path, _body, headers in providers.requests:
        assert ("X-Backend-Api-Key" in headers) == path.startswith("/api/")

    assert _count("discord", OUTCOME_SENT) == sent_before[0] + 1
    assert _count("telegram", OUTCOME_SENT) == sent_before[1] + 1
    assert _latency_count("discord") == latency_before + 1


@pytest.mark.asyncio
async def test_a_listing_is_alerted_once_and_its_rest_snapshot_that_day_too(providers):
    duplicates_before = alerts_deduplicated_total._value.get()
    listing = _skinport_listing(market_hash_name="AK-47 | Slate (Field-Tested)", price_usd=10.0)

    async with providers.dispatcher([providers.discord()]) as dispatcher:
        dispatcher.submit(_alert(listing))
        dispatcher.submit(_alert(listing))  # The feed sent the same listing again
        dispatcher.submit(_alert(_snapshot(10.0)))  # The next poll shows it as the lowest ask
        dispatcher.submit(_alert(_snapshot(9.0)))  # A new price is a new alert

    assert len(providers.paths("/webhooks")) == 2
    assert alerts_deduplicated_total._value.get() == duplicates_before + 2


@pytest.mark.asyncio
async def test_discord_429_is_waited_out_and_retried(providers):
    providers.replies["/webhooks"] = [web.json_response({"retry_after": 0.05, "global": False}, status=429)]

    async with providers.dispatcher([providers.discord()]) as dispatcher:
        dispatcher.submit(_alert())

    assert len(providers.paths("/webhooks")) == 2
    assert providers.paths("/api/")[0]["outcome"] == OUTCOME_SENT


@pytest.mark.asyncio
async def test_telegram_429_reads_retry_after_from_parameters(providers):
    clock = FakeClock()
    too_many = {"ok": False, "error_code": 429, "description": "Too Many Requests", "parameters": {"retry_after": 7}}
    providers.replies["/bot"] = [web.json_response(too_many, status=429)]

    async with providers.dispatcher([providers.telegram()], clock=clock, sleep=clock.sleep) as dispatcher:
        dispatcher.submit(_alert())

    assert clock.slept == [7.0]
    assert len(providers.paths("/bot")) == 2
    assert providers.paths("/api/")[0]["outcome"] == OUTCOME_SENT


@pytest.mark.asyncio
async def test_alert_gives_up_after_repeated_429s(providers):
    clock = FakeClock()
    providers.replies["/webhooks"] = [
        web.json_response({"retry_after": 1}, status=429),
        web.Response(status=429, headers={"Retry-After": "2"}),
    ]
    before = _count("discord", OUTCOME_RATE_LIMITED)

    async with providers.dispatcher([providers.discord()], max_attempts=2, clock=clock, sleep=clock.sleep) as dispatcher:
        dispatcher.submit(_alert())

    assert clock.slept == [1.0]
    assert len(providers.paths("/webhooks")) == 2
    assert providers.paths("/api/")[0]["outcome"] == OUTCOME_RATE_LIMITED
    assert _count("discord", OUTCOME_RATE_LIMITED) == before + 1


@pytest.mark.asyncio
async def test_a_long_429_wait_fails_the_alert_and_holds_the_channel(providers):
    clock = FakeClock()
    providers.replies["/bot"] = [web.json_response({"ok": False, "parameters": {"retry_after": 3600}}, status=429)]

    async with providers.dispatcher([providers.telegram()], clock=clock, sleep=clock.sleep) as dispatcher:
        dispatcher.submit(_alert())
        dispatcher.submit(_alert(_snapshot()))

    # One request: the first alert gave up instead of waiting an hour, and the second did not try.
    assert len(providers.paths("/bot")) == 1
    assert [record["outcome"] for record in providers.paths("/api/")] == [OUTCOME_RATE_LIMITED, OUTCOME_RATE_LIMITED]
    assert clock.slept == []


@pytest.mark.asyncio
async def test_provider_errors_fail_the_alert_without_retrying(providers):
    # An error page that is not JSON is read as no reply body.
    providers.replies["/webhooks"] = [web.Response(status=500, text="<html>oops</html>", content_type="text/html")]
    providers.replies["/bot"] = [web.json_response({"ok": False, "description": "Bad Request: chat not found"}, status=400)]
    failed_before = _count("discord", OUTCOME_FAILED), _count("telegram", OUTCOME_FAILED)

    async with providers.dispatcher([providers.discord(), providers.telegram()]) as dispatcher:
        dispatcher.submit(_alert())

    assert len(providers.paths("/webhooks")) == 1
    assert len(providers.paths("/bot")) == 1
    assert sorted(record["outcome"] for record in providers.paths("/api/")) == [OUTCOME_FAILED, OUTCOME_FAILED]
    assert _count("discord", OUTCOME_FAILED) == failed_before[0] + 1
    assert _count("telegram", OUTCOME_FAILED) == failed_before[1] + 1


@pytest.mark.asyncio
async def test_telegram_reply_that_is_not_ok_is_a_failure(providers):
    providers.replies["/bot"] = [web.json_response({"ok": False}, status=200)]

    async with providers.dispatcher([providers.telegram()]) as dispatcher:
        dispatcher.submit(_alert())

    assert providers.paths("/api/")[0]["outcome"] == OUTCOME_FAILED


@pytest.mark.asyncio
async def test_unreachable_provider_fails_the_alert_and_logs_no_secret(providers, caplog):
    unreachable = TelegramBotChannel(BOT_TOKEN, "-100200300", api_url="http://127.0.0.1:1")
    unreachable_discord = DiscordWebhookChannel(f"http://127.0.0.1:1/webhooks/1/{WEBHOOK_SECRET}")

    with caplog.at_level(logging.INFO):
        async with providers.dispatcher([unreachable, unreachable_discord]) as dispatcher:
            dispatcher.submit(_alert())

    assert sorted(record["outcome"] for record in providers.paths("/api/")) == [OUTCOME_FAILED, OUTCOME_FAILED]
    assert "request failed: ClientConnectorError" in caplog.text
    assert BOT_TOKEN not in caplog.text
    assert WEBHOOK_SECRET not in caplog.text


@pytest.mark.asyncio
async def test_a_full_channel_queue_drops_the_alert_instead_of_waiting(providers):
    providers.hold = asyncio.Event()
    dropped_before = _count("discord", OUTCOME_DROPPED)

    async with providers.dispatcher([providers.discord()], queue_size=1, shutdown_timeout=5) as dispatcher:
        dispatcher.submit(_alert(_snapshot(10.0)))
        dispatcher.submit(_alert(_snapshot(9.0)))  # The queue still holds the first alert
        providers.hold.set()

    assert _count("discord", OUTCOME_DROPPED) == dropped_before + 1
    assert len(providers.paths("/webhooks")) == 1


@pytest.mark.asyncio
async def test_backend_failure_to_record_does_not_affect_sending(providers):
    providers.replies["/api/"] = [web.json_response({"detail": "error"}, status=500)]
    rejected_before = alert_records_total.labels(outcome="rejected")._value.get()

    async with providers.dispatcher([providers.discord()]) as dispatcher:
        dispatcher.submit(_alert())

    assert len(providers.paths("/webhooks")) == 1
    assert alert_records_total.labels(outcome="rejected")._value.get() == rejected_before + 1


@pytest.mark.asyncio
async def test_unreachable_backend_is_counted_as_a_record_error(providers):
    errors_before = alert_records_total.labels(outcome="error")._value.get()

    dispatcher = providers.dispatcher([providers.discord()], record_url="http://127.0.0.1:1/api/v1/ingest/alert")
    async with dispatcher:
        dispatcher.submit(_alert())

    assert len(providers.paths("/webhooks")) == 1
    assert alert_records_total.labels(outcome="error")._value.get() == errors_before + 1


@pytest.mark.asyncio
async def test_dispatcher_without_channels_ignores_alerts():
    async with AlertDispatcher([]) as dispatcher:
        dispatcher.submit(_alert())


@pytest.mark.asyncio
async def test_submit_after_close_drops_the_alert(providers):
    dispatcher = providers.dispatcher([providers.discord()])
    async with dispatcher:
        pass
    dropped_before = _count("discord", OUTCOME_DROPPED)

    dispatcher.submit(_alert())

    assert _count("discord", OUTCOME_DROPPED) == dropped_before + 1
    assert providers.paths("/webhooks") == []


# --- Configuration ----------------------------------------------------------------------------------------------------


@pytest.fixture
def alert_env(monkeypatch):
    for name in (
        "DISCORD_WEBHOOK_URL",
        "DISCORD_ALERTS_ENABLED",
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_ALERTS_ENABLED",
        "ALERT_MAX_PER_MINUTE",
    ):
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_no_credentials_means_no_channels(alert_env):
    assert AlertDispatcher.from_env("http://backend:8080", shutdown_timeout=5).channels == []


def test_channels_are_configured_from_the_environment(alert_env):
    alert_env.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/token")
    alert_env.setenv("TELEGRAM_BOT_TOKEN", BOT_TOKEN)
    alert_env.setenv("TELEGRAM_CHAT_ID", "42")

    dispatcher = AlertDispatcher.from_env("http://backend:8080/", shutdown_timeout=5)

    assert [channel.name for channel in dispatcher.channels] == ["discord", "telegram"]
    assert dispatcher._record_url == "http://backend:8080/api/v1/ingest/alert"


@pytest.mark.parametrize("disabled", ["DISCORD_ALERTS_ENABLED", "TELEGRAM_ALERTS_ENABLED"])
def test_a_channel_can_be_turned_off_without_removing_its_credentials(alert_env, disabled):
    alert_env.setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/1/token")
    alert_env.setenv("TELEGRAM_BOT_TOKEN", BOT_TOKEN)
    alert_env.setenv("TELEGRAM_CHAT_ID", "42")
    alert_env.setenv(disabled, "false")

    names = [channel.name for channel in AlertDispatcher.from_env("http://backend:8080", shutdown_timeout=5).channels]

    assert names == (["telegram"] if disabled == "DISCORD_ALERTS_ENABLED" else ["discord"])


def test_telegram_needs_a_chat_id(alert_env, caplog):
    alert_env.setenv("TELEGRAM_BOT_TOKEN", BOT_TOKEN)

    with caplog.at_level(logging.WARNING):
        assert AlertDispatcher.from_env("http://backend:8080", shutdown_timeout=5).channels == []
    assert "TELEGRAM_CHAT_ID" in caplog.text
    assert BOT_TOKEN not in caplog.text


def test_invalid_rate_is_refused(alert_env):
    alert_env.setenv("ALERT_MAX_PER_MINUTE", "0")

    with pytest.raises(ValueError, match="ALERT_MAX_PER_MINUTE"):
        AlertDispatcher.from_env("http://backend:8080", shutdown_timeout=5)


@pytest.mark.asyncio
async def test_dispatcher_without_a_record_url_only_sends(providers):
    async with providers.dispatcher([providers.discord()], record_url=None) as dispatcher:
        dispatcher.submit(_alert())

    assert len(providers.paths("/webhooks")) == 1
    assert providers.paths("/api/") == []


@pytest.mark.asyncio
async def test_alert_memory_forgets_the_oldest_listing_first(providers, monkeypatch):
    monkeypatch.setattr(alerts_module, "ALERTED_MEMORY_SIZE", 2)

    async with providers.dispatcher([providers.discord()], record_url=None) as dispatcher:
        for listing_id in ("1", "2", "1"):
            dispatcher.submit(_alert(_skinport_listing(listing_id=listing_id)))

    # A listing holds two keys (listing and snapshot), so the memory of two forgot listing 1 when 2 came.
    assert len(providers.paths("/webhooks")) == 3


@pytest.mark.asyncio
async def test_sending_before_the_dispatcher_runs_is_an_error(providers):
    dispatcher = providers.dispatcher([providers.discord()])

    with pytest.raises(RuntimeError, match="not running"):
        await dispatcher._send(providers.discord(), _alert())
