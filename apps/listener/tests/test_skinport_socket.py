"""Skinport's sale feed read directly over a websocket (scrapers/skinport_socket.py, SkinportScraper)."""

import logging
import struct

import aiohttp
import msgpack
import pytest
from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestServer
from listener_telemetry import feed_connected, feed_reconnects_total
from models import FeedEvent, MarketTick
from scrapers.base import FeedConnectionLost
from scrapers.skinport import SkinportScraper
from scrapers.skinport_socket import (
    CONNECT,
    CONNECT_ERROR,
    DEFAULT_RECEIVE_TIMEOUT_SECONDS,
    DISCONNECT,
    EVENT,
    JOIN_EVENT,
    JOIN_PARAMETERS,
    decode_packet,
    encode_packet,
    feed_headers,
    receive_timeout,
    sale_feed_events,
    socket_url,
)

OPEN_FRAME = '0{"sid":"s","upgrades":[],"pingInterval":15000,"pingTimeout":30000,"maxPayload":1000000}'
LISTED = {"eventType": "listed", "sales": [{"marketHashName": "AK-47 | Redline", "salePrice": 1550, "productId": 7}]}


def _text(data: str) -> aiohttp.WSMessage:
    return aiohttp.WSMessage(WSMsgType.TEXT, data, None)


def _binary(data: bytes) -> aiohttp.WSMessage:
    return aiohttp.WSMessage(WSMsgType.BINARY, data, None)


def _event(name: str, *args) -> aiohttp.WSMessage:
    return _binary(encode_packet(EVENT, [name, *args]))


class FakeSocket:
    """Hands out scripted frames (or raises scripted errors) and keeps what the client sent."""

    def __init__(self, messages) -> None:
        self._messages = list(messages)
        self.sent: list = []
        self.timeouts: list = []

    async def receive(self, timeout=None):
        self.timeouts.append(timeout)
        message = self._messages.pop(0)
        if isinstance(message, BaseException):
            raise message
        return message

    async def send_str(self, data: str) -> None:
        self.sent.append(data)

    async def send_bytes(self, data: bytes) -> None:
        self.sent.append(decode_packet(data))

    def exception(self) -> Exception:
        return RuntimeError("reset by peer")


async def _read(socket, debug=False):
    payloads = []
    with pytest.raises(FeedConnectionLost) as lost:
        async for payload, received_at_ms in sale_feed_events(socket, debug):
            assert received_at_ms > 1_700_000_000_000
            payloads.append(payload)
    return payloads, str(lost.value)


def test_socket_url_and_headers():
    assert socket_url("https://skinport.com") == "wss://skinport.com/socket.io/?EIO=4&transport=websocket"
    assert socket_url("http://127.0.0.1:8123/") == "ws://127.0.0.1:8123/socket.io/?EIO=4&transport=websocket"
    assert "Cookie" not in feed_headers("")
    headers = feed_headers("cf_clearance=abc")
    assert headers["Cookie"] == "cf_clearance=abc"
    assert headers["Origin"] == "https://skinport.com"


def test_packets_and_notepack_extensions():
    assert decode_packet(encode_packet(CONNECT)) == {"type": CONNECT, "nsp": "/"}
    assert decode_packet(encode_packet(EVENT, ["saleFeed", {}]))["data"] == ["saleFeed", {}]
    # notepack sends JavaScript undefined and Date as extension 0; JSON.stringify would drop or stringify them.
    epoch_ms = struct.pack(">d", 1_790_000_000_123.0)
    payload = {"a": msgpack.ExtType(0, b"\x00"), "b": msgpack.ExtType(0, epoch_ms), "c": msgpack.ExtType(9, b"x")}
    frame = msgpack.packb({"type": EVENT, "data": ["saleFeed", payload]})
    assert decode_packet(frame)["data"][1] == {"a": None, "b": "2026-09-21T14:13:20.123Z", "c": None}
    for frame in (b"\xc1", msgpack.packb([1, 2]), msgpack.packb({"data": 1})):
        with pytest.raises(ValueError):
            decode_packet(frame)


def test_receive_timeout_comes_from_the_open_packet():
    assert receive_timeout(OPEN_FRAME) == 45.0
    assert receive_timeout("0{not json") == DEFAULT_RECEIVE_TIMEOUT_SECONDS
    assert receive_timeout('0{"pingInterval": 1}') == DEFAULT_RECEIVE_TIMEOUT_SECONDS


@pytest.mark.asyncio
async def test_handshake_join_ping_and_sale_feed_events(caplog):
    feed_connected.set(0)
    socket = FakeSocket(
        [
            _text(OPEN_FRAME),
            _binary(encode_packet(CONNECT, {"sid": "x"})),
            _text("2"),
            _event("maintenanceUpdated", {"maintenance": False}),
            _event("saleFeed", LISTED),
            _binary(b"\xc1"),  # Not MessagePack: skipped
            _binary(msgpack.packb({"type": EVENT, "data": "not a list"})),
            _event("saleFeed", "not an object"),
            _binary(encode_packet(3)),  # A packet type the client does not handle
            _text("6"),  # Engine.IO noop
            aiohttp.WSMessage(WSMsgType.PING, b"", None),
            aiohttp.WSMessage(WSMsgType.CLOSE, 1000, ""),
        ]
    )
    with caplog.at_level(logging.INFO, logger="listener.skinport"):
        payloads, reason = await _read(socket, debug=True)

    assert payloads == [LISTED]
    assert reason == "the server closed the connection"
    join = {"type": EVENT, "nsp": "/", "data": [JOIN_EVENT, JOIN_PARAMETERS]}
    assert socket.sent == [{"type": CONNECT, "nsp": "/"}, join, "3"]
    assert socket.timeouts[0] == DEFAULT_RECEIVE_TIMEOUT_SECONDS
    assert socket.timeouts[1] == 45.0
    assert feed_connected._value.get() == 1
    assert "Event: maintenanceUpdated" in caplog.text
    assert "not a Socket.IO packet" in caplog.text


@pytest.mark.parametrize(
    ("ending", "reason"),
    [
        (_text("1"), "the server closed the session"),
        (_binary(encode_packet(DISCONNECT)), "the server ended the session"),
        (_binary(encode_packet(CONNECT_ERROR, "banned")), "the server refused the connection: banned"),
        (aiohttp.WSMessage(WSMsgType.ERROR, None, None), "websocket error: reset by peer"),
        (TimeoutError(), "no frame for 45 seconds"),
    ],
)
@pytest.mark.asyncio
async def test_every_way_a_connection_ends_is_a_lost_connection(ending, reason):
    payloads, lost = await _read(FakeSocket([_text(OPEN_FRAME), ending]))
    assert payloads == []
    assert lost == reason


@pytest.mark.asyncio
async def test_scraper_reads_the_feed_from_the_socket(monkeypatch):
    """SkinportScraper against a local server that speaks the protocol, then hangs up."""
    seen_headers = {}

    async def handle(request: web.Request) -> web.WebSocketResponse:
        seen_headers.update(request.headers)
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        await socket.send_str(OPEN_FRAME)
        assert decode_packet(await socket.receive_bytes())["type"] == CONNECT
        await socket.send_bytes(encode_packet(CONNECT, {"sid": "x"}))
        assert decode_packet(await socket.receive_bytes())["data"][0] == JOIN_EVENT
        await socket.send_bytes(encode_packet(EVENT, ["saleFeed", LISTED]))
        await socket.close()
        return socket

    app = web.Application()
    app.router.add_get("/socket.io/", handle)
    async with TestServer(app) as server:
        monkeypatch.setenv("SKINPORT_WS_URL", f"http://127.0.0.1:{server.port}")
        monkeypatch.setenv("CLOUDFLARE_COOKIE", "cf_clearance=abc")
        reconnects = feed_reconnects_total._value.get()
        items = []
        with pytest.raises(FeedConnectionLost, match="closed the connection"):
            async for item in SkinportScraper().listen_websocket_stream():
                items.append(item)

    feed_event, tick = items
    assert isinstance(feed_event, FeedEvent)
    assert feed_event.payload == LISTED
    assert isinstance(tick, MarketTick)
    assert tick.listing_id == "7"
    assert tick.price_usd == 15.5
    assert seen_headers["Cookie"] == "cf_clearance=abc"
    assert feed_connected._value.get() == 0
    assert feed_reconnects_total._value.get() == reconnects + 1


@pytest.mark.asyncio
async def test_scraper_turns_a_refused_connection_into_a_lost_one(monkeypatch, unused_tcp_port):
    monkeypatch.setenv("SKINPORT_WS_URL", f"http://127.0.0.1:{unused_tcp_port}")
    with pytest.raises(FeedConnectionLost):
        async for _ in SkinportScraper().listen_websocket_stream():
            pass
