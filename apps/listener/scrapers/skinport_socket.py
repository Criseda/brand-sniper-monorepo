"""
Skinport's live sale feed, read directly over an aiohttp websocket (#175). It replaced a Node.js sidecar that
relayed the feed through Redis Pub/Sub; docs/benchmarks/ingress_latency.md has the measurements and the decision.

The feed is Socket.IO on Engine.IO v4. Engine.IO packets are text frames; Socket.IO packets are binary frames
encoded in MessagePack, as socket.io-msgpack-parser (notepack) encodes them. A connection goes:

    server  "0{...}"                                   Engine.IO open: ping interval and timeout
    client  {type: 0, nsp: "/"}                        Socket.IO CONNECT
    server  {type: 0, data: {sid}}                     connected
    client  {type: 2, data: ["saleFeedJoin", {...}]}   join the CS2 sale feed in USD
    server  {type: 2, data: ["saleFeed", {...}]}       one per listed or sold event (other events are ignored)
    server  "2", client "3"                            Engine.IO ping and pong

Docs: https://docs.skinport.com/websocket/sale-feed. Observed behaviour: docs/skinport_feed.md.
"""

import json
import struct
import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import aiohttp
import msgpack
from listener_telemetry import feed_connected
from scrapers.base import FeedConnectionLost
from shared_utils import get_logger

logger = get_logger("listener.skinport")

DEFAULT_FEED_URL = "https://skinport.com"
# Points the listener at another feed server; the latency benchmark (#175) sets it to its local stand-in.
FEED_URL_ENV = "SKINPORT_WS_URL"
SOCKET_PATH = "/socket.io/"
SOCKET_QUERY = "EIO=4&transport=websocket"
SALE_FEED_EVENT = "saleFeed"
JOIN_EVENT = "saleFeedJoin"
JOIN_PARAMETERS = {"currency": "USD", "locale": "en", "appid": 730}
# Skinport sits behind Cloudflare, which expects a browser's headers.
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
# Receive timeout before the server's open packet states its ping interval and timeout.
DEFAULT_RECEIVE_TIMEOUT_SECONDS = 60.0

# Engine.IO v4 packet types (text frames).
ENGINE_OPEN = "0"
ENGINE_CLOSE = "1"
ENGINE_PING = "2"
ENGINE_PONG = "3"

# Socket.IO packet types (https://github.com/socketio/socket.io-protocol).
CONNECT = 0
DISCONNECT = 1
EVENT = 2
CONNECT_ERROR = 4

# notepack's MessagePack extensions: JavaScript `undefined` (one byte) and Date (a float64 of epoch milliseconds).
NOTEPACK_EXTENSION = 0
DATE_SIZE = 8


def socket_url(feed_url: str) -> str:
    """The Engine.IO websocket URL of a feed served at `feed_url` (http or https, as socket.io-client takes it)."""
    parts = urlsplit(feed_url)
    scheme = {"http": "ws", "https": "wss"}.get(parts.scheme, parts.scheme)
    return urlunsplit((scheme, parts.netloc, SOCKET_PATH, SOCKET_QUERY, ""))


def feed_headers(cookie: str) -> dict[str, str]:
    headers = {"User-Agent": USER_AGENT, "Origin": "https://skinport.com", "Referer": "https://skinport.com/"}
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _notepack_extension(code: int, data: bytes) -> Any:
    """
    Decodes notepack's extensions to what JSON.stringify would have made of them, so a payload stays plain JSON:
    `undefined` becomes None and a Date its ISO string. Any other extension is dropped to None.
    """
    if code == NOTEPACK_EXTENSION and len(data) == DATE_SIZE:
        (epoch_ms,) = struct.unpack(">d", data)
        return datetime.fromtimestamp(epoch_ms / 1000, UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return None


def encode_packet(packet_type: int, data: Any = None) -> bytes:
    packet: dict[str, Any] = {"type": packet_type, "nsp": "/"}
    if data is not None:
        packet["data"] = data
    return msgpack.packb(packet)


def decode_packet(frame: bytes) -> dict[str, Any]:
    """A Socket.IO packet from a binary frame. Raises ValueError for anything else."""
    try:
        packet = msgpack.unpackb(frame, ext_hook=_notepack_extension)
    except (msgpack.UnpackException, ValueError) as err:
        raise ValueError(f"not a MessagePack frame: {err}") from err
    if not isinstance(packet, dict) or not isinstance(packet.get("type"), int):
        raise ValueError("not a Socket.IO packet")
    return packet


def receive_timeout(open_frame: str) -> float:
    """Seconds without a frame after which the connection is dead: the ping interval plus the ping timeout."""
    try:
        handshake = json.loads(open_frame[len(ENGINE_OPEN) :])
        return (int(handshake["pingInterval"]) + int(handshake["pingTimeout"])) / 1000
    except (ValueError, KeyError, TypeError):
        return DEFAULT_RECEIVE_TIMEOUT_SECONDS


async def sale_feed_events(
    ws: aiohttp.ClientWebSocketResponse, debug: bool = False
) -> AsyncGenerator[tuple[dict[str, Any], int], None]:
    """
    Speaks the protocol on an open websocket and yields every `saleFeed` payload with the epoch milliseconds it
    arrived at. Raises FeedConnectionLost when the connection ends, for whatever reason.
    """
    timeout = DEFAULT_RECEIVE_TIMEOUT_SECONDS
    while True:
        # A server that stops pinging for longer than its ping interval plus timeout has gone away.
        try:
            message = await ws.receive(timeout=timeout)
        except TimeoutError as err:
            raise FeedConnectionLost(f"no frame for {timeout:.0f} seconds") from err
        if message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
            raise FeedConnectionLost("the server closed the connection")
        if message.type == aiohttp.WSMsgType.ERROR:
            raise FeedConnectionLost(f"websocket error: {ws.exception()}")

        if message.type == aiohttp.WSMsgType.TEXT:
            frame: str = message.data
            if frame == ENGINE_PING:
                await ws.send_str(ENGINE_PONG)
            elif frame.startswith(ENGINE_OPEN):
                timeout = receive_timeout(frame)
                await ws.send_bytes(encode_packet(CONNECT))
            elif frame == ENGINE_CLOSE:
                raise FeedConnectionLost("the server closed the session")
            continue
        if message.type != aiohttp.WSMsgType.BINARY:
            continue

        received_at_ms = int(time.time() * 1000)
        try:
            packet = decode_packet(message.data)
        except ValueError as err:
            logger.warning("[SKINPORT WS] Skipping a frame that is not a Socket.IO packet: %s", err)
            continue
        if packet["type"] == CONNECT:
            await ws.send_bytes(encode_packet(EVENT, [JOIN_EVENT, JOIN_PARAMETERS]))
            feed_connected.set(1)
            logger.info("[SKINPORT WS] Connected. Joined the CS2 USD sale feed.")
        elif packet["type"] == EVENT:
            data = packet.get("data")
            if not isinstance(data, list) or not data or not isinstance(data[0], str):
                continue
            name, payload = data[0], data[1] if len(data) > 1 else None
            if debug:
                logger.info("[SKINPORT WS ANY] Event: %s, payload: %s", name, json.dumps(payload, default=str)[:500])
            if name == SALE_FEED_EVENT and isinstance(payload, dict):
                yield payload, received_at_ms
        elif packet["type"] == DISCONNECT:
            raise FeedConnectionLost("the server ended the session")
        elif packet["type"] == CONNECT_ERROR:
            raise FeedConnectionLost(f"the server refused the connection: {packet.get('data')}")
