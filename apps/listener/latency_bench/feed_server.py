"""
A local stand-in for Skinport's Socket.IO sale feed (Engine.IO v4 over a websocket, Socket.IO packets encoded
with MessagePack as socket.io-msgpack-parser does). It runs in its own process, so its work never lands on the
listener's event loop, and it encodes every frame before play starts.

It answers the client's CONNECT, notes the `saleFeedJoin`, and on command plays the scheduled `saleFeed`
events, stamping each one with time.perf_counter_ns() just before it is written to the socket.

The orchestrator drives it over stdin and stdout, one command or reply per line:
    -> READY <port>       listening
    -> JOINED             the client sent saleFeedJoin
    <- CLOCK              -> CLOCK <ns>   (clock comparison with the orchestrator)
    <- WARMUP             -> WARMED       (every warm-up event written)
    <- GO                 -> PLAYED       (every measured event written)
    <- STOP               -> DONE         (emit times written to --emit-out)
"""

import asyncio
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import msgpack
from aiohttp import WSMsgType, web

SOCKET_PATH = "/socket.io/"
SESSION_ID = "latency-bench"
# The Engine.IO open packet. The client expects a ping within pingInterval + pingTimeout.
PING_INTERVAL_MS = 25_000
OPEN_PACKET = "0" + json.dumps(
    {"sid": SESSION_ID, "upgrades": [], "pingInterval": PING_INTERVAL_MS, "pingTimeout": 20_000, "maxPayload": 100_000_000}
)
ENGINE_PING = "2"

# Socket.IO packet types (https://github.com/socketio/socket.io-protocol).
CONNECT = 0
EVENT = 2

# Play starts this long after GO, so the first frame is not written while the GO reply is still in flight.
PLAY_LEAD_NS = 50_000_000


def encode_packet(packet_type: int, data: Any) -> bytes:
    return msgpack.packb({"type": packet_type, "nsp": "/", "data": data})


def encode_event(name: str, *args: Any) -> bytes:
    return encode_packet(EVENT, [name, *args])


def decode_packet(frame: bytes) -> dict[str, Any]:
    packet = msgpack.unpackb(frame)
    if not isinstance(packet, dict) or not isinstance(packet.get("type"), int):
        raise ValueError("not a Socket.IO packet")
    return packet


@dataclass(frozen=True, slots=True)
class PreparedFrame:
    offset_ns: int  # From the start of its phase
    key: str  # The event's first productId
    frame: bytes
    measured: bool


def prepare_frames(schedule: list[dict[str, Any]]) -> list[PreparedFrame]:
    """Encode every scheduled event ({"offset_ms", "measured", "payload"}) as the saleFeed packet Skinport would send."""
    return [
        PreparedFrame(
            offset_ns=round(event["offset_ms"] * 1_000_000),
            key=str(event["payload"]["sales"][0]["productId"]),
            frame=encode_event("saleFeed", event["payload"]),
            measured=bool(event["measured"]),
        )
        for event in schedule
    ]


class FeedServer:
    def __init__(self, frames: list[PreparedFrame]) -> None:
        self._warmup = [frame for frame in frames if not frame.measured]
        self._measured = [frame for frame in frames if frame.measured]
        self._socket: web.WebSocketResponse | None = None
        self.joined = asyncio.Event()
        self.emitted: list[tuple[str, int]] = []

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_get(SOCKET_PATH, self._handle)
        return app

    async def _ping(self, socket: web.WebSocketResponse) -> None:
        while not socket.closed:
            await asyncio.sleep(PING_INTERVAL_MS / 1000)
            await socket.send_str(ENGINE_PING)

    async def _handle(self, request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse(max_msg_size=0)
        await socket.prepare(request)
        await socket.send_str(OPEN_PACKET)
        self._socket = socket
        ping_task = asyncio.create_task(self._ping(socket))
        try:
            async for message in socket:
                if message.type == WSMsgType.BINARY:
                    await self._on_packet(socket, decode_packet(message.data))
                # Text frames are Engine.IO pongs ("3"); nothing to do.
        finally:
            ping_task.cancel()
        return socket

    async def _on_packet(self, socket: web.WebSocketResponse, packet: dict[str, Any]) -> None:
        if packet["type"] == CONNECT:
            await socket.send_bytes(encode_packet(CONNECT, {"sid": SESSION_ID}))
        elif packet["type"] == EVENT and isinstance(packet.get("data"), list) and packet["data"]:
            name = packet["data"][0]
            if name == "saleFeedJoin":
                self.joined.set()

    async def play(self, measured: bool) -> None:
        """
        Write the warm-up or the measured frames, each at its offset from now. A late frame is written at once,
        and the schedule keeps its pace.
        """
        if self._socket is None:
            raise RuntimeError("no client is connected")
        socket = self._socket
        start_ns = time.perf_counter_ns() + PLAY_LEAD_NS
        for frame in self._measured if measured else self._warmup:
            delay_ns = start_ns + frame.offset_ns - time.perf_counter_ns()
            if delay_ns > 0:
                await asyncio.sleep(delay_ns / 1e9)
            emitted_ns = time.perf_counter_ns()
            await socket.send_bytes(frame.frame)
            self.emitted.append((frame.key, emitted_ns))


async def _read_command() -> str:
    line = await asyncio.to_thread(sys.stdin.readline)
    return line.strip()


def _reply(line: str) -> None:
    print(line, flush=True)


async def serve(schedule_path: Path, emit_out: Path, port: int) -> None:
    schedule = [json.loads(line) for line in schedule_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    server = FeedServer(prepare_frames(schedule))
    runner = web.AppRunner(server.app())
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    bound_port = runner.addresses[0][1]
    _reply(f"READY {bound_port}")
    try:
        await server.joined.wait()
        _reply("JOINED")
        while True:
            command = await _read_command()
            if command == "CLOCK":
                _reply(f"CLOCK {time.perf_counter_ns()}")
            elif command == "WARMUP":
                await server.play(measured=False)
                _reply("WARMED")
            elif command == "GO":
                await server.play(measured=True)
                _reply("PLAYED")
            elif command in ("STOP", ""):
                break
        emit_out.write_text(json.dumps({"emitted": server.emitted}), encoding="utf-8")
        _reply("DONE")
    finally:
        await runner.cleanup()
