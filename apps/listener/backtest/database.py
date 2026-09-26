"""
Replay input from PostgreSQL: recorded feed events, REST snapshots, and the dated baseline builds.

Rows are streamed with server-side cursors, so a range of days does not have to fit in memory.
"""

from collections.abc import AsyncIterator
from datetime import datetime, timedelta

from backtest.sources import (
    BaselineSchedule,
    BaselineSnapshot,
    PollClock,
    RecordedEvent,
    RecordedFeedEvent,
    RecordedSnapshot,
    ScheduledBuild,
    merge_ordered,
)
from shared_utils import applied_sticker_name, edge_baseline_payload
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

STREAM_CHUNK_ROWS = 5_000

# Exact duplicate rows (same type, receive time, and payload) can only come from storing one received
# event twice, so they are dropped. Re-sent events with a new receive time are kept: live saw them too.
_FEED_EVENTS_QUERY = text(
    """
    SELECT received_at, payload
    FROM (
        SELECT DISTINCT ON (event_type, received_at, md5(payload::text)) id, received_at, payload
        FROM feed_events
        WHERE source = :source AND received_at >= :start AND received_at < :end
        ORDER BY event_type, received_at, md5(payload::text), id
    ) AS unique_events
    ORDER BY received_at, id
    """
)

# REST snapshots are the ticks without a feed event type.
_SNAPSHOTS_QUERY = text(
    """
    SELECT tick.inserted_at, item.market_hash_name, tick.price_cents
    FROM live_market_ticks AS tick
    JOIN market_items AS item ON item.id = tick.item_id
    WHERE tick.marketplace_source = :source
      AND tick.event_type IS NULL
      AND tick.inserted_at >= :start
      AND tick.inserted_at < :end
    ORDER BY tick.inserted_at, tick.id
    """
)

# Builds that can be in effect during [start, end): the newest one built at or before the start, and
# every one built after it and before the end.
_BUILDS_QUERY = text(
    """
    SELECT id, built_at
    FROM baseline_builds
    WHERE venue = :venue
      AND built_at < :end
      AND built_at >= COALESCE(
          (SELECT MAX(built_at) FROM baseline_builds WHERE venue = :venue AND built_at <= :start),
          :start
      )
    ORDER BY built_at, id
    """
)

_BUILD_ROWS_QUERY = text(
    """
    SELECT market_hash_name, support_floor_cents, latest_price_cents, rolling_30d_avg_cents, volatility_cents,
           drift_percent
    FROM venue_baselines
    WHERE build_id = :build_id
    ORDER BY market_hash_name
    """
)


_EPOCH = datetime(1970, 1, 1)
_ONE_MILLISECOND = timedelta(milliseconds=1)


def _epoch_ms(naive_utc: datetime) -> int:
    """Epoch milliseconds of a stored timestamp (naive UTC), without float rounding."""
    return (naive_utc - _EPOCH) // _ONE_MILLISECOND


def _default_engine() -> AsyncEngine:
    # Imported lazily: the module builds its engine from DATABASE_URL at import time.
    from shared_utils.db_connection import async_engine

    return async_engine


async def stream_feed_events(start: datetime, end: datetime, source: str, engine: AsyncEngine) -> AsyncIterator[RecordedEvent]:
    async with engine.connect() as connection:
        result = await connection.stream(
            _FEED_EVENTS_QUERY.execution_options(yield_per=STREAM_CHUNK_ROWS),
            {"source": source, "start": start, "end": end},
        )
        async for received_at, payload in result:
            yield RecordedFeedEvent(received_at_ms=_epoch_ms(received_at), payload=payload)


async def stream_snapshots(start: datetime, end: datetime, source: str, engine: AsyncEngine) -> AsyncIterator[RecordedEvent]:
    async with engine.connect() as connection:
        result = await connection.stream(
            _SNAPSHOTS_QUERY.execution_options(yield_per=STREAM_CHUNK_ROWS),
            {"source": source, "start": start, "end": end},
        )
        clock = PollClock()
        async for inserted_at, market_hash_name, price_cents in result:
            yield RecordedSnapshot(
                observed_at_ms=clock.stamp(_epoch_ms(inserted_at)),
                market_hash_name=market_hash_name,
                price_cents=price_cents,
            )


async def stream_recorded_events(
    start: datetime,
    end: datetime,
    *,
    source: str = "skinport",
    engine: AsyncEngine | None = None,
) -> AsyncIterator[RecordedEvent]:
    """Feed events and REST snapshots in [start, end) (naive UTC), merged into replay order."""
    engine = engine or _default_engine()
    async for event in merge_ordered(
        stream_feed_events(start, end, source, engine),
        stream_snapshots(start, end, source, engine),
    ):
        yield event


async def load_build_snapshot(build_id: int, built_at: str, engine: AsyncEngine | None = None) -> BaselineSnapshot:
    """
    One stored build, shaped exactly as the backend serves it and the listener loads it into the edge
    Redis (shared_utils.edge_baseline_payload, sticker prices keyed by applied sticker name).
    """
    engine = engine or _default_engine()
    baselines: dict[str, dict] = {}
    sticker_prices: dict[str, int] = {}
    async with engine.connect() as connection:
        result = await connection.execute(_BUILD_ROWS_QUERY, {"build_id": build_id})
        for name, support_floor, latest_price, rolling_30d_avg, volatility, drift in result:
            baselines[name] = edge_baseline_payload(
                support_floor_cents=support_floor,
                latest_price_cents=latest_price,
                rolling_30d_avg_cents=rolling_30d_avg,
                volatility_cents=volatility,
                drift_percent=drift,
            )
            sticker_name = applied_sticker_name(name)
            if sticker_name is not None:
                sticker_prices[sticker_name] = latest_price
    return BaselineSnapshot(baselines=baselines, sticker_prices=sticker_prices, as_of=built_at)


async def load_baseline_schedule(
    start: datetime, end: datetime, *, venue: str = "skinport", engine: AsyncEngine | None = None
) -> BaselineSchedule:
    """The builds in effect during [start, end) (naive UTC). Each build's rows load when replay reaches it."""
    engine = engine or _default_engine()
    async with engine.connect() as connection:
        result = await connection.execute(_BUILDS_QUERY, {"venue": venue, "start": start, "end": end})
        builds = [
            ScheduledBuild(build_id=build_id, built_at=built_at.isoformat(), effective_from_ms=_epoch_ms(built_at))
            for build_id, built_at in result
        ]
    built_at_by_id = {build.build_id: build.built_at for build in builds}

    async def load(build_id: int) -> BaselineSnapshot:
        return await load_build_snapshot(build_id, built_at_by_id[build_id], engine)

    return BaselineSchedule(builds=builds, load=load)
