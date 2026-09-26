"""
Keeps the edge Redis loaded with the newest baseline build for the listener's venue.

The edge Redis lives in RAM only, so it is empty after every restart. The listener therefore loads the
newest build from the backend as soon as it starts, then asks for a newer one every
`BASELINE_REFRESH_SECONDS`. The request carries the loaded build ID, so an unchanged build costs one empty
response. The loaded build ID is read back from Redis each time, so a Redis restart triggers a full
reload by itself.

A load replaces the venue's hashes atomically (fill a staging key, then RENAME it over the live one), so
the DRE never sees half a build and items missing from the new build do not linger.

Without baselines the DRE rejects every anomaly, and with an old build it judges listings against old
prices. Both are logged as errors, exported as metrics, and reported by the health endpoint.
"""

import asyncio
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, cast

import aiohttp
from listener_telemetry import baseline_build_age_seconds, baselines_loaded
from redis.asyncio import Redis
from shared_utils import (
    edge_baseline_meta_key,
    edge_baselines_key,
    edge_sticker_prices_key,
    get_logger,
    utc_now_naive,
)

logger = get_logger("listener.baselines")

BASELINE_REFRESH_SECONDS = float(os.getenv("BASELINE_REFRESH_SECONDS", "900"))
BASELINE_RETRY_SECONDS = float(os.getenv("BASELINE_RETRY_SECONDS", "60"))
# A build older than this is reported as stale. The builder refreshes after 20 hours, so this allows a
# day of missed builds before anything is flagged.
BASELINE_MAX_AGE_HOURS = float(os.getenv("BASELINE_MAX_AGE_HOURS", "48"))


@dataclass(frozen=True, slots=True)
class LoadedBuild:
    """The baseline build currently in the edge Redis for one venue."""

    build_id: int
    built_at: datetime  # naive UTC
    item_count: int


class BaselineState:
    """The build the listener has loaded, shared with the health endpoint."""

    def __init__(self, venue: str, max_age: timedelta = timedelta(hours=BASELINE_MAX_AGE_HOURS)) -> None:
        self.venue = venue
        self.max_age = max_age
        self.loaded: LoadedBuild | None = None

    def problem(self, now: datetime) -> str | None:
        """Why the loaded baselines cannot be trusted, or None when they are current."""
        if self.loaded is None or self.loaded.item_count == 0:
            return f"no {self.venue} baselines loaded, so the DRE rejects every anomaly"
        age = now - self.loaded.built_at
        if age > self.max_age:
            hours = age.total_seconds() / 3600
            return f"{self.venue} baseline build {self.loaded.build_id} is {hours:.0f} hours old"
        return None


def baselines_url(backend_base_url: str, venue: str) -> str:
    return f"{backend_base_url}/api/v1/baselines/{venue}/latest"


async def read_loaded_build(cache: Redis, venue: str) -> LoadedBuild | None:
    """The build recorded in the venue's meta hash, or None when nothing (valid) is loaded."""
    meta = await cache.hgetall(edge_baseline_meta_key(venue))
    try:
        return LoadedBuild(
            build_id=int(meta["build_id"]),
            built_at=datetime.fromisoformat(str(meta["built_at"])),
            item_count=int(meta["item_count"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


async def _replace_hash(cache: Redis, key: str, mapping: Mapping[str, str]) -> None:
    if not mapping:
        await cache.delete(key)
        return
    staging_key = f"{key}:staging"
    await cache.delete(staging_key)
    # redis-py types hash field names invariantly, so a plain str mapping needs the cast.
    await cache.hset(staging_key, mapping=cast(Mapping[Any, Any], mapping))
    await cache.rename(staging_key, key)


def _validate_build(document: Any) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise ValueError("baseline build must be a JSON object")
    if not isinstance(document.get("build_id"), int) or not isinstance(document.get("built_at"), str):
        raise ValueError("baseline build is missing build_id or built_at")
    if not isinstance(document.get("baselines"), dict) or not document["baselines"]:
        raise ValueError("baseline build has no baselines")
    if not isinstance(document.get("sticker_prices"), dict):
        raise ValueError("baseline build has no sticker_prices object")
    datetime.fromisoformat(document["built_at"])
    return document


async def store_build(cache: Redis, venue: str, document: Mapping[str, Any]) -> LoadedBuild:
    """Replace the venue's baselines and sticker prices with this build, then record it in the meta hash."""
    baselines = {name: json.dumps(baseline, sort_keys=True) for name, baseline in document["baselines"].items()}
    sticker_prices = {name: str(int(price)) for name, price in document["sticker_prices"].items()}
    await _replace_hash(cache, edge_baselines_key(venue), baselines)
    await _replace_hash(cache, edge_sticker_prices_key(venue), sticker_prices)

    loaded = LoadedBuild(
        build_id=document["build_id"],
        built_at=datetime.fromisoformat(document["built_at"]),
        item_count=len(baselines),
    )
    await cache.hset(
        edge_baseline_meta_key(venue),
        mapping={
            "build_id": str(loaded.build_id),
            "built_at": loaded.built_at.isoformat(),
            "item_count": str(loaded.item_count),
        },
    )
    return loaded


async def fetch_latest_build(session: aiohttp.ClientSession, url: str, after_build_id: int | None) -> dict[str, Any] | None:
    """The newest build from the backend, or None when it is not newer than `after_build_id`."""
    params = {} if after_build_id is None else {"after_build_id": str(after_build_id)}
    async with session.get(url, params=params) as response:
        if response.status == 204:
            return None
        response.raise_for_status()
        return _validate_build(await response.json())


async def refresh_baselines(session: aiohttp.ClientSession, url: str, cache: Redis, venue: str) -> LoadedBuild | None:
    """Load the newest build into the edge Redis unless it is already there. Returns the loaded build."""
    loaded = await read_loaded_build(cache, venue)
    document = await fetch_latest_build(session, url, loaded.build_id if loaded is not None else None)
    if document is None:
        return loaded
    loaded = await store_build(cache, venue, document)
    logger.info(
        "[BASELINES] Loaded %s build %d: %d baselines, built %s UTC.",
        venue,
        loaded.build_id,
        loaded.item_count,
        loaded.built_at.isoformat(),
    )
    return loaded


def publish_metrics(state: BaselineState, now: datetime) -> None:
    loaded = state.loaded
    baselines_loaded.labels(venue=state.venue).set(loaded.item_count if loaded is not None else 0)
    age = (now - loaded.built_at).total_seconds() if loaded is not None else 0
    baseline_build_age_seconds.labels(venue=state.venue).set(age)


async def keep_baselines_loaded(
    state: BaselineState,
    session_factory: Callable[[], Awaitable[aiohttp.ClientSession]],
    url: str,
    cache: Redis,
    *,
    clock: Callable[[], datetime] = utc_now_naive,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Load baselines now, then keep them current. Retries sooner while they are missing or stale."""
    while True:
        try:
            state.loaded = await refresh_baselines(await session_factory(), url, cache, state.venue)
        # Broad on purpose: a failed load (backend down, no build yet, Redis hiccup) must not stop the listener.
        except Exception as err:
            logger.error("[BASELINES] Could not load %s baselines from %s: %s", state.venue, url, err)

        now = clock()
        publish_metrics(state, now)
        problem = state.problem(now)
        if problem is not None:
            logger.error("[BASELINES] %s. Retrying in %.0f seconds.", problem, BASELINE_RETRY_SECONDS)
        await sleep(BASELINE_RETRY_SECONDS if problem is not None else BASELINE_REFRESH_SECONDS)
