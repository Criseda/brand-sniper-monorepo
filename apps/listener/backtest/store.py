"""In-memory stand-in for the edge Redis, covering only the commands the decision path uses."""

import json
from collections.abc import Mapping
from typing import Any


class InMemoryEdgeStore:
    """
    Async, single-process replacement for the edge Redis (decode_responses=True) during replay.

    Sorted sets follow Redis semantics exactly, because the price window depends on them: members are
    unique, and ranks order by (score, member bytes). Everything else is a plain string or hash.
    """

    def __init__(self) -> None:
        self._strings: dict[str, str] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._sorted_sets: dict[str, dict[str, float]] = {}

    # --- Loading ---

    def load_baselines(self, baselines: Mapping[str, Mapping[str, Any]], sticker_prices: Mapping[str, int]) -> None:
        """Seed the store the way `update_baselines.py` seeds the edge Redis."""
        for market_hash_name, baseline in baselines.items():
            self._strings[f"baseline:{market_hash_name}"] = json.dumps(dict(baseline))
        if sticker_prices:
            self._hashes["sticker_prices"] = {name: str(price) for name, price in sticker_prices.items()}

    # --- Strings and hashes ---

    async def get(self, key: str) -> str | None:
        return self._strings.get(key)

    async def set(self, key: str, value: str) -> bool:
        self._strings[key] = value
        return True

    async def hget(self, name: str, key: str) -> str | None:
        return self._hashes.get(name, {}).get(key)

    # --- Sorted sets ---

    def _ordered_members(self, key: str) -> list[str]:
        members = self._sorted_sets.get(key, {})
        return sorted(members, key=lambda member: (members[member], member.encode("utf-8")))

    @staticmethod
    def _rank_slice(length: int, start: int, stop: int) -> range:
        """Resolve Redis inclusive, possibly negative rank bounds to a Python range."""
        if start < 0:
            start = max(length + start, 0)
        if stop < 0:
            stop = length + stop
        stop = min(stop, length - 1)
        return range(start, stop + 1) if start <= stop else range(0)

    async def zadd(self, key: str, mapping: Mapping[str, float]) -> int:
        members = self._sorted_sets.setdefault(key, {})
        added = sum(1 for member in mapping if member not in members)
        members.update(mapping)
        return added

    async def zcard(self, key: str) -> int:
        return len(self._sorted_sets.get(key, {}))

    async def zrange(self, key: str, start: int, stop: int) -> list[str]:
        ordered = self._ordered_members(key)
        return [ordered[rank] for rank in self._rank_slice(len(ordered), start, stop)]

    async def zremrangebyrank(self, key: str, start: int, stop: int) -> int:
        ordered = self._ordered_members(key)
        members = self._sorted_sets.get(key, {})
        removed = 0
        for rank in self._rank_slice(len(ordered), start, stop):
            del members[ordered[rank]]
            removed += 1
        if not members:
            self._sorted_sets.pop(key, None)
        return removed

    async def aclose(self) -> None:
        """Matches the Redis client's close so live code paths can shut the store down."""
