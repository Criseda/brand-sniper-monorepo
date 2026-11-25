"""
One lazily created aiohttp session, closed once at shutdown (#78).

The listener talks to its backend from two places, the batch delivery path (`main.py`) and the paper trade
executor (`executor.py`), each with its own timeout: a paper trade waits for the backend inside the decision
path and must give up sooner. Both keep a `LazySession` instead of their own global and close helper.
"""

from collections.abc import Callable, Mapping

import aiohttp


class LazySession:
    """An aiohttp session created on first use, recreated if something closed it, and closed by `close`."""

    def __init__(self, timeout: aiohttp.ClientTimeout, headers: Callable[[], Mapping[str, str]] | None = None) -> None:
        self._timeout = timeout
        # Read when the session is created, so a test or a rotated key set before first use is picked up.
        self._headers = headers
        self._session: aiohttp.ClientSession | None = None

    async def get(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = dict(self._headers()) if self._headers is not None else None
            self._session = aiohttp.ClientSession(headers=headers, timeout=self._timeout)
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None
