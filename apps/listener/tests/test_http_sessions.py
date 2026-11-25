import aiohttp
import pytest
from http_sessions import LazySession


@pytest.mark.asyncio
async def test_one_session_is_reused_until_it_is_closed():
    calls = []

    def headers():
        calls.append(1)
        return {"X-Test": "1"}

    lazy = LazySession(aiohttp.ClientTimeout(total=5, connect=2), headers=headers)
    first = await lazy.get()
    second = await lazy.get()

    assert first is second
    assert first.headers["X-Test"] == "1"
    assert first.timeout.total == 5
    assert calls == [1]

    await lazy.close()
    assert first.closed
    third = await lazy.get()
    assert third is not first and not third.closed
    await lazy.close()
    # Closing twice, or before any use, is harmless.
    await lazy.close()
    await LazySession(aiohttp.ClientTimeout(total=1)).close()


@pytest.mark.asyncio
async def test_a_session_closed_elsewhere_is_replaced():
    lazy = LazySession(aiohttp.ClientTimeout(total=1))
    first = await lazy.get()
    await first.close()

    replacement = await lazy.get()

    assert replacement is not first and not replacement.closed
    assert "X-Test" not in replacement.headers
    await lazy.close()
