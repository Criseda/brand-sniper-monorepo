from datetime import UTC, datetime

from shared_utils import utc_fromtimestamp_naive, utc_now_naive


def test_utc_now_naive_returns_naive_utc():
    now = utc_now_naive()
    assert now.tzinfo is None
    assert abs((datetime.now(UTC).replace(tzinfo=None) - now).total_seconds()) < 5


def test_utc_fromtimestamp_naive_epoch():
    assert utc_fromtimestamp_naive(0) == datetime(1970, 1, 1)
    assert utc_fromtimestamp_naive(0).tzinfo is None


def test_utc_fromtimestamp_naive_matches_utc_epoch():
    ts = 1_700_000_000
    assert utc_fromtimestamp_naive(ts) == datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None)
    assert utc_fromtimestamp_naive(ts).tzinfo is None
