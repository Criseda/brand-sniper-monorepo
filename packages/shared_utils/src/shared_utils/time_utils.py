from datetime import UTC, datetime


def utc_now_naive() -> datetime:
    """Returns the current UTC time as a naive datetime (tzinfo stripped)."""
    return datetime.now(UTC).replace(tzinfo=None)


def utc_fromtimestamp_naive(timestamp: float) -> datetime:
    """Converts a POSIX timestamp to a naive UTC datetime (tzinfo stripped)."""
    return datetime.fromtimestamp(timestamp, tz=UTC).replace(tzinfo=None)
