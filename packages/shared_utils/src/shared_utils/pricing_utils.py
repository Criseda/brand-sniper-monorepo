"""
Shared pricing analysis utilities used by both the edge listener and backend compute node.
Consolidates duplicated median resolution, downtrend detection, and unit conversion logic.
"""


def to_cents(val: float | None) -> int | None:
    """Converts a USD float value to integer cents, returning None if input is None."""
    return round(float(val) * 100) if val is not None else None


def resolve_recent_median(history_entry: dict) -> float | None:
    """
    Resolves the most recent median price with active volume from a Skinport
    sales history entry dict. Falls through from 24h -> 7d -> 30d -> 90d.

    Returns the median as a USD float, or None if no valid data is available.
    """
    h24 = history_entry.get("last_24_hours") or {}
    h7 = history_entry.get("last_7_days") or {}
    h30 = history_entry.get("last_30_days") or {}
    h90 = history_entry.get("last_90_days") or {}

    m24 = h24.get("median")
    m7 = h7.get("median")
    m30 = h30.get("median")
    m90 = h90.get("median")

    if m24 and h24.get("volume", 0) > 0:
        return float(m24)
    elif m7 and h7.get("volume", 0) > 0:
        return float(m7)
    elif m30 and h30.get("volume", 0) > 0:
        return float(m30)
    elif m90:
        return float(m90)

    return None


def detect_downtrend(history_entry: dict) -> tuple[bool, float]:
    """
    Analyzes a Skinport sales history entry for active price downtrends
    by comparing median price windows.

    Returns:
        (downtrend_detected, downtrend_severity)
        where severity is a float representing the cumulative percentage decline.
    """
    h24 = history_entry.get("last_24_hours") or {}
    h7 = history_entry.get("last_7_days") or {}
    h30 = history_entry.get("last_30_days") or {}
    h90 = history_entry.get("last_90_days") or {}

    m24 = h24.get("median")
    m7 = h7.get("median")
    m30 = h30.get("median")
    m90 = h90.get("median")

    downtrend_detected = False
    downtrend_severity = 0.0

    # Medium-term trend: compare 7d (or 24h) against 30d (or 90d)
    ref_recent = m7 if m7 else m24
    ref_older = m30 if m30 else m90

    if ref_recent and ref_older and ref_recent < ref_older:
        downtrend_detected = True
        downtrend_severity += (ref_older - ref_recent) / ref_older

    # Short-term panic: 24h median lower than 7-day average
    if m24 and m7 and m24 < m7:
        downtrend_detected = True
        downtrend_severity += (m7 - m24) / m7

    return downtrend_detected, downtrend_severity
