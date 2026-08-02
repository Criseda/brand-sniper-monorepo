"""
Shared pricing analysis utilities used by both the edge listener and backend compute node.
Consolidates duplicated median resolution, downtrend detection, and unit conversion logic.
"""

import logging
from typing import NamedTuple

_logger = logging.getLogger("shared_utils.pricing")


class _HistoryWindows(NamedTuple):
    """Median price windows extracted from a Skinport sales history entry."""

    h24: dict
    h7: dict
    h30: dict
    h90: dict


def _require_history_entry(history_entry: object) -> dict:
    """Validates that a sales history entry is a dict, raising TypeError otherwise."""
    if not isinstance(history_entry, dict):
        raise TypeError(
            f"history_entry must be a dict, got {type(history_entry).__name__}. "
            "Refusing to analyze a non-dict sales history entry."
        )
    return history_entry


def _parse_history_windows(history_entry: dict) -> _HistoryWindows:
    """Extracts the four median price windows, missing keys default to {}."""
    entry = _require_history_entry(history_entry)
    return _HistoryWindows(
        h24=entry.get("last_24_hours") or {},
        h7=entry.get("last_7_days") or {},
        h30=entry.get("last_30_days") or {},
        h90=entry.get("last_90_days") or {},
    )


def _safe_median(window: dict, label: str) -> float | None:
    """Coerces a window's median to float, treating malformed values as missing."""
    median = window.get("median")
    if median is None:
        return None
    try:
        return float(median)
    except (TypeError, ValueError):
        _logger.warning("[PRICING] Ignoring non-numeric median in %s window: %r", label, median)
        return None


def to_cents(val: float | None) -> int | None:
    """Converts a USD float value to integer cents, returning None if input is None.

    Raises TypeError if a non-numeric value is passed, since that indicates a
    caller bug rather than a data quality issue.
    """
    if val is None:
        return None
    if not isinstance(val, (int, float)):
        raise TypeError(f"to_cents expects a numeric value or None, got {type(val).__name__}.")
    return round(float(val) * 100)


def resolve_recent_median(history_entry: dict) -> float | None:
    """
    Resolves the most recent median price with active volume from a Skinport
    sales history entry dict. Falls through from 24h -> 7d -> 30d -> 90d.

    Returns the median as a USD float, or None if no valid data is available.
    Raises TypeError if history_entry is not a dict.
    """
    h24, h7, h30, h90 = _parse_history_windows(history_entry)

    m24 = _safe_median(h24, "last_24_hours")
    m7 = _safe_median(h7, "last_7_days")
    m30 = _safe_median(h30, "last_30_days")
    m90 = _safe_median(h90, "last_90_days")

    if m24 and h24.get("volume", 0) > 0:
        return m24
    elif m7 and h7.get("volume", 0) > 0:
        return m7
    elif m30 and h30.get("volume", 0) > 0:
        return m30
    elif m90:
        return m90

    return None


def detect_downtrend(history_entry: dict) -> tuple[bool, float]:
    """
    Analyzes a Skinport sales history entry for active price downtrends
    by comparing median price windows.

    Returns:
        (downtrend_detected, downtrend_severity)
        where severity is a float representing the cumulative percentage decline.
    """
    h24, h7, h30, h90 = _parse_history_windows(history_entry)

    m24 = _safe_median(h24, "last_24_hours")
    m7 = _safe_median(h7, "last_7_days")
    m30 = _safe_median(h30, "last_30_days")
    m90 = _safe_median(h90, "last_90_days")

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
