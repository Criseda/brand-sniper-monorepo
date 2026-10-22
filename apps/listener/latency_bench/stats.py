"""Distributions and stage breakdowns from the recorded stage boundaries of one run."""

import math
from collections.abc import Iterable, Mapping
from typing import Any

from latency_bench.probes import ListingTimes

PERCENTILES = (50, 95, 99)


def percentile(sorted_values: list[float], percent: float) -> float:
    """Nearest rank percentile of an ascending list."""
    if not sorted_values:
        raise ValueError("no values")
    rank = max(1, math.ceil(percent / 100 * len(sorted_values)))
    return sorted_values[rank - 1]


def distribution_ms(values_ns: Iterable[int]) -> dict[str, Any]:
    """count, mean, p50, p95, p99 and max, in milliseconds."""
    values = sorted(value / 1e6 for value in values_ns)
    if not values:
        return {"count": 0}
    summary: dict[str, Any] = {"count": len(values), "mean": sum(values) / len(values)}
    for percent in PERCENTILES:
        summary[f"p{percent}"] = percentile(values, percent)
    summary["max"] = values[-1]
    return summary


def _span(start: int | None, end: int | None) -> int | None:
    return end - start if start is not None and end is not None else None


def listing_stages(times: ListingTimes, emitted: int | None) -> dict[str, int | None]:
    """
    Each stage's duration for one listing, in nanoseconds (None when it did not happen or was not probed).

    The stages tile the path from the feed server's write to the decision, so for a listing with every boundary
    they add up to `end_to_end`. The feed server's times are its perf_counter, which reads the same monotonic clock
    as the listener's; the run checks that the boundaries are in order.
    """
    decided = times.decided
    stages: dict[str, int | None] = {
        "end_to_end": _span(emitted, decided),
        "python_ingress_to_decision": _span(times.parse_start, decided),
        # The websocket read, Engine.IO framing and MessagePack decoding, up to the sale feed parser.
        "socket_to_parse": _span(emitted, times.parse_start),
    }
    stages["parse"] = _span(times.parse_start, times.parse_end)
    stages["parse_to_enqueue"] = _span(times.parse_end, times.enqueued)
    stages["queue_wait"] = _span(times.enqueued, times.dequeued)
    stages["consumer_routing"] = _span(times.dequeued, times.window_start)
    stages["window_write"] = _span(times.window_start, times.window_end)
    stages["window_read_and_score"] = _span(times.score_start, times.score_end)
    stages["z_score"] = times.zscore_ns
    stages["trigger_and_handoff"] = _span(times.score_end, times.detect_end)
    if times.triggered:
        stages["anomaly_pool_wait"] = _span(times.detect_end, times.dre_start)
        stages["dre"] = _span(times.dre_start, times.dre_end)
    if times.executed is not None:
        # Approved: from the DRE's verdict to the order being placed (the backend call itself is left out).
        stages["dre_to_order"] = _span(times.dre_end, times.executed)
    return stages


def stage_breakdown(rows: Iterable[Mapping[str, int | None]]) -> dict[str, dict[str, Any]]:
    collected: dict[str, list[int]] = {}
    for row in rows:
        for stage, value in row.items():
            if value is not None:
                collected.setdefault(stage, []).append(value)
    return {stage: distribution_ms(values) for stage, values in collected.items()}


def clock_order_violations(times: ListingTimes, emitted: int | None) -> int:
    """How many boundaries of one listing are out of order across processes (should always be 0)."""
    chain = [emitted, times.parse_start, times.decided]
    known = [value for value in chain if value is not None]
    return sum(1 for earlier, later in zip(known, known[1:], strict=False) if later < earlier)
