"""
What an item's price window holds (#269), shared by the live listener, the replay harness and the feature
module (#234), so the window features mean the same thing wherever they are computed.

The window holds the item's last 20 prices on one venue: every feed listing and every REST lowest ask that
is not a duplicate. A REST lowest ask that has not changed since the item's previous poll enters only while
the window is too short to score locally, or when no REST ask of the item has entered it for an hour.

Polls are about 5 minutes apart, so taking every poll filled a quiet item's window with 20 copies of one
price, its local variance went to about zero and any listing a little below that price looked like an
outlier. Taking only changes starved the window and moved scoring to the macro Z score, which is the same
test as the DRE's macro_sigma rule. One reading an hour keeps the window full and spreads it over the last
day or so of a quiet item. Filling a short window from every poll keeps the time after a restart, when the
edge Redis is empty, as it was: without it a quiet item would need three hours to leave the macro Z score.
"""

# Names the rule in the replay log header, so runs before and after #269 stay comparable.
PRICE_WINDOW_RULE = "unchanged_snapshot_hourly_once_filled"
# An unchanged REST lowest ask enters the window when no REST ask of the item has entered it for this long.
UNCHANGED_SNAPSHOT_WINDOW_SECONDS = 3600


def hourly_snapshot_due(timestamp: int, last_windowed_snapshot_at: int | None) -> bool:
    """True when no REST ask of the item has entered its window for an hour before `timestamp` (epoch seconds).

    `last_windowed_snapshot_at` is when the item's last REST ask entered the window, changed or not; None
    when none has since the listener started.
    """
    return last_windowed_snapshot_at is None or timestamp - last_windowed_snapshot_at >= UNCHANGED_SNAPSHOT_WINDOW_SECONDS


def unchanged_snapshot_enters_window(
    timestamp: int, last_windowed_snapshot_at: int | None, window_size: int, min_history_points: int
) -> bool:
    """True when an unchanged REST lowest ask at `timestamp` enters the price window.

    `window_size` is how many prices the window holds now, and `min_history_points` how many the Z score
    needs before it scores against the window.
    """
    return window_size < min_history_points or hourly_snapshot_due(timestamp, last_windowed_snapshot_at)
