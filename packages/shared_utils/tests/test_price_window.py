from shared_utils import UNCHANGED_SNAPSHOT_WINDOW_SECONDS, hourly_snapshot_due, unchanged_snapshot_enters_window

FULL = 20
MIN_HISTORY = 4


def test_an_unchanged_snapshot_enters_a_full_window_after_an_hour_without_one():
    assert UNCHANGED_SNAPSHOT_WINDOW_SECONDS == 3600
    assert not unchanged_snapshot_enters_window(1_790_003_599, 1_790_000_000, FULL, MIN_HISTORY)
    assert unchanged_snapshot_enters_window(1_790_003_600, 1_790_000_000, FULL, MIN_HISTORY)


def test_an_unchanged_snapshot_enters_a_window_too_short_to_score():
    assert unchanged_snapshot_enters_window(1_790_000_305, 1_790_000_000, MIN_HISTORY - 1, MIN_HISTORY)
    assert not unchanged_snapshot_enters_window(1_790_000_305, 1_790_000_000, MIN_HISTORY, MIN_HISTORY)


def test_an_hourly_snapshot_is_due_when_none_has_entered_yet():
    assert hourly_snapshot_due(1_790_000_000, None)
    assert unchanged_snapshot_enters_window(1_790_000_000, None, FULL, MIN_HISTORY)
