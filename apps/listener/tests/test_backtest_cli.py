import json
from datetime import datetime
from pathlib import Path

import pytest
from backtest import __main__ as cli
from backtest import database
from backtest.sources import (
    BaselineSchedule,
    BaselineSnapshot,
    RecordedFeedEvent,
    RecordedSnapshot,
    ScheduledBuild,
    load_fixture,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "replay"
ITEM = "AK-47 | Slate (Field-Tested)"


def _run_fixture(out: Path, *extra: str) -> int:
    return cli.main(
        [
            "run",
            "--fixture",
            str(FIXTURE_DIR / "events.jsonl"),
            "--baselines",
            str(FIXTURE_DIR / "baselines.json"),
            "--out",
            str(out),
            *extra,
        ]
    )


def test_two_cli_runs_write_byte_identical_logs(tmp_path):
    assert _run_fixture(tmp_path / "first.jsonl") == 0
    assert _run_fixture(tmp_path / "second.jsonl", "--timings", str(tmp_path / "timings.jsonl")) == 0

    first = (tmp_path / "first.jsonl").read_bytes()
    assert first == (tmp_path / "second.jsonl").read_bytes()
    assert b"\r\n" not in first
    timings = [json.loads(line) for line in (tmp_path / "timings.jsonl").read_text(encoding="utf-8").splitlines()]
    assert timings and all(timing["elapsed_ns"] >= 0 for timing in timings)


def test_log_from_limits_the_logged_decisions(tmp_path):
    _run_fixture(tmp_path / "all.jsonl")
    _run_fixture(tmp_path / "late.jsonl", "--log-from", "2026-09-24T23:00:00+00:00")

    def logged(path: Path) -> int:
        return json.loads(path.read_text(encoding="utf-8").splitlines()[-1])["logged"]

    assert 0 < logged(tmp_path / "late.jsonl") < logged(tmp_path / "all.jsonl")


def test_fixture_run_requires_baselines(tmp_path):
    with pytest.raises(SystemExit, match="--baselines"):
        cli.main(["run", "--fixture", str(FIXTURE_DIR / "events.jsonl"), "--out", str(tmp_path / "out.jsonl")])


def test_database_run_requires_an_end(tmp_path):
    with pytest.raises(SystemExit, match="--end"):
        cli.main(["run", "--start", "2026-09-24T22:00", "--out", str(tmp_path / "out.jsonl")])


def test_parse_utc_converts_offsets_to_naive_utc():
    assert cli.parse_utc("2026-09-24T23:00:00+01:00") == datetime(2026, 9, 24, 22, 0)
    assert cli.parse_utc("2026-09-24T22:00") == datetime(2026, 9, 24, 22, 0)


def test_database_run_warms_up_before_the_start_and_warns_on_newer_baselines(monkeypatch, tmp_path, caplog):
    requested: dict = {}

    async def fake_stream(start, end, *, source):
        requested.update(start=start, end=end, source=source)
        for event in load_fixture(FIXTURE_DIR / "events.jsonl"):
            yield event

    monkeypatch.setattr(database, "stream_recorded_events", fake_stream)
    monkeypatch.setattr(database, "load_baseline_schedule", _fixture_schedule("2026-10-01T00:00:00", requested))
    out = tmp_path / "db.jsonl"

    exit_code = cli.main(
        ["run", "--start", "2026-09-24T23:00", "--end", "2026-09-24T23:40", "--warmup-hours", "1", "--out", str(out)]
    )

    assert exit_code == 0
    assert requested == {
        "start": datetime(2026, 9, 24, 22, 0),
        "end": datetime(2026, 9, 24, 23, 40),
        "source": "skinport",
        "schedule": (datetime(2026, 9, 24, 22, 0), datetime(2026, 9, 24, 23, 40), "skinport"),
    }
    header = json.loads(out.read_text(encoding="utf-8").splitlines()[0])
    assert header["source"] == "database:skinport:2026-09-24T23:00:00/2026-09-24T23:40:00"
    assert header["log_from_ms"] == 1_790_290_800_000
    assert (header["baseline_mode"], header["baseline_builds"]) == ("schedule", [1])
    assert "decisions use later information" in caplog.text


def test_database_run_without_any_build_stops_with_a_hint(monkeypatch, tmp_path):
    async def no_builds(start, end, *, venue):
        return BaselineSchedule(builds=[], load=_never_called)

    monkeypatch.setattr(database, "load_baseline_schedule", no_builds)

    with pytest.raises(SystemExit, match="build_baselines.py"):
        cli.main(["run", "--start", "2026-09-24T23:00", "--end", "2026-09-24T23:40", "--out", str(tmp_path / "db.jsonl")])


def test_database_run_can_use_a_fixed_baseline_file(monkeypatch, tmp_path):
    async def fake_stream(start, end, *, source):
        for event in load_fixture(FIXTURE_DIR / "events.jsonl"):
            yield event

    monkeypatch.setattr(database, "stream_recorded_events", fake_stream)
    out = tmp_path / "db.jsonl"

    cli.main(
        [
            "run",
            "--start",
            "2026-09-24T23:00",
            "--end",
            "2026-09-24T23:40",
            "--baselines",
            str(FIXTURE_DIR / "baselines.json"),
            "--out",
            str(out),
        ]
    )

    assert json.loads(out.read_text(encoding="utf-8").splitlines()[0])["baseline_mode"] == "fixed"


async def _never_called(build_id: int) -> BaselineSnapshot:
    raise AssertionError("no build should be loaded")


def _fixture_schedule(built_at: str, requested: dict | None = None):
    """A fake load_baseline_schedule with one build (ID 1) holding the committed fixture baselines."""

    async def fake_schedule(start, end, *, venue):
        if requested is not None:
            requested["schedule"] = (start, end, venue)

        async def load(build_id: int) -> BaselineSnapshot:
            snapshot = BaselineSnapshot.load(FIXTURE_DIR / "baselines.json")
            return BaselineSnapshot(snapshot.baselines, snapshot.sticker_prices, as_of=built_at)

        effective_from_ms = cli._epoch_ms(cli.parse_utc(built_at))
        return BaselineSchedule(builds=[ScheduledBuild(1, built_at, effective_from_ms)], load=load)

    return fake_schedule


def _feed(event_type: str, *sales: dict) -> RecordedFeedEvent:
    return RecordedFeedEvent(received_at_ms=1, payload={"eventType": event_type, "sales": list(sales)})


def test_select_items_takes_the_first_listed_items():
    events = [
        _feed("sold", {"marketHashName": "Sold Only"}),
        _feed("listed", {"marketHashName": "A"}, {"marketHashName": "B", "version": "Phase 1"}, {"marketHashName": "A"}),
        _feed("listed", {"marketHashName": "C"}, "junk", {"salePrice": 1}),
    ]

    assert cli.select_items(events, 10) == ["A", cli.build_versioned_name("B", "Phase 1"), "C"]
    assert cli.select_items(events, 2) == ["A", cli.build_versioned_name("B", "Phase 1")]


def test_build_fixture_keeps_chosen_items_and_their_sticker_prices():
    sticker = {"name": "Titan | Katowice 2014", "wear": None}
    events = [
        RecordedSnapshot(observed_at_ms=1, market_hash_name=ITEM, price_cents=1000),
        RecordedSnapshot(observed_at_ms=1, market_hash_name="Other", price_cents=1000),
        _feed("listed", {"marketHashName": ITEM, "salePrice": 900, "steamid": "7656", "stickers": [sticker]}),
        _feed("listed", {"marketHashName": "Other", "salePrice": 900}),
    ]
    baselines = BaselineSnapshot(
        baselines={ITEM: {"latest_price_cents": 1000}, "Other": {"latest_price_cents": 5}},
        sticker_prices={"Titan | Katowice 2014": 500_000, "Unused": 1},
        as_of="2026-07-11T00:00:00",
    )

    kept, fixture_baselines = cli.build_fixture(events, baselines, [ITEM])

    assert kept == [
        events[0],
        _feed("listed", {"marketHashName": ITEM, "salePrice": 900, "stickers": [sticker]}),
    ]
    assert fixture_baselines == BaselineSnapshot(
        baselines={ITEM: {"latest_price_cents": 1000}},
        sticker_prices={"Titan | Katowice 2014": 500_000},
        as_of="2026-07-11T00:00:00",
    )


def test_export_writes_a_fixture_that_replays(monkeypatch, tmp_path):
    source_events = load_fixture(FIXTURE_DIR / "events.jsonl")

    async def fake_stream(start, end, *, source):
        for event in source_events:
            yield event

    monkeypatch.setattr(database, "stream_recorded_events", fake_stream)
    monkeypatch.setattr(database, "load_baseline_schedule", _fixture_schedule("2026-09-24T00:00:00"))
    out_dir = tmp_path / "export"

    exit_code = cli.main(
        ["export", "--start", "2026-09-24T21:58", "--end", "2026-09-24T23:40", "--out-dir", str(out_dir), "--max-items", "5"]
    )

    assert exit_code == 0
    exported = load_fixture(out_dir / "events.jsonl")
    assert 0 < len(exported) < len(source_events)
    exported_baselines = BaselineSnapshot.load(out_dir / "baselines.json")
    assert len(exported_baselines.baselines) <= 5
    assert exported_baselines.as_of == "2026-09-24T00:00:00"
    assert (
        cli.main(
            [
                "run",
                "--fixture",
                str(out_dir / "events.jsonl"),
                "--baselines",
                str(out_dir / "baselines.json"),
                "--out",
                str(tmp_path / "decisions.jsonl"),
            ]
        )
        == 0
    )
