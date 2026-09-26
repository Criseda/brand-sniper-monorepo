import io
import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import baseline_scorecard
import pytest
from baseline_scorecard import (
    MIN_SAMPLE,
    OUTCOME_LABELED,
    OUTCOME_NEUTRAL,
    OUTCOME_PENDING,
    OUTCOME_UNLABELED,
    TICK_KIND_LISTING,
    TICK_KIND_REST_SNAPSHOT,
    TRADABLE_REST_FROM,
    DecisionLog,
    LoggedDecision,
    ScorecardInputs,
    Trade,
    apply_listing_label,
    build_scorecard,
    collect_trades,
    fetch_listing_labels,
    fetch_profitable_listings,
    fetch_volumes,
    label_snapshot_trades,
    liquidity_bucket,
    log_to_mlflow,
    max_drawdown_cents,
    mean_interval,
    parse_database_source,
    price_tier,
    read_decision_log,
    render_markdown,
    score_recall,
    score_trades,
    sweep_thresholds,
    time_slices,
    wilson_interval,
    would_trigger,
)
from label_outcomes import FeedSale, LabelConfig, SoldIndex

START = datetime(2026, 10, 1)
END = datetime(2026, 10, 15)
DAY = timedelta(days=1)
# Late enough that everything decided in [START, END) has matured.
AS_OF = datetime(2026, 11, 1)
NAME = "AK-47 | Slate (Field-Tested)"
SOURCE_LABEL = f"database:skinport:{START.isoformat()}/{END.isoformat()}"
LIVE_CONFIG = {"z_score_threshold": -2.0, "z_score_sticker_threshold": -1.0, "min_savings_cents": 50}


def epoch(moment: datetime) -> int:
    return int((moment - datetime(1970, 1, 1)).total_seconds())


def decision_row(listing_id, at, *, approve=False, reason="below_threshold", price=900, score=-1.0, name=NAME, **features):
    return {
        "type": "decision",
        "seq": 0,
        "timestamp": epoch(at),
        "venue": "skinport",
        "listing_id": listing_id,
        "event_type": "listed" if listing_id is not None else None,
        "market_hash_name": name,
        "price_cents": price,
        "approve": approve,
        "reason": reason,
        "score": score,
        "features": {"mean_cents": 1000.0, "window_size": 20, "z_source": "local", "sticker_count": 0, **features},
    }


def write_log(path, rows, *, strategy="zscore_dre_sweep", source=SOURCE_LABEL, builds=(3,)):
    header = {"type": "run", "format": 2, "strategy": strategy, "config": LIVE_CONFIG, "source": source}
    header["baseline_builds"] = list(builds)
    lines = [header, *rows, {"type": "summary", "decisions": len(rows)}]
    path.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    return path


def baseline_row(build_id, at, look_ahead=False):
    return {"type": "baseline", "build_id": build_id, "from_ms": epoch(at) * 1000, "look_ahead": look_ahead}


def logged(row, build_id=3, look_ahead=False):
    return LoggedDecision(row=row, build_id=build_id, look_ahead=look_ahead)


def make_log(decisions, strategy="zscore_dre_sweep"):
    return DecisionLog(
        header={"strategy": strategy, "config": LIVE_CONFIG},
        decisions=decisions,
        summary=None,
        sha256="x",
        venue="skinport",
        start=START,
        end=END,
    )


def trade(outcome=OUTCOME_LABELED, margin=100, at=START, listing_id="L", price=900, **fields):
    item = Trade(
        decided_at=at,
        tick_kind=TICK_KIND_LISTING if listing_id else TICK_KIND_REST_SNAPSHOT,
        listing_id=listing_id,
        market_hash_name=NAME,
        price_cents=price,
        rule="support_floor",
        z_source="local",
        build_id=3,
        **fields,
    )
    item.outcome = outcome
    if outcome == OUTCOME_LABELED:
        item.net_margin_cents = margin
        item.is_profitable = margin >= 0
    return item


# --- Decision log ---


def test_read_decision_log_tracks_the_build_behind_each_decision(tmp_path):
    rows = [
        baseline_row(3, START, look_ahead=True),
        decision_row("1", START),
        baseline_row(4, START + DAY),
        {"type": "note"},  # a record type the scorecard does not read is skipped
        decision_row("2", START + DAY),
    ]

    log = read_decision_log(write_log(tmp_path / "log.jsonl", rows))

    assert [(item.row["listing_id"], item.build_id, item.look_ahead) for item in log.decisions] == [
        ("1", 3, True),
        ("2", 4, False),
    ]
    assert log.look_ahead_ranges == [(epoch(START) * 1000, epoch(START + DAY) * 1000)]
    assert (log.venue, log.start, log.end) == ("skinport", START, END)
    assert log.strategy == "zscore_dre_sweep"
    assert log.summary == {"type": "summary", "decisions": len(rows)}
    assert len(log.sha256) == 64


def test_read_decision_log_rejects_a_fixture_replay(tmp_path):
    with pytest.raises(ValueError, match="database replay"):
        read_decision_log(write_log(tmp_path / "log.jsonl", [], source="fixture:events.jsonl"))


def test_read_decision_log_rejects_another_strategy(tmp_path):
    with pytest.raises(ValueError, match="live rules"):
        read_decision_log(write_log(tmp_path / "log.jsonl", [], strategy="onnx_shadow"))


def test_read_decision_log_needs_a_header(tmp_path):
    path = tmp_path / "log.jsonl"
    path.write_text(json.dumps(decision_row("1", START)) + "\n\n", encoding="utf-8")

    with pytest.raises(ValueError, match="no run header"):
        read_decision_log(path)


def test_parse_database_source_reads_offsets_as_utc():
    assert parse_database_source("database:skinport:2026-10-01T01:00:00+01:00/2026-10-02T00:00:00") == (
        "skinport",
        START,
        datetime(2026, 10, 2),
    )


# --- Trades and labels ---


def test_collect_trades_counts_a_listing_once_and_each_rest_snapshot():
    decisions = [
        logged(decision_row("1", START, approve=True, reason="support_floor")),
        logged(decision_row("1", START + DAY, approve=True, reason="support_floor")),
        logged(decision_row(None, START, approve=True, reason="macro_sigma")),
        logged(decision_row(None, START + DAY, approve=True, reason="macro_sigma")),
        logged(decision_row("2", START)),  # not approved
        logged(decision_row("3", START - DAY, approve=True, reason="support_floor")),  # before the range
        logged(decision_row("4", START, approve=True, reason="support_floor"), look_ahead=True),
    ]

    trades = collect_trades(decisions, START)

    assert [(item.listing_id, item.tick_kind, item.rule) for item in trades] == [
        ("1", TICK_KIND_LISTING, "support_floor"),
        (None, TICK_KIND_REST_SNAPSHOT, "macro_sigma"),
        (None, TICK_KIND_REST_SNAPSHOT, "macro_sigma"),
    ]
    assert trades[0].decided_at == START


@pytest.mark.parametrize(
    ("label", "decided_at", "expected"),
    [
        (None, START, OUTCOME_UNLABELED),
        (None, AS_OF - DAY, OUTCOME_PENDING),
        ({"is_profitable": None, "resale_net_margin_cents": None, "label_available_at": START}, START, OUTCOME_NEUTRAL),
        ({"is_profitable": True, "resale_net_margin_cents": 250, "label_available_at": START}, START, OUTCOME_LABELED),
        # A label that is not available yet at as_of is not read.
        ({"is_profitable": True, "resale_net_margin_cents": 250, "label_available_at": AS_OF + DAY}, START, OUTCOME_UNLABELED),
    ],
)
def test_apply_listing_label(label, decided_at, expected):
    item = trade(outcome=OUTCOME_PENDING, at=decided_at)

    apply_listing_label(item, label, LabelConfig(), AS_OF)

    assert item.outcome == expected
    if expected == OUTCOME_LABELED:
        assert (item.is_profitable, item.net_margin_cents) == (True, 250)


def test_rest_snapshots_are_labeled_with_the_v1_rule():
    phase_name = "★ Karambit | Doppler (Phase 2) (Factory New)"
    sold = [
        FeedSale(f"S{i}", phase_name, "★ Karambit | Doppler (Factory New)", 1500, START + 8 * DAY + timedelta(hours=i))
        for i in range(3)
    ]
    mature = trade(outcome=OUTCOME_PENDING, listing_id=None, price=1000)
    mature.market_hash_name = phase_name
    recent = trade(outcome=OUTCOME_PENDING, listing_id=None, at=AS_OF - DAY)

    label_snapshot_trades([mature, recent], SoldIndex.build(sold), LabelConfig(), AS_OF)

    # Resale at 1500, less the 8% fee (120), less the 1000 buy price.
    assert (mature.outcome, mature.net_margin_cents, mature.is_profitable) == (OUTCOME_LABELED, 380, True)
    assert recent.outcome == OUTCOME_PENDING


# --- Statistics ---


def test_wilson_interval_matches_the_known_values():
    low, high = wilson_interval(5, 10)
    assert low == pytest.approx(0.2366, abs=1e-4)
    assert high == pytest.approx(0.7634, abs=1e-4)
    assert wilson_interval(0, 10)[0] == 0.0
    with pytest.raises(ValueError):
        wilson_interval(0, 0)


def test_mean_interval_is_a_seeded_bootstrap_for_small_samples():
    values = [100, -50, 30, 80, -20] * 8

    first = mean_interval(values)
    second = mean_interval(values)

    assert first == second
    low, high, method = first
    assert method == "bootstrap"
    assert low < sum(values) / len(values) < high


def test_mean_interval_is_normal_for_large_samples():
    low, high, method = mean_interval([0, 100] * 300)

    assert method == "normal"
    assert low == pytest.approx(50 - 1.96 * 50.04 / 600**0.5, abs=0.01)
    assert high == pytest.approx(50 + 1.96 * 50.04 / 600**0.5, abs=0.01)


def test_max_drawdown_measures_from_the_running_peak():
    # Cumulative 100, 70, 120, -80, -70: the fall from the 120 peak to -80.
    assert max_drawdown_cents([100, -30, 50, -200, 10]) == 200
    assert max_drawdown_cents([-40, 10]) == 40
    assert max_drawdown_cents([]) == 0


def test_score_trades_says_insufficient_data_below_the_minimum_sample():
    trades = [trade(margin=100)] * (MIN_SAMPLE - 1) + [trade(outcome=OUTCOME_NEUTRAL), trade(outcome=OUTCOME_PENDING)]

    cell = score_trades(trades)

    assert cell["status"] == "insufficient_data"
    assert (cell["trades"], cell["labeled"], cell["neutral"], cell["pending"]) == (MIN_SAMPLE + 1, MIN_SAMPLE - 1, 1, 1)
    assert cell["precision"] is None and cell["net_pnl_cents"] is None and cell["max_drawdown_cents"] is None


def test_score_trades_reports_precision_pnl_and_drawdown_at_the_minimum_sample():
    wins = [trade(margin=200, at=START + timedelta(hours=i)) for i in range(20)]
    losses = [trade(margin=-100, at=START + timedelta(hours=20 + i)) for i in range(10)]

    cell = score_trades(losses + wins)

    assert cell["status"] == "ok"
    assert cell["precision"]["value"] == pytest.approx(20 / 30)
    assert cell["precision"]["low"] < 20 / 30 < cell["precision"]["high"]
    assert cell["net_pnl_cents"]["total"] == 3000
    assert cell["net_pnl_cents"]["mean"] == 100
    # Trades are ordered by time: the ten losses come after the wins.
    assert cell["max_drawdown_cents"] == 1000


def test_score_recall_counts_approved_profitable_listings():
    universe = [f"P{i}" for i in range(MIN_SAMPLE)]
    trades = [trade(listing_id="P0", margin=10), trade(listing_id="P1", margin=-10), trade(listing_id="X", margin=10)]

    recall = score_recall(trades, universe)

    assert (recall["status"], recall["approved"], recall["value"]) == ("ok", 1, 1 / MIN_SAMPLE)
    assert score_recall(trades, universe[:5])["value"] is None


def test_buckets():
    assert price_tier(trade(price=499)) == "under $5"
    assert price_tier(trade(price=500)) == "$5 to $50"
    assert price_tier(trade(price=60_000)) == "$500 and over"
    assert liquidity_bucket(trade()) == "no baseline"
    assert liquidity_bucket(trade(avg_volume_30d=0.5)) == "under 1 sale a day"
    assert liquidity_bucket(trade(avg_volume_30d=12.0)) == "10 to 50 sales a day"


def test_time_slices_cover_the_range_without_gaps():
    assert time_slices(START, START + 10 * DAY, 7) == [(START, START + 7 * DAY), (START + 7 * DAY, START + 10 * DAY)]


# --- Threshold sweep ---


@pytest.mark.parametrize("z_score", [-3.0, -2.0, -1.5, -0.5])
@pytest.mark.parametrize("price_cents", [900, 960, 999])
@pytest.mark.parametrize("sticker_count", [0, 2])
@pytest.mark.parametrize(("z_threshold", "min_savings"), [(-2.0, 50), (-1.5, 25), (-2.5, 100)])
def test_would_trigger_matches_the_live_rule(monkeypatch, z_score, price_cents, sticker_count, z_threshold, min_savings):
    import zscore
    from models import MarketTick

    monkeypatch.setattr(zscore, "Z_SCORE_THRESHOLD", z_threshold)
    monkeypatch.setattr(zscore, "MIN_SAVINGS_CENTS", min_savings)
    stickers = [{"name": f"Sticker {i}"} for i in range(sticker_count)]
    tick = MarketTick(venue="skinport", market_hash_name=NAME, price_usd=price_cents / 100, stickers=stickers)

    expected = zscore.should_trigger_anomaly(z_score, 1000.0, tick)

    assert (
        would_trigger(
            z_score,
            1000.0,
            price_cents,
            sticker_count,
            z_threshold=z_threshold,
            sticker_z_threshold=zscore.Z_SCORE_STICKER_THRESHOLD,
            min_savings_cents=min_savings,
        )
        == expected
    )


def _sweep_decisions():
    return [
        # Approved live.
        logged(decision_row("A", START, approve=True, reason="support_floor", score=-2.5, price=800)),
        # Below the live threshold, but the DRE would approve it: only looser thresholds trade it.
        logged(decision_row("B", START, score=-1.7, price=900, dre_reason="macro_sigma")),
        # Below the live threshold and the DRE would reject it.
        logged(decision_row("C", START, score=-1.7, price=900, dre_reason=None)),
        # Triggered but rejected by the DRE.
        logged(decision_row("D", START, reason="dre_rejected", score=-3.5, price=700)),
        # Not scored.
        logged(decision_row("E", START, reason="insufficient_history", score=None)),
    ]


def _cell(sweep, z_threshold, min_savings):
    [row] = [row for row in sweep["rows"] if (row["z_threshold"], row["min_savings_cents"]) == (z_threshold, min_savings)]
    return row


def test_sweep_recomputes_the_live_cell_and_trades_more_with_a_looser_threshold():
    decisions = _sweep_decisions()

    def listing_trade(decision):
        return trade(listing_id=decision.row["listing_id"])

    sweep = sweep_thresholds(decisions, make_log(decisions), START, listing_trade)

    assert sweep["live_matches_log"] is True
    assert sweep["in_sample"] is True
    assert _cell(sweep, -2.0, 50)["live"] is True
    assert _cell(sweep, -2.0, 50)["trades"] == 1
    assert _cell(sweep, -1.5, 50)["trades"] == 2
    assert _cell(sweep, -3.0, 50)["trades"] == 0
    # A is 200 cents under the mean, so a 200 cent floor still takes it.
    assert _cell(sweep, -2.0, 200)["trades"] == 1


def test_sweep_flags_a_live_cell_that_does_not_match_the_log():
    decisions = [logged(decision_row("A", START, approve=True, reason="support_floor", score=-1.0))]

    sweep = sweep_thresholds(decisions, make_log(decisions), START, lambda decision: trade())

    assert sweep["live_matches_log"] is False
    assert "do not trust" in render_markdown(_card_with(sweep))


def test_sweep_needs_the_sweep_strategy():
    sweep = sweep_thresholds([], make_log([], strategy="zscore_dre"), START, lambda decision: trade())

    assert sweep["available"] is False
    assert "Not available" in render_markdown(_card_with(sweep))


# --- Database queries ---


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.executed = []

    async def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        return _Result(self.rows)


class FakeEngine:
    def __init__(self, rows=()):
        self.conn = FakeConn(list(rows))

    def connect(self):
        @asynccontextmanager
        async def _ctx():
            yield self.conn

        return _ctx()


@pytest.mark.asyncio
async def test_fetch_listing_labels_queries_in_chunks(monkeypatch):
    engine = FakeEngine([("1", True, 120, START)])
    monkeypatch.setattr(baseline_scorecard, "async_engine", engine)
    monkeypatch.setattr(baseline_scorecard, "_ID_CHUNK_SIZE", 2)

    labels = await fetch_listing_labels(["3", "1", "2", "1"], "v1")

    assert labels == {"1": {"is_profitable": True, "resale_net_margin_cents": 120, "label_available_at": START}}
    assert [params["listing_ids"] for _, params in engine.conn.executed] == [["1", "2"], ["3"]]
    assert engine.conn.executed[0][1]["label_version"] == "v1"


@pytest.mark.asyncio
async def test_fetch_profitable_listings_reads_only_available_labels(monkeypatch):
    engine = FakeEngine([("1", START)])
    monkeypatch.setattr(baseline_scorecard, "async_engine", engine)

    assert await fetch_profitable_listings(START, END, "v1", AS_OF) == {"1": START}
    sql, params = engine.conn.executed[0]
    assert "is_profitable IS TRUE" in sql and "label_available_at <= :as_of" in sql
    assert params["as_of"] == AS_OF


@pytest.mark.asyncio
async def test_fetch_volumes_reads_the_build_each_trade_used(monkeypatch):
    engine = FakeEngine([(3, NAME, 4.5)])
    monkeypatch.setattr(baseline_scorecard, "async_engine", engine)

    assert await fetch_volumes([trade()]) == {(3, NAME): 4.5}
    assert engine.conn.executed[0][1] == {"build_ids": [3], "names": [NAME]}
    assert await fetch_volumes([]) == {}


# --- The scorecard, end to end ---


def _card_with(sweep):
    card = {
        "as_of": AS_OF.isoformat(),
        "git_commit": None,
        "data": {
            "venue": "skinport",
            "replay_start": START.isoformat(),
            "start": START.isoformat(),
            "end": END.isoformat(),
            "tradable_rest_from": TRADABLE_REST_FROM.isoformat(),
            "decision_log_sha256": "x",
            "strategy": "zscore_dre_sweep",
            "strategy_config": LIVE_CONFIG,
        },
        "labels": {"version": "v1", "horizon_seconds": 14 * 86_400, "min_comparable_sales": 3},
        "fees": {"venue": "skinport", "fee_tiers": [{"min_price_cents": 0, "fee_bps": 800}], "hold_seconds": 7 * 86_400},
        "rules": {"min_sample": 30, "bootstrap_resamples": 2000, "bootstrap_seed": 249, "bootstrap_max_trades": 500},
        "notes": [],
        "coverage": {
            "trades": 0,
            "listing_trades": 0,
            "rest_snapshot_trades": 0,
            "excluded_trade_locked_rest": 0,
            "excluded_look_ahead": 0,
        },
        "overall": {**score_trades([]), "recall": score_recall([], [])},
        "slices": [],
        "breakdowns": {},
        "sweep": sweep,
    }
    card["fees"]["min_margin_cents"] = 0
    return card


def _fake_database(monkeypatch, labels, profitable, sold=()):
    async def fake_labels(listing_ids, label_version):
        return {listing_id: labels[listing_id] for listing_id in listing_ids if listing_id in labels}

    async def fake_profitable(start, end, label_version, as_of):
        return profitable

    async def fake_volumes(trades):
        return {(3, NAME): 20.0}

    async def fake_sold(event_type, start, end, names=None):
        assert event_type == "sold"
        return list(sold)

    monkeypatch.setattr(baseline_scorecard, "fetch_listing_labels", fake_labels)
    monkeypatch.setattr(baseline_scorecard, "fetch_profitable_listings", fake_profitable)
    monkeypatch.setattr(baseline_scorecard, "fetch_volumes", fake_volumes)
    monkeypatch.setattr(baseline_scorecard, "fetch_feed_sales", fake_sold)
    monkeypatch.setattr(baseline_scorecard, "git_commit", lambda: "abc123")


def _label(profitable, margin):
    return {"is_profitable": profitable, "resale_net_margin_cents": margin, "label_available_at": START + 15 * DAY}


@pytest.mark.asyncio
async def test_scorecard_scores_approvals_against_their_labels(monkeypatch, tmp_path):
    rows = [baseline_row(3, START)]
    labels = {}
    for index in range(40):
        listing_id = f"L{index}"
        at = START + timedelta(hours=index)
        rows.append(decision_row(listing_id, at, approve=True, reason="support_floor", score=-2.5, price=800))
        labels[listing_id] = _label(index % 4 != 0, 150 if index % 4 else -60)
    rows.append(decision_row(None, START, approve=True, reason="macro_sigma", score=-2.5, price=800))
    rows.append(decision_row("early", TRADABLE_REST_FROM - DAY, approve=True, reason="support_floor"))
    profitable = {f"L{index}": START for index in range(40) if index % 4} | {f"M{index}": START for index in range(30)}
    sold = [FeedSale(f"S{i}", NAME, NAME, 1000, START + 8 * DAY + timedelta(hours=i)) for i in range(3)]
    _fake_database(monkeypatch, labels, profitable, sold)
    log = read_decision_log(
        write_log(tmp_path / "log.jsonl", rows, source=f"database:skinport:2026-09-20T00:00/{END.isoformat()}")
    )

    card = await build_scorecard(ScorecardInputs(log=log, label_version="v1", as_of=AS_OF))

    overall = card["overall"]
    assert (overall["trades"], overall["labeled"]) == (41, 41)
    assert overall["status"] == "ok"
    # 30 profitable listings, and the REST snapshot resold at 1000 less the 80 fee: 120 over its 800 price.
    assert overall["precision"]["value"] == pytest.approx(31 / 41)
    assert overall["net_pnl_cents"]["total"] == 30 * 150 - 10 * 60 + 120
    assert overall["recall"]["approved"] == 30
    assert overall["recall"]["profitable_listings"] == 60
    assert card["coverage"]["excluded_trade_locked_rest"] == 1
    assert card["data"]["start"] == TRADABLE_REST_FROM.isoformat()
    assert set(card["breakdowns"]) == {"tick_kind", "dre_rule", "z_source", "price_tier", "item_type", "liquidity"}
    assert card["breakdowns"]["liquidity"]["10 to 50 sales a day"]["trades"] == 41
    assert card["breakdowns"]["tick_kind"][TICK_KIND_REST_SNAPSHOT]["status"] == "insufficient_data"
    assert card["sweep"]["live_matches_log"] is True
    assert card["notes"] == []
    assert card["git_commit"] == "abc123"
    markdown = render_markdown(card)
    assert "| 41 | 41 |" in markdown
    assert "insufficient data" in markdown


@pytest.mark.asyncio
async def test_scorecard_notes_matured_trades_without_labels(monkeypatch, tmp_path):
    rows = [decision_row("L1", START, approve=True, reason="support_floor", score=-2.5, price=800)]
    _fake_database(monkeypatch, labels={}, profitable={})
    log = read_decision_log(write_log(tmp_path / "log.jsonl", rows))

    card = await build_scorecard(ScorecardInputs(log=log, label_version="v2", as_of=AS_OF))

    assert card["overall"]["unlabeled"] == 1
    assert card["overall"]["precision"] is None
    assert any("label_outcomes.py" in note for note in card["notes"])


@pytest.mark.asyncio
async def test_scorecard_leaves_rest_snapshots_unlabeled_for_another_label_version(monkeypatch, tmp_path):
    rows = [decision_row(None, START, approve=True, reason="macro_sigma", score=-2.5, price=800)]
    _fake_database(monkeypatch, labels={}, profitable={})
    log = read_decision_log(write_log(tmp_path / "log.jsonl", rows))

    card = await build_scorecard(ScorecardInputs(log=log, label_version="v2", as_of=AS_OF))

    assert card["overall"]["unlabeled"] == 1
    assert any("current rule" in note for note in card["notes"])


@pytest.mark.asyncio
async def test_scorecard_rejects_a_range_before_tradable_rest_prices(monkeypatch, tmp_path):
    source = f"database:skinport:2026-09-20T00:00/{TRADABLE_REST_FROM.isoformat()}"
    log = read_decision_log(write_log(tmp_path / "log.jsonl", [], source=source))

    with pytest.raises(ValueError, match="#275"):
        await build_scorecard(ScorecardInputs(log=log, label_version="v1", as_of=AS_OF))


@pytest.mark.asyncio
async def test_scorecard_rejects_another_venue(tmp_path):
    source = f"database:csfloat:{START.isoformat()}/{END.isoformat()}"
    log = read_decision_log(write_log(tmp_path / "log.jsonl", [], source=source))

    with pytest.raises(ValueError, match="csfloat"):
        await build_scorecard(ScorecardInputs(log=log, label_version="v1", as_of=AS_OF))


@pytest.mark.asyncio
async def test_harness_log_scores_end_to_end(monkeypatch, tmp_path):
    """A log written by the listener's replay harness reads and scores without translation."""
    from backtest.harness import run_replay
    from backtest.sources import BaselineSnapshot, RecordedFeedEvent, RecordedSnapshot
    from backtest.strategy import ZScoreDreSweepStrategy

    t0_ms = epoch(START) * 1000
    events = [RecordedSnapshot(t0_ms + i * 305_000, NAME, price) for i, price in enumerate([1000, 1010, 990, 1005, 995])]
    sales = [{"marketHashName": NAME, "salePrice": 400, "productId": 99, "currency": "USD"}]
    events.append(RecordedFeedEvent(t0_ms + 1_600_000, {"eventType": "listed", "sales": sales}))
    baselines = BaselineSnapshot(baselines={NAME: {"support_floor_cents": 500, "latest_price_cents": 1000}})
    text = io.StringIO()
    await run_replay(events, ZScoreDreSweepStrategy(), baselines, text, source_label=SOURCE_LABEL)
    path = tmp_path / "log.jsonl"
    path.write_text(text.getvalue(), encoding="utf-8")
    _fake_database(monkeypatch, labels={"99": _label(True, 520)}, profitable={"99": START})

    card = await build_scorecard(ScorecardInputs(log=read_decision_log(path), label_version="v1", as_of=AS_OF))

    assert (card["overall"]["trades"], card["overall"]["labeled"]) == (1, 1)
    assert card["sweep"]["live_matches_log"] is True
    assert card["breakdowns"]["dre_rule"]["support_floor"]["trades"] == 1


# --- CLI and MLflow ---


def _write_cli_log(tmp_path):
    rows = [decision_row("L1", START, approve=True, reason="support_floor", score=-2.5, price=800)]
    return write_log(tmp_path / "log.jsonl", rows)


@pytest.mark.asyncio
async def test_run_writes_the_json_and_markdown_reports(monkeypatch, tmp_path):
    _fake_database(monkeypatch, labels={"L1": _label(True, 100)}, profitable={})
    out_dir = tmp_path / "benchmarks"
    args = baseline_scorecard.build_parser().parse_args(
        ["--decisions", str(_write_cli_log(tmp_path)), "--out-dir", str(out_dir), "--as-of", AS_OF.isoformat(), "--no-mlflow"]
    )

    assert await baseline_scorecard.run(args) == 0

    card = json.loads((out_dir / "baseline_scorecard.json").read_text(encoding="utf-8"))
    assert card["overall"]["trades"] == 1
    assert card["as_of"] == AS_OF.isoformat()
    assert (out_dir / "baseline_scorecard.md").read_text(encoding="utf-8").startswith("# Baseline scorecard")


@pytest.mark.asyncio
async def test_run_logs_to_mlflow_and_reports_a_failure(monkeypatch, tmp_path):
    _fake_database(monkeypatch, labels={}, profitable={})
    argv = ["--decisions", str(_write_cli_log(tmp_path)), "--out-dir", str(tmp_path), "--as-of", AS_OF.isoformat()]
    args = baseline_scorecard.build_parser().parse_args(argv)
    monkeypatch.setattr(baseline_scorecard, "log_to_mlflow", MagicMock(return_value="run-1"))

    assert await baseline_scorecard.run(args) == 0

    monkeypatch.setattr(baseline_scorecard, "log_to_mlflow", MagicMock(side_effect=ConnectionError("down")))
    assert await baseline_scorecard.run(args) == 1


@pytest.mark.asyncio
async def test_run_rejects_a_slice_shorter_than_a_day(tmp_path):
    args = baseline_scorecard.build_parser().parse_args(["--decisions", str(_write_cli_log(tmp_path)), "--slice-days", "0"])

    with pytest.raises(SystemExit):
        await baseline_scorecard.run(args)


def _scored_card():
    trades = [trade(margin=100, at=START + timedelta(hours=i), listing_id=f"L{i}") for i in range(MIN_SAMPLE)]
    card = _card_with({"available": False, "reason": "test"})
    universe = [f"L{i}" for i in range(2 * MIN_SAMPLE)]
    card["overall"] = {**score_trades(trades), "recall": score_recall(trades, universe)}
    return card


def test_git_commit_reads_head_and_tolerates_a_missing_git():
    with patch("baseline_scorecard.subprocess.run") as run:
        run.return_value.stdout = "abc123\n"
        assert baseline_scorecard.git_commit() == "abc123"
        run.side_effect = OSError("no git")
        assert baseline_scorecard.git_commit() is None


def test_main_runs_the_cli(monkeypatch, tmp_path):
    _fake_database(monkeypatch, labels={}, profitable={})
    argv = ["--decisions", str(_write_cli_log(tmp_path)), "--out-dir", str(tmp_path), "--no-mlflow"]

    assert baseline_scorecard.main(argv) == 0
    assert (tmp_path / "baseline_scorecard.json").exists()


@patch("baseline_scorecard.MlflowClient")
def test_log_to_mlflow_logs_params_metrics_and_artifacts(mock_client_cls, tmp_path):
    client = MagicMock()
    client.get_experiment_by_name.return_value = None
    client.create_experiment.return_value = "7"
    client.create_run.return_value.info.run_id = "run-1"
    mock_client_cls.return_value = client
    artifact = tmp_path / "baseline_scorecard.json"
    artifact.write_text("{}", encoding="utf-8")

    assert log_to_mlflow(_scored_card(), [artifact]) == "run-1"

    client.create_experiment.assert_called_once_with("baseline-scorecard")
    params = {call.args[1]: call.args[2] for call in client.log_param.call_args_list}
    assert params["label_version"] == "v1"
    assert params["config.z_score_threshold"] == "-2.0"
    metrics = {call.args[1]: call.args[2] for call in client.log_metric.call_args_list}
    assert metrics["precision"] == 1.0
    assert metrics["recall"] == 0.5
    assert metrics["trades"] == MIN_SAMPLE
    client.log_artifact.assert_called_once_with("run-1", str(artifact))
    client.set_terminated.assert_called_once_with("run-1", status="FINISHED")


@patch("baseline_scorecard.MlflowClient")
def test_log_to_mlflow_leaves_insufficient_metrics_out_and_marks_a_failed_run(mock_client_cls):
    client = MagicMock()
    client.create_run.return_value.info.run_id = "run-2"
    client.log_artifact.side_effect = OSError("disk")
    mock_client_cls.return_value = client

    with pytest.raises(OSError):
        log_to_mlflow(_card_with({"available": False, "reason": "test"}), ["missing.json"])

    metrics = {call.args[1] for call in client.log_metric.call_args_list}
    assert "precision" not in metrics and "trades" in metrics
    client.set_terminated.assert_called_once_with("run-2", status="FAILED")
