import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import cfo_calibration
import pytest
from baseline_scorecard import (
    MIN_SAMPLE,
    OUTCOME_LABELED,
    OUTCOME_NEUTRAL,
    TICK_KIND_LISTING,
    TICK_KIND_REST_SNAPSHOT,
    DecisionLog,
    LoggedDecision,
    Trade,
)
from cfo_calibration import (
    PROMPT_VERSION,
    STATUS_ERROR,
    STATUS_OK,
    VARIANT_ADVERSARIAL,
    VARIANT_NEUTRAL,
    BaselineSnapshot,
    MarketHistory,
    QuotaExhaustedError,
    TradeCase,
    Verdict,
    ask_cfo,
    brier_score,
    build_report,
    case_key,
    decision_features,
    estimate_tokens,
    fetch_baseline_snapshots,
    fetch_listing_details,
    group_summary,
    latest_verdicts,
    load_cases,
    paired_veto_difference,
    parse_verdict,
    read_verdicts,
    reliability,
    render_case_prompt,
    render_markdown,
    roc_auc,
    run_tool,
    run_verdicts,
    sample_cases,
    score_variant,
)
from label_outcomes import FeedSale
from pydantic import ValidationError

NAME = "AK-47 | Slate (Field-Tested)"
OTHER = "AK-47 | Redline (Field-Tested)"
LATER_ONLY = "AK-47 | Head Shot (Field-Tested)"
DECIDED_AT = datetime(2026, 9, 27, 12, 0)
HOUR = timedelta(hours=1)


def baseline(latest=1000):
    return {
        "latest_price_cents": latest,
        "rolling_30d_avg_cents": latest,
        "rolling_90d_avg_cents": latest,
        "volatility_cents": 50,
        "support_floor_cents": latest - 100,
        "avg_volume_30d": 5.0,
        "drift_percent": 0.0,
    }


def sale(name, at, price, listing_id=None):
    return FeedSale(
        listing_id=listing_id or f"{name}-{at.isoformat()}",
        market_hash_name=name,
        feed_name=name,
        price_cents=price,
        seen_at=at,
    )


def history():
    """A build before the decision and one after it, and sales and listings on both sides of it."""
    builds = [
        BaselineSnapshot(build_id=1, built_at=DECIDED_AT - 6 * HOUR, rows={NAME: baseline(1000), OTHER: baseline(3000)}),
        BaselineSnapshot(build_id=2, built_at=DECIDED_AT + HOUR, rows={NAME: baseline(9999), LATER_ONLY: baseline(5000)}),
    ]
    sold = [
        sale(NAME, DECIDED_AT - timedelta(days=7), 950),
        sale(NAME, DECIDED_AT - timedelta(days=8), 111),
        sale(NAME, DECIDED_AT - 2 * HOUR, 980),
        sale(NAME, DECIDED_AT, 7777),
        sale(NAME, DECIDED_AT + HOUR, 8888),
    ]
    listed = [
        sale(NAME, DECIDED_AT - HOUR, 990),
        sale(NAME, DECIDED_AT - 25 * HOUR, 123),
        sale(NAME, DECIDED_AT + HOUR, 555),
    ]
    return MarketHistory.build(builds, sold, listed)


def make_trade(listing_id="L1", margin=100, at=DECIDED_AT, name=NAME, price=900):
    trade = Trade(
        decided_at=at,
        tick_kind=TICK_KIND_LISTING if listing_id else TICK_KIND_REST_SNAPSHOT,
        listing_id=listing_id,
        market_hash_name=name,
        price_cents=price,
        rule="support_floor",
        z_source="hybrid",
        build_id=1,
    )
    trade.outcome = OUTCOME_LABELED
    trade.net_margin_cents = margin
    trade.is_profitable = margin >= 0
    return trade


def make_case(listing_id="L1", margin=100, name=NAME, at=DECIDED_AT):
    trade = make_trade(listing_id=listing_id, margin=margin, name=name, at=at)
    listing = {"float_value": 0.25, "pattern": 7, "stickers": [{"name": "Crown (Foil)"}]} if listing_id else None
    features = {
        "z_score": -2.5,
        "z_source": "hybrid",
        "window_size": 20,
        "mean_cents": 1100.0,
        "dre_rule": "support_floor",
        "estimated_net_profit_cents": 12,
    }
    return TradeCase(key=case_key(trade), trade=trade, features=features, listing=listing)


# --- Look-ahead guard: the tools see only what was known at the decision time ---


def test_view_uses_the_newest_build_fetched_by_the_decision():
    view = history().as_of(DECIDED_AT)

    assert view.build is not None and view.build.build_id == 1
    assert view.baseline(NAME)["latest_price_cents"] == 1000
    assert history().as_of(DECIDED_AT + 2 * HOUR).build.build_id == 2


def test_view_before_any_build_has_no_baseline():
    view = history().as_of(DECIDED_AT - 7 * HOUR)

    assert view.build is None
    assert view.baseline(NAME) is None
    assert view.search_baselines("AK") == []
    assert view.market_floor(NAME)["baseline_built_at"] is None


def test_market_floor_holds_only_events_before_the_decision():
    floor = history().as_of(DECIDED_AT).market_floor(NAME)

    sales = floor["sales_last_7_days"]
    assert sales["count"] == 2  # The sale 7 days back counts; the one at the decision and later ones do not
    assert sales["min_cents"] == 950 and sales["max_cents"] == 980
    assert [entry["hours_before_trade"] for entry in sales["latest"]] == [168.0, 2.0]
    assert floor["listings_last_24_hours"] == {"count": 1, "lowest_prices_cents": [990]}
    assert floor["baseline"]["latest_price_cents"] == 1000
    assert "7777" not in json.dumps(floor) and "8888" not in json.dumps(floor) and "9999" not in json.dumps(floor)


def test_search_reads_the_build_in_effect():
    view = history().as_of(DECIDED_AT)

    assert [row["market_hash_name"] for row in view.search_baselines("ak-47")] == [OTHER, NAME]
    assert view.search_baselines("Head Shot") == []
    assert view.search_baselines("  ") == []


@pytest.mark.asyncio
async def test_tools_return_only_point_in_time_data():
    view = history().as_of(DECIDED_AT)

    floor = json.loads(await run_tool(view, "fetch_market_floor", {"market_hash_name": NAME}))
    search = json.loads(await run_tool(view, "search_item_baselines", {"query": "Head Shot"}))
    float_check = json.loads(await run_tool(view, "verify_float_value", {"market_hash_name": NAME, "float_value": 0.16}))

    assert floor["as_of"] == DECIDED_AT.isoformat()
    assert floor["sales_last_7_days"]["count"] == 2
    assert search == {"query": "Head Shot", "results": []}
    assert float_check["wear_tier"] == "Field-Tested"


@pytest.mark.asyncio
async def test_bad_tool_calls_return_an_error_the_model_can_read():
    view = history().as_of(DECIDED_AT)

    assert "Invalid arguments" in json.loads(await run_tool(view, "fetch_market_floor", {}))["error"]
    assert "Unknown tool" in json.loads(await run_tool(view, "search_macro_trends", {"query": "crash"}))["error"]


def test_tool_descriptions_do_not_promise_news_or_live_data():
    descriptions = " ".join(schema["function"]["description"] for schema in cfo_calibration.TOOL_SCHEMAS).lower()

    assert "live" not in descriptions
    assert "not news" in descriptions


def test_prompt_shows_decision_time_facts_and_no_outcome():
    case = make_case(margin=4321)
    prompt = render_case_prompt(case)

    assert NAME in prompt and "$9.00" in prompt and "-2.5" in prompt
    assert "Float value: 0.250000" in prompt and "Crown (Foil)" in prompt
    assert "$43.21" not in prompt and "4321" not in prompt and "True" not in prompt
    assert "lowest ask" in render_case_prompt(make_case(listing_id=None))
    assert "Float value" not in render_case_prompt(make_case(listing_id=None))


def test_prompt_handles_a_listing_without_float_or_features():
    case = make_case()
    case.listing = {"float_value": None, "pattern": None, "stickers": []}
    case.features = {}

    prompt = render_case_prompt(case)

    assert "Float value: none" in prompt and "Stickers: none" in prompt
    assert "Bot's estimated profit after the seller fee: unknown" in prompt


# --- Parsing and the tool loop ---


def test_parse_verdict_accepts_fenced_or_wrapped_json():
    fenced = parse_verdict('```json\n{"probability_of_profit": 0.4, "reasoning": "ok"}\n```')
    wrapped = parse_verdict('Here: {"probability_of_profit": 1, "reasoning": "sure"} done')

    assert fenced.probability_of_profit == 0.4
    assert wrapped.probability_of_profit == 1.0


def test_parse_verdict_rejects_missing_or_out_of_range_answers():
    with pytest.raises(ValueError):
        parse_verdict("no json here")
    with pytest.raises(ValidationError):
        parse_verdict('{"probability_of_profit": 70, "reasoning": "x"}')


def completion(content=None, tool_calls=None, finish_reason="stop", prompt_tokens=100, completion_tokens=20):
    message = MagicMock()
    message.role = "assistant"
    message.content = content
    message.tool_calls = tool_calls
    choice = SimpleNamespace(message=message, finish_reason=finish_reason)
    return SimpleNamespace(
        choices=[choice], usage=SimpleNamespace(prompt_tokens=prompt_tokens, completion_tokens=completion_tokens)
    )


def tool_call(name, arguments, call_id="call-1"):
    call = MagicMock()
    call.id = call_id
    call.function.name = name
    call.function.arguments = arguments
    call.model_dump.return_value = {"id": call_id, "type": "function", "function": {"name": name, "arguments": arguments}}
    return call


@pytest.fixture
def fake_llm(monkeypatch):
    client = MagicMock()
    monkeypatch.setattr(cfo_calibration.cfo, "openai_client", client)

    async def run_inline(func, *args, **kwargs):
        return func(*args, **kwargs)

    monkeypatch.setattr(cfo_calibration.asyncio, "to_thread", run_inline)
    sleeps = []

    async def no_sleep(seconds):
        sleeps.append(seconds)

    monkeypatch.setattr(cfo_calibration.cfo, "_sleep", no_sleep)
    client.sleeps = sleeps
    return client


@pytest.mark.asyncio
async def test_ask_cfo_runs_tools_against_the_view_and_reads_the_answer(fake_llm):
    fake_llm.chat.completions.create.side_effect = [
        completion(
            tool_calls=[tool_call("fetch_market_floor", json.dumps({"market_hash_name": NAME}))], finish_reason="tool_calls"
        ),
        completion('{"probability_of_profit": 0.35, "reasoning": "Sales sit near the buy price."}'),
    ]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")

    assert verdict.status == STATUS_OK and verdict.probability == 0.35
    assert verdict.tool_calls == ["fetch_market_floor"]
    assert (verdict.llm_calls, verdict.prompt_tokens, verdict.completion_tokens) == (2, 200, 40)
    second_call = fake_llm.chat.completions.create.call_args_list[1].kwargs
    assert second_call["model"] == "model-a" and second_call["temperature"] == 0
    assert second_call["messages"][0]["content"] == cfo_calibration.NEUTRAL_SYSTEM
    tool_message = next(message for message in second_call["messages"] if message["role"] == "tool")
    assert "8888" not in tool_message["content"] and json.loads(tool_message["content"])["sales_last_7_days"]["count"] == 2


@pytest.mark.asyncio
async def test_ask_cfo_asks_once_more_for_json(fake_llm):
    fake_llm.chat.completions.create.side_effect = [
        completion(
            "The trade looks bad.", tool_calls=[tool_call("verify_float_value", "not json")], finish_reason="tool_calls"
        ),
        completion("The trade looks bad."),
        completion('{"probability_of_profit": 0.1, "reasoning": "Bad."}'),
    ]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_ADVERSARIAL, "model-a")

    assert verdict.probability == 0.1
    calls = fake_llm.chat.completions.create.call_args_list
    assert calls[0].kwargs["messages"][0]["content"] == cfo_calibration.ADVERSARIAL_SYSTEM
    assert calls[2].kwargs["tool_choice"] == "none"


@pytest.mark.asyncio
async def test_ask_cfo_records_a_failure_as_an_error_verdict(fake_llm):
    fake_llm.chat.completions.create.side_effect = [completion("nope"), completion("still nope")]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")

    assert verdict.status == STATUS_ERROR and verdict.probability is None
    assert verdict.reasoning.startswith("Evaluation failed")


@pytest.mark.asyncio
async def test_ask_cfo_waits_out_rate_limits_and_stops_on_quota(fake_llm):
    fake_llm.chat.completions.create.side_effect = [
        RuntimeError("Rate limit reached. Please try again in 2.5s"),
        completion('{"probability_of_profit": 0.5, "reasoning": "Even."}'),
    ]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")

    assert verdict.probability == 0.5
    assert fake_llm.sleeps == [3.5]

    fake_llm.chat.completions.create.side_effect = [RuntimeError("Limit 200000, used 200000 tokens per day (TPD)")]
    with pytest.raises(QuotaExhaustedError):
        await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")


@pytest.mark.asyncio
async def test_ask_cfo_gives_up_after_repeated_failures(fake_llm, monkeypatch):
    monkeypatch.setattr(cfo_calibration, "MAX_RATE_LIMIT_RETRIES", 2)
    fake_llm.chat.completions.create.side_effect = [RuntimeError("boom"), RuntimeError("boom")]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")

    assert verdict.status == STATUS_ERROR and "boom" in verdict.reasoning


# --- Verdict files ---


def verdict(key, probability, variant=VARIANT_NEUTRAL, model="model-a", status=STATUS_OK, version=PROMPT_VERSION):
    return Verdict(
        key=key,
        variant=variant,
        model=model,
        prompt_version=version,
        status=status,
        probability=probability,
        reasoning="",
        prompt_tokens=1000,
        completion_tokens=100,
        llm_calls=2,
        tool_calls=["fetch_market_floor"],
    )


def test_latest_verdicts_prefers_a_success_and_ignores_old_prompts():
    verdicts = [
        verdict("a", None, status=STATUS_ERROR),
        verdict("a", 0.4),
        verdict("a", None, status=STATUS_ERROR),
        verdict("b", 0.9, version=PROMPT_VERSION - 1),
    ]

    latest = latest_verdicts(verdicts)

    assert list(latest) == [(VARIANT_NEUTRAL, "a", "model-a", PROMPT_VERSION)]
    assert latest[(VARIANT_NEUTRAL, "a", "model-a", PROMPT_VERSION)].probability == 0.4


@pytest.mark.asyncio
async def test_run_verdicts_resumes_without_asking_twice(tmp_path, monkeypatch):
    asked = []

    async def fake_ask(case, view, variant, model):
        asked.append((variant, case.key, view.moment))
        status = STATUS_ERROR if case.key == "listing:L2" and len(asked) < 3 else STATUS_OK
        return verdict(case.key, None if status == STATUS_ERROR else 0.6, variant=variant, status=status)

    monkeypatch.setattr(cfo_calibration, "ask_cfo", fake_ask)
    cases = [make_case("L1"), make_case("L2")]
    out = tmp_path / "verdicts.jsonl"

    first = await run_verdicts(cases, history(), [VARIANT_NEUTRAL], "model-a", out)
    second = await run_verdicts(cases, history(), [VARIANT_NEUTRAL], "model-a", out)

    assert first == {"asked": 2, "skipped": 0, "errors": 1}
    assert second == {"asked": 1, "skipped": 1, "errors": 0}
    assert asked[0][2] == DECIDED_AT
    assert len(read_verdicts(out)) == 3
    assert read_verdicts(tmp_path / "missing.jsonl") == []


# --- Trades ---


def test_decision_features_come_from_the_first_scorable_approval():
    row = {
        "type": "decision",
        "timestamp": int((DECIDED_AT - datetime(1970, 1, 1)).total_seconds()),
        "venue": "skinport",
        "listing_id": "L1",
        "event_type": "listed",
        "market_hash_name": NAME,
        "price_cents": 900,
        "approve": True,
        "reason": "support_floor",
        "score": -2.5,
        "features": {"z_source": "hybrid", "estimated_net_profit_cents": 12},
    }
    later = {**row, "score": -9.0, "timestamp": row["timestamp"] + 60}
    rejected = {**row, "approve": False, "listing_id": "L9"}
    log = DecisionLog(
        header={"strategy": "zscore_dre_sweep"},
        decisions=[LoggedDecision(row=r, build_id=1, look_ahead=False) for r in (rejected, row, later)],
        summary=None,
        sha256="x",
        venue="skinport",
        start=datetime(2026, 9, 26, 17, 30),
        end=datetime(2026, 10, 1),
    )

    features = decision_features(log, [make_trade("L1")])

    assert features == {
        "listing:L1": {
            "z_score": -2.5,
            "dre_rule": "support_floor",
            "event_type": "listed",
            "z_source": "hybrid",
            "estimated_net_profit_cents": 12,
        }
    }


def test_case_keys_name_the_listing_or_the_snapshot():
    assert case_key(make_trade("L1")) == "listing:L1"
    assert case_key(make_trade(None)) == f"rest:{NAME}:900:{DECIDED_AT.isoformat()}"


def test_sample_cases_is_seeded_and_keeps_decision_order():
    cases = [make_case(f"L{index}") for index in range(20)]

    first = sample_cases(cases, 5)

    assert first == sample_cases(cases, 5)
    assert len(first) == 5
    assert [cases.index(case) for case in first] == sorted(cases.index(case) for case in first)
    assert sample_cases(cases, None) == cases
    assert sample_cases(cases, 50) == cases


def test_estimate_tokens_sizes_one_variant():
    estimate = estimate_tokens([make_case(), make_case("L2")], history())

    assert estimate["trades"] == 2
    assert estimate["input_tokens_per_variant"] == pytest.approx(2 * estimate["input_tokens_per_trade"])
    assert estimate_tokens([], history())["input_tokens_per_trade"] == 0.0


# --- Loading ---


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class QueueEngine:
    """Answers each query with the next prepared result."""

    def __init__(self, *results):
        self.results = list(results)
        self.executed = []

    def connect(self):
        @asynccontextmanager
        async def _ctx():
            yield self

        return _ctx()

    async def execute(self, stmt, params=None):
        self.executed.append((str(stmt), params))
        return _Result(self.results.pop(0))


@pytest.mark.asyncio
async def test_fetch_listing_details_reads_the_first_listed_tick(monkeypatch):
    engine = QueueEngine([("L1", 0.2, 5, [])])
    monkeypatch.setattr(cfo_calibration, "async_engine", engine)

    assert await fetch_listing_details(["L1"]) == {"L1": {"float_value": 0.2, "pattern": 5, "stickers": []}}
    assert "event_type = 'listed'" in engine.executed[0][0]
    assert await fetch_listing_details([]) == {}


@pytest.mark.asyncio
async def test_fetch_baseline_snapshots_stops_at_the_last_decision(monkeypatch):
    row = (1, NAME, 1000, 1000, 1000, 50, 900, 5.0, 0.0, 1000, 5, 1000, 30, 800, 150)
    engine = QueueEngine([(1, DECIDED_AT - HOUR)], [row])
    monkeypatch.setattr(cfo_calibration, "async_engine", engine)

    builds = await fetch_baseline_snapshots(DECIDED_AT)

    assert builds[0].build_id == 1 and builds[0].rows[NAME]["support_floor_cents"] == 900
    assert engine.executed[0][1] == {"venue": "skinport", "until": DECIDED_AT}
    assert "built_at <= :until" in engine.executed[0][0]

    monkeypatch.setattr(cfo_calibration, "async_engine", QueueEngine([]))
    assert await fetch_baseline_snapshots(DECIDED_AT) == []


@pytest.mark.asyncio
async def test_load_cases_keeps_labeled_trades_and_cuts_market_data_at_the_last_decision(monkeypatch):
    listing_row = {
        "type": "decision",
        "timestamp": int((DECIDED_AT - datetime(1970, 1, 1)).total_seconds()),
        "venue": "skinport",
        "listing_id": "L1",
        "event_type": "listed",
        "market_hash_name": NAME,
        "price_cents": 900,
        "approve": True,
        "reason": "support_floor",
        "score": -2.5,
        "features": {"z_source": "hybrid"},
    }
    neutral_row = {**listing_row, "listing_id": "L2", "timestamp": listing_row["timestamp"] + 60}
    snapshot_row = {
        **listing_row,
        "listing_id": None,
        "event_type": None,
        "price_cents": 800,
        "timestamp": listing_row["timestamp"] + 120,
    }
    log = DecisionLog(
        header={"strategy": "zscore_dre_sweep"},
        decisions=[LoggedDecision(row=r, build_id=1, look_ahead=False) for r in (listing_row, neutral_row, snapshot_row)],
        summary=None,
        sha256="x",
        venue="skinport",
        start=datetime(2026, 9, 26, 17, 30),
        end=datetime(2026, 10, 1),
    )
    as_of = datetime(2026, 11, 1)
    fetched = []

    async def fake_labels(listing_ids, version):
        return {
            "L1": {"is_profitable": False, "resale_net_margin_cents": -50, "label_available_at": DECIDED_AT},
            "L2": {"is_profitable": None, "resale_net_margin_cents": None, "label_available_at": DECIDED_AT},
        }

    async def fake_sales(event_type, start, end, names=None):
        fetched.append((event_type, start, end))
        if event_type == "sold" and end > DECIDED_AT + timedelta(days=1):
            return [sale(NAME, DECIDED_AT + timedelta(days=8, hours=index), 1200, f"S{index}") for index in range(3)]
        return [sale(NAME, DECIDED_AT - HOUR, 950)]

    async def fake_details(listing_ids):
        return {"L1": {"float_value": 0.2, "pattern": 1, "stickers": []}}

    async def fake_builds(until):
        fetched.append(("builds", until))
        return [BaselineSnapshot(build_id=1, built_at=DECIDED_AT - HOUR, rows={NAME: baseline()})]

    monkeypatch.setattr(cfo_calibration, "fetch_listing_labels", fake_labels)
    monkeypatch.setattr(cfo_calibration, "fetch_feed_sales", fake_sales)
    monkeypatch.setattr(cfo_calibration, "fetch_listing_details", fake_details)
    monkeypatch.setattr(cfo_calibration, "fetch_baseline_snapshots", fake_builds)

    cases, market = await load_cases(log, as_of)

    assert [case.key for case in cases] == ["listing:L1", f"rest:{NAME}:800:{(DECIDED_AT + timedelta(minutes=2)).isoformat()}"]
    assert cases[0].trade.net_margin_cents == -50 and cases[0].listing["float_value"] == 0.2
    assert cases[1].trade.is_profitable is True and cases[1].listing is None
    assert cases[0].features["z_score"] == -2.5
    last_decision = DECIDED_AT + timedelta(minutes=2)
    assert ("listed", DECIDED_AT - timedelta(hours=24), last_decision) in fetched
    assert ("sold", DECIDED_AT - timedelta(days=7), last_decision) in fetched
    assert ("builds", last_decision) in fetched
    assert market.as_of(DECIDED_AT).market_floor(NAME)["sales_last_7_days"]["count"] == 1


@pytest.mark.asyncio
async def test_load_cases_without_labeled_trades(monkeypatch):
    async def no_labels(listing_ids, version):
        return {}

    monkeypatch.setattr(cfo_calibration, "fetch_listing_labels", no_labels)

    async def no_details(listing_ids):
        return {}

    monkeypatch.setattr(cfo_calibration, "fetch_listing_details", no_details)
    log = DecisionLog(
        header={"strategy": "zscore_dre_sweep"},
        decisions=[],
        summary=None,
        sha256="x",
        venue="skinport",
        start=datetime(2026, 9, 26, 17, 30),
        end=datetime(2026, 10, 1),
    )

    cases, market = await load_cases(log, datetime(2026, 11, 1))

    assert cases == [] and market.builds == []


# --- Scoring ---


def test_brier_and_auc_known_values():
    assert brier_score([1.0, 0.0], [True, False]) == 0.0
    assert brier_score([0.5, 0.5], [True, False]) == 0.25
    assert roc_auc([0.9, 0.1], [True, False]) == 1.0
    assert roc_auc([0.5, 0.5], [True, False]) == 0.5
    assert roc_auc([0.2, 0.8], [True, False]) == 0.0
    assert roc_auc([0.2], [True]) is None


def test_reliability_bins_forecasts_and_outcomes():
    rows = reliability([0.1, 0.15, 0.9, 1.0], [False, True, True, True])

    assert [row["trades"] for row in rows] == [2, 0, 0, 0, 2]
    assert rows[0]["mean_forecast"] == pytest.approx(0.125)
    assert rows[0]["observed"]["value"] == 0.5
    assert rows[1]["observed"] is None
    assert rows[4]["observed"]["value"] == 1.0


def test_group_summary_gives_intervals_only_at_the_minimum_sample():
    small = group_summary([make_trade(margin=100), make_trade(margin=-300)])
    large = group_summary([make_trade(margin=100)] * MIN_SAMPLE)

    assert small == {"trades": 2, "profitable": 1, "net_pnl_cents": -200, "precision": None, "mean_pnl": None}
    assert large["precision"]["value"] == 1.0
    assert large["mean_pnl"]["value"] == 100


def test_paired_veto_difference_is_the_vetoed_pnl_with_an_item_bootstrap():
    trades = [make_trade(margin=-500, name="A"), make_trade(margin=200, name="B"), make_trade(margin=-100, name="C")]

    paired = paired_veto_difference(trades, {0, 2})

    assert paired["difference_cents"] == 600
    assert paired["items"] == 3
    assert paired["low_cents"] <= 600 <= paired["high_cents"]
    assert paired == paired_veto_difference(trades, {0, 2})
    assert paired_veto_difference(trades, set())["difference_cents"] == 0


def scored_cases(count=40):
    """Half profitable; the forecast is high for the profitable half, so the CFO looks informative."""
    cases = []
    verdicts = {}
    for index in range(count):
        profitable = index % 2 == 0
        case = make_case(f"L{index}", margin=150 if profitable else -250, name=f"item {index % 10}")
        cases.append(case)
        verdicts[case.key] = verdict(case.key, 0.8 if profitable else 0.3)
    return cases, verdicts


def test_score_variant_measures_calibration_and_the_veto():
    cases, verdicts = scored_cases()
    verdicts[cases[0].key] = verdict(cases[0].key, None, status=STATUS_ERROR)
    del verdicts[cases[1].key]

    scored = score_variant(cases, verdicts)

    assert (scored["trades"], scored["answered"], scored["errors"], scored["missing"]) == (40, 38, 1, 1)
    assert scored["auc"] == 1.0
    assert scored["brier_skill"] > 0
    gate = scored["gates"][0]
    assert gate["threshold"] == 0.5
    assert gate["vetoed"]["trades"] == 19  # The failed and missing verdicts keep their trades
    assert gate["paired"]["difference_cents"] == 19 * 250
    assert scored["dre_alone"]["trades"] == 40


def test_score_variant_without_answers():
    cases, _ = scored_cases(4)

    assert score_variant(cases, {}) == {"trades": 4, "answered": 0, "errors": 0, "missing": 4}


def test_build_report_and_markdown(monkeypatch):
    monkeypatch.setattr(cfo_calibration, "git_commit", lambda: "abc")
    cases, by_key = scored_cases()
    cases[3].trade.outcome = OUTCOME_NEUTRAL  # Only the tick kind counts read it; every case here is labeled
    verdicts = list(by_key.values()) + [verdict(case.key, 0.6, variant=VARIANT_ADVERSARIAL) for case in cases]
    verdicts.append(verdict("other", 0.6, model="model-b"))

    report = build_report(cases, verdicts, "model-a", {"label_version": "v1", "as_of": "2026-10-13"}, (0.15, 0.6))

    assert set(report["variants"]) == {VARIANT_ADVERSARIAL, VARIANT_NEUTRAL}
    usage = report["variants"][VARIANT_NEUTRAL]["usage"]
    assert usage["prompt_tokens"] == 40_000 and usage["llm_calls"] == 80
    assert usage["cost_usd"] == pytest.approx((40_000 * 0.15 + 4_000 * 0.6) / 1_000_000)
    markdown = render_markdown(report)
    assert "### neutral: reliability" in markdown and "kept at 0.7" in markdown and "Paired with the DRE alone" in markdown
    assert report["tick_kinds"] == {TICK_KIND_LISTING: 40, TICK_KIND_REST_SNAPSHOT: 0}

    no_prices = build_report(cases, verdicts, "model-a", {})
    assert "cost_usd" not in no_prices["variants"][VARIANT_NEUTRAL]["usage"]
    assert "| unknown |" in render_markdown(no_prices)


def test_markdown_lists_a_prompt_without_answers(monkeypatch):
    monkeypatch.setattr(cfo_calibration, "git_commit", lambda: None)
    cases, _ = scored_cases(2)
    failed = [verdict(case.key, None, status=STATUS_ERROR) for case in cases]

    markdown = render_markdown(build_report(cases, failed, "model-a", {}))

    assert "| neutral | 0 | 2 |" in markdown


# --- Command line ---


@pytest.fixture
def fake_inputs(monkeypatch):
    cases, _ = scored_cases(4)

    async def fake_load(log, as_of):
        return cases, history()

    monkeypatch.setattr(cfo_calibration, "read_decision_log", lambda path: SimpleNamespace(sha256="abc"))
    monkeypatch.setattr(cfo_calibration, "load_cases", fake_load)
    monkeypatch.setattr(cfo_calibration, "git_commit", lambda: None)
    return cases


def test_main_dry_run_calls_no_llm(fake_inputs, tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(cfo_calibration, "run_verdicts", MagicMock(side_effect=AssertionError("no LLM in a dry run")))

    code = cfo_calibration.main(
        ["run", "--decisions", "log.jsonl", "--as-of", "2026-10-13T01:40:07", "--out", str(tmp_path), "--dry-run"]
    )

    assert code == 0
    assert json.loads(capsys.readouterr().out)["trades"] == 4


def test_main_run_and_report(fake_inputs, tmp_path, capsys, monkeypatch):
    async def fake_run(cases, market, variants, model, out_path):
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as out:
            for case in cases:
                out.write(json.dumps(verdict(case.key, 0.7, model=model).__dict__) + "\n")
        return {"asked": len(cases), "skipped": 0, "errors": 0, "variants": variants}

    monkeypatch.setattr(cfo_calibration, "run_verdicts", fake_run)
    common = ["--decisions", "log.jsonl", "--as-of", "2026-10-13T01:40:07", "--out", str(tmp_path)]

    assert cfo_calibration.main(["run", *common, "--variants", "neutral"]) == 0
    assert json.loads(capsys.readouterr().out)["variants"] == ["neutral"]
    assert cfo_calibration.main(["report", *common, "--input-usd-per-mtok", "0.1", "--output-usd-per-mtok", "0.5"]) == 0

    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["model"] == "openai/gpt-oss-120b" and report["decision_log_sha256"] == "abc"
    assert report["variants"]["neutral"]["answered"] == 4
    assert (tmp_path / "report.md").exists()


def test_main_rejects_unknown_variants_and_stops_on_quota(fake_inputs, tmp_path, monkeypatch):
    common = ["--decisions", "log.jsonl", "--as-of", "2026-10-13T01:40:07", "--out", str(tmp_path)]
    with pytest.raises(SystemExit, match="Unknown variants"):
        cfo_calibration.main(["run", *common, "--variants", "optimistic"])

    async def quota(*args):
        raise QuotaExhaustedError("tokens per day")

    monkeypatch.setattr(cfo_calibration, "run_verdicts", quota)
    assert cfo_calibration.main(["run", *common]) == 2


def test_only_a_daily_limit_counts_as_quota():
    per_minute = RuntimeError(
        "Rate limit reached on tokens per minute (TPM): Limit 8000. Need more tokens? Upgrade to Dev Tier today"
    )

    assert not cfo_calibration.is_daily_quota(per_minute)
    assert cfo_calibration.is_daily_quota(RuntimeError("Limit 200000, Used 199000 on tokens per day (TPD)"))


class _BadRequest(Exception):
    status_code = 400


@pytest.mark.asyncio
async def test_a_rejected_request_is_not_retried(fake_llm):
    fake_llm.chat.completions.create.side_effect = [_BadRequest("context too long")]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")

    assert verdict.status == STATUS_ERROR
    assert fake_llm.chat.completions.create.call_count == 1 and fake_llm.sleeps == []


@pytest.mark.asyncio
async def test_a_rejected_format_tool_call_asks_again_without_tools(fake_llm):
    fake_llm.chat.completions.create.side_effect = [
        _BadRequest("tool_use_failed: attempted to call tool 'JSON' which was not in request.tools"),
        completion('{"probability_of_profit": 0.6, "reasoning": "Fine."}'),
    ]

    verdict = await ask_cfo(make_case(), history().as_of(DECIDED_AT), VARIANT_NEUTRAL, "model-a")

    assert verdict.probability == 0.6
    assert fake_llm.chat.completions.create.call_args_list[1].kwargs["tool_choice"] == "none"


def test_a_partial_run_is_scored_on_the_trades_both_prompts_answered(monkeypatch):
    monkeypatch.setattr(cfo_calibration, "git_commit", lambda: None)
    cases, by_key = scored_cases()
    adversarial = [verdict(case.key, 0.6, variant=VARIANT_ADVERSARIAL) for case in cases[:10]]

    report = build_report(cases, list(by_key.values()) + adversarial, "model-a", {})

    assert (report["trades"], report["trades_in_range"]) == (10, 40)
    assert report["variants"][VARIANT_NEUTRAL]["trades"] == 10
    assert "10 of 40 labeled trades scored" in render_markdown(report)


@pytest.mark.asyncio
async def test_run_verdicts_asks_every_prompt_about_a_trade_before_the_next(tmp_path, monkeypatch):
    asked = []

    async def fake_ask(case, view, variant, model):
        asked.append((case.key, variant))
        return verdict(case.key, 0.5, variant=variant)

    monkeypatch.setattr(cfo_calibration, "ask_cfo", fake_ask)
    cases = [make_case(f"L{index}") for index in range(6)]

    await run_verdicts(cases, history(), [VARIANT_ADVERSARIAL, VARIANT_NEUTRAL], "model-a", tmp_path / "v.jsonl")

    assert [variant for _, variant in asked] == [VARIANT_ADVERSARIAL, VARIANT_NEUTRAL] * 6
    assert asked[0][0] == asked[1][0]
    assert [key for key, _ in asked[::2]] != [case.key for case in cases]  # Seeded random order, not decision order
