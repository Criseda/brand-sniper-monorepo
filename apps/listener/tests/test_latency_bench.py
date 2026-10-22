import asyncio
import gzip
import json
from pathlib import Path

import pytest
from aiohttp import WSMsgType
from aiohttp.test_utils import TestClient, TestServer
from backtest.sources import BaselineSnapshot, RecordedFeedEvent
from latency_bench import probes
from latency_bench.__main__ import aggregate, build_parser, markdown_report
from latency_bench.feed_server import (
    CONNECT,
    EVENT,
    OPEN_PACKET,
    FeedServer,
    decode_packet,
    encode_event,
    encode_packet,
    prepare_frames,
)
from latency_bench.fixture import (
    Fixture,
    deidentify,
    deidentify_sale,
    fixture_baselines,
    recorded_event_sizes,
    select_templates,
    write_fixture,
)
from latency_bench.listener import listener_main
from latency_bench.run import (
    RunConfig,
    _sidecar_timings,
    clock_offsets,
    decision_rate,
    prepare_redis,
    summarize,
    wait_for_decisions,
    write_schedule,
)
from latency_bench.scenarios import (
    BARGAIN_PRICE_FACTOR,
    FIRST_PRODUCT_ID,
    SCENARIOS,
    WARMUP_PRICES_PER_ITEM,
    Scenario,
    ScenarioBuilder,
    listed_count,
)
from latency_bench.stats import (
    SidecarTiming,
    clock_order_violations,
    distribution_ms,
    listing_stages,
    percentile,
    stage_breakdown,
)
from models import MarketTick, TickKind
from shared_utils import edge_baselines_key, edge_sticker_prices_key

AK = "AK-47 | Slate (Field-Tested)"
AWP = "AWP | Atheris (Minimal Wear)"


def _sale(name: str, product_id: int, price: int = 1000, **extra) -> dict:
    return {"marketHashName": name, "productId": product_id, "salePrice": price, "version": "default", "stickers": [], **extra}


def _baseline(latest: int, floor: int) -> dict:
    return {"latest_price_cents": latest, "support_floor_cents": floor, "rolling_30d_avg_cents": latest}


def _fixture() -> Fixture:
    return Fixture(
        listed_sales=[_sale(AK, 1, 1000), _sale(AWP, 2, 5000)],
        sold_sales=[_sale(AK, 3, 990)],
        baselines=BaselineSnapshot(baselines={AK: _baseline(1000, 900), AWP: _baseline(5000, 4500)}),
        event_sizes={"listed": {1: 3, 5: 1}, "sold": {2: 2}},
    )


# --- Fixture ---


def test_deidentify_keeps_length_and_type():
    assert deidentify("76561199571197899") == "00000000000000000"
    assert deidentify("steam://run/730") == "xxxxx://xxx/000"
    assert deidentify(489690762) == 100000000
    assert deidentify(None) is None
    assert deidentify(True) is True
    assert deidentify(["a1", {"b": "c2"}]) == ["x0", {"b": "x0"}]
    assert deidentify(1.5) == 1.5


def test_deidentify_sale_replaces_only_identifying_fields():
    sale = _sale(AK, 44175289, steamid="76561199571197899", assetid="44558511046", customName=None)
    cleaned = deidentify_sale(sale)
    assert cleaned["steamid"] == "00000000000000000"
    assert cleaned["assetid"] == "00000000000"
    assert cleaned["marketHashName"] == AK
    assert cleaned["productId"] == 44175289
    assert len(json.dumps(cleaned)) == len(json.dumps(sale))


def test_templates_sizes_and_baselines_from_recorded_events():
    events = [
        RecordedFeedEvent(1, {"eventType": "listed", "sales": [_sale(AK, 1), _sale("No Baseline", 2), _sale(AK, 3)]}),
        RecordedFeedEvent(2, {"eventType": "sold", "sales": [_sale(AK, 4), "not a sale"]}),
        RecordedFeedEvent(3, {"eventType": "listed", "sales": [_sale(AWP, 5, stickers=[{"name": "Sticker | A"}])]}),
        RecordedFeedEvent(4, {"eventType": "listed", "sales": []}),
    ]
    baselines = BaselineSnapshot(
        baselines={AK: _baseline(1000, 900), AWP: _baseline(5000, 4500)}, sticker_prices={"Sticker | A": 25, "Sticker | B": 9}
    )
    listed, sold = select_templates(events, baselines, max_items=5, max_sold=5)
    assert [sale["productId"] for sale in listed] == [1, 5]
    assert [sale["productId"] for sale in sold] == [4]
    assert recorded_event_sizes(events) == {"listed": {1: 1, 3: 1}, "sold": {2: 1}}
    assert fixture_baselines(listed, baselines).sticker_prices == {"Sticker | A": 25}


def test_fixture_round_trip(tmp_path: Path):
    fixture = _fixture()
    write_fixture(
        tmp_path, fixture.listed_sales, fixture.sold_sales, fixture.baselines, fixture.event_sizes, {"venue": "skinport"}
    )
    loaded = Fixture.load(tmp_path)
    assert loaded == fixture


def test_fixture_refuses_another_version(tmp_path: Path):
    fixture = _fixture()
    write_fixture(tmp_path, fixture.listed_sales, fixture.sold_sales, fixture.baselines, fixture.event_sizes, {})
    metadata = json.loads((tmp_path / "fixture.json").read_text(encoding="utf-8"))
    metadata["version"] = 99
    (tmp_path / "fixture.json").write_text(json.dumps(metadata), encoding="utf-8")
    with pytest.raises(ValueError, match="version 99"):
        Fixture.load(tmp_path)


def test_committed_fixture_loads():
    fixture = Fixture.load(Path(__file__).resolve().parents[1] / "latency_bench" / "fixtures" / "skinport_v1")
    assert len(fixture.listed_sales) == 300
    assert all(sale["steamid"].strip("0") == "" for sale in fixture.listed_sales + fixture.sold_sales if "steamid" in sale)


# --- Scenarios ---


def _scenario(**overrides) -> Scenario:
    values = {
        "name": "test",
        "description": "test",
        "measured_listings": 50,
        "sales_per_event": None,
        "events_per_burst": 2,
        "event_gap_ms": 10.0,
        "burst_gap_ms": 100.0,
        "sold_events": True,
    }
    return Scenario(**{**values, **overrides})


def test_schedule_is_deterministic_and_product_ids_unique():
    first = ScenarioBuilder(_fixture(), seed=7).schedule(_scenario())
    second = ScenarioBuilder(_fixture(), seed=7).schedule(_scenario())
    assert first == second
    product_ids = [product_id for event in first for product_id in event.product_ids]
    assert len(product_ids) == len(set(product_ids))
    assert min(int(product_id) for product_id in product_ids) == FIRST_PRODUCT_ID


def test_schedule_counts_and_phases():
    events = ScenarioBuilder(_fixture(), seed=1).schedule(_scenario())
    assert listed_count(events, measured=False) == WARMUP_PRICES_PER_ITEM * 2
    assert listed_count(events, measured=True) == 50
    assert listed_count(events) == 50 + WARMUP_PRICES_PER_ITEM * 2
    measured = [event for event in events if event.measured]
    assert measured[0].offset_ms == 0.0
    assert any(event.event_type == "sold" for event in measured)
    listed_offsets = [event.offset_ms for event in measured if event.event_type == "listed"]
    assert listed_offsets[:3] == [0.0, 10.0, 100.0]


def test_fixed_size_scenario_without_sold_events():
    events = ScenarioBuilder(_fixture(), seed=1).measured(_scenario(sales_per_event=4, sold_events=False), measured_listings=10)
    assert [len(event.payload["sales"]) for event in events] == [4, 4, 2]
    assert all(event.event_type == "listed" for event in events)


def test_bargains_are_priced_below_the_support_floor(monkeypatch):
    monkeypatch.setattr("latency_bench.scenarios.BARGAIN_SHARE", 1.0)
    events = ScenarioBuilder(_fixture(), seed=1).measured(_scenario(sold_events=False), measured_listings=20)
    for event in events:
        for sale in event.payload["sales"]:
            floor = 900 if sale["marketHashName"] == AK else 4500
            latest = 1000 if sale["marketHashName"] == AK else 5000
            assert sale["salePrice"] == min(floor - 1, round(latest * BARGAIN_PRICE_FACTOR))


def test_builder_needs_listed_sales_and_sizes():
    fixture = _fixture()
    with pytest.raises(ValueError, match="no listed sales"):
        ScenarioBuilder(Fixture([], [], fixture.baselines, fixture.event_sizes), seed=1)
    builder = ScenarioBuilder(Fixture(fixture.listed_sales, [], fixture.baselines, {}), seed=1)
    with pytest.raises(ValueError, match="no recorded event sizes"):
        builder.measured(_scenario(), measured_listings=5)


def test_every_scenario_measures_at_least_ten_thousand_listings():
    assert all(scenario.measured_listings >= 10_000 for scenario in SCENARIOS.values())


# --- Feed server ---


def test_packets_round_trip():
    assert decode_packet(encode_event("saleFeed", {"a": 1})) == {"type": EVENT, "nsp": "/", "data": ["saleFeed", {"a": 1}]}
    assert decode_packet(encode_packet(CONNECT, {"sid": "x"}))["data"] == {"sid": "x"}
    with pytest.raises(ValueError):
        decode_packet(b"\x01")


def _schedule_record(offset_ms: float, product_id: int, measured: bool) -> dict:
    return {"offset_ms": offset_ms, "measured": measured, "payload": {"eventType": "listed", "sales": [_sale(AK, product_id)]}}


@pytest.mark.asyncio
async def test_feed_server_handshake_play_and_flush():
    frames = prepare_frames([_schedule_record(0, 1, False), _schedule_record(0, 2, True), _schedule_record(5, 3, True)])
    server = FeedServer(frames)
    async with TestClient(TestServer(server.app())) as client:
        socket = await client.ws_connect("/socket.io/")
        assert (await socket.receive_str()) == OPEN_PACKET
        await socket.send_bytes(encode_packet(CONNECT, None))
        assert decode_packet(await socket.receive_bytes())["type"] == CONNECT
        await socket.send_bytes(encode_event("saleFeedJoin", {"currency": "USD"}))
        await asyncio.wait_for(server.joined.wait(), 1)

        await server.play(measured=False)
        await server.play(measured=True)
        received = [decode_packet(await socket.receive_bytes())["data"][1]["sales"][0]["productId"] for _ in range(3)]
        assert received == [1, 2, 3]
        assert [key for key, _ in server.emitted] == ["1", "2", "3"]

        flush = asyncio.create_task(server.flush())
        assert decode_packet(await socket.receive_bytes())["data"] == ["benchFlush"]
        await socket.send_bytes(encode_event("benchFlushed", {"events": 3}))
        await asyncio.wait_for(flush, 1)
        assert server.flush_reply == {"events": 3}
        await socket.close()
        assert socket.closed


@pytest.mark.asyncio
async def test_feed_server_needs_a_client():
    server = FeedServer([])
    with pytest.raises(RuntimeError):
        await server.play(measured=True)
    with pytest.raises(RuntimeError):
        await server.flush()


def test_ws_message_type_is_binary():
    # The sidecar's parser only accepts binary frames; encode_event must produce bytes.
    assert isinstance(encode_event("saleFeed", {}), bytes)
    assert WSMsgType.BINARY != WSMsgType.TEXT


# --- Probes ---


def _listing(listing_id: str, price_usd: float = 10.0) -> MarketTick:
    return MarketTick(
        venue="skinport",
        kind=TickKind.LISTED,
        market_hash_name=AK,
        price_usd=price_usd,
        timestamp=1_790_000_000,
        event_type="listed",
        listing_id=listing_id,
    )


def test_recorder_counts_each_listing_once_and_notes_repeats():
    recorder = probes.StageRecorder(expected=2)
    first = recorder.times("1")
    assert first is not None
    recorder.decide(first, 10)
    recorder.decide(first, 20)
    assert first.decided == 10
    assert first.decisions == 2
    assert not recorder.all_decided.is_set()
    second = recorder.times("2")
    assert second is not None
    recorder.decide(second, 30)
    assert recorder.all_decided.is_set()
    assert recorder.times(None) is None


@pytest.mark.asyncio
async def test_instrumented_queue_notes_put_and_get():
    recorder = probes.StageRecorder()
    queue = probes.InstrumentedQueue(recorder)
    await queue.put(_listing("7"))
    await queue.put("not a tick")
    await queue.get()
    await queue.get()
    times = recorder.listings["7"]
    assert times.enqueued is not None
    assert times.dequeued is not None
    assert times.dequeued >= times.enqueued


@pytest.mark.asyncio
async def test_bench_executor_notes_the_order_once():
    recorder = probes.StageRecorder()
    executor = probes.BenchExecutor(recorder)
    trade = {"market_hash_name": AK, "purchase_price_cents": 500, "estimated_profit_cents": 50, "z_score": -3.0}
    await executor.execute(**trade, listing_id="9", venue="skinport")
    await executor.execute(**trade, listing_id="9", venue="skinport")
    assert executor.orders == 1
    assert recorder.listings["9"].executed is not None


@pytest.mark.asyncio
async def test_acknowledge_batch_acknowledges_the_record():
    class Store:
        acknowledged: list[str] = []

        async def acknowledge(self, record_id: str) -> None:
            self.acknowledged.append(record_id)

    class Batch:
        record_id = "1-0"

    store = Store()
    await probes.acknowledge_batch(store, Batch(), url="ignored")
    assert store.acknowledged == ["1-0"]


@pytest.mark.parametrize("level", ["e2e", "full"])
def test_install_restores_the_listener_functions(level):
    import detection
    from scrapers import skinport

    main = listener_main()
    originals = (main.update_window_and_detect, detection.dre_approval_reason, skinport.parse_sale_feed_message)
    with probes.install(probes.StageRecorder(level=level)):
        assert main.update_window_and_detect is not originals[0]
        assert (skinport.parse_sale_feed_message is not originals[2]) == (level == "full")
    assert (main.update_window_and_detect, detection.dre_approval_reason, skinport.parse_sale_feed_message) == originals


@pytest.mark.asyncio
async def test_probes_time_every_stage_of_a_scored_listing():
    """One listing through the real parse, window and DRE functions, with the edge Redis in memory."""
    import detection
    from backtest.store import InMemoryEdgeStore
    from scrapers import skinport

    cache = InMemoryEdgeStore()
    cache.load_baselines({AK: {"latest_price_cents": 1000, "support_floor_cents": 900, "rolling_30d_avg_cents": 1000}}, {})
    for second, price in enumerate((1000, 1010, 990, 1005, 995)):
        await cache.zadd(
            detection.price_window_key("skinport", AK), {f"{1_789_999_000 + second}:{price}": 1_789_999_000 + second}
        )

    class Pool:
        async def submit(self, job) -> None:
            await job()

    recorder = probes.StageRecorder(level="full", expected=1)
    executor = probes.BenchExecutor(recorder)
    message = json.dumps({"receivedAt": 1_790_000_000_000, "payload": {"eventType": "listed", "sales": [_sale(AK, 42, 500)]}})
    with probes.install(recorder):
        _, ticks = skinport.parse_sale_feed_message(message)
        await listener_main().update_window_and_detect(ticks[0], cache, Pool(), executor)

    times = recorder.listings["42"]
    assert times.triggered
    assert times.dre_reason == "support_floor"
    assert executor.orders == 1
    assert recorder.all_decided.is_set()
    stages = listing_stages(times, emitted=times.parse_start, sidecar=None)
    for stage in (
        "parse",
        "window_write",
        "window_read_and_score",
        "z_score",
        "dre",
        "dre_to_order",
        "python_ingress_to_decision",
    ):
        assert stages[stage] is not None and stages[stage] >= 0, stage


# --- Stats ---


def test_percentile_and_distribution():
    assert percentile([1.0, 2.0, 3.0, 4.0], 50) == 2.0
    assert percentile([1.0, 2.0, 3.0, 4.0], 99) == 4.0
    with pytest.raises(ValueError):
        percentile([], 50)
    summary = distribution_ms([1_000_000, 3_000_000])
    assert summary == {"count": 2, "mean": 2.0, "p50": 1.0, "p95": 3.0, "p99": 3.0, "max": 3.0}
    assert distribution_ms([]) == {"count": 0}


def _times(**values) -> probes.ListingTimes:
    times = probes.ListingTimes()
    for name, value in values.items():
        setattr(times, name, value)
    return times


def test_listing_stages_tile_the_path():
    times = _times(
        parse_start=40,
        parse_end=45,
        enqueued=46,
        dequeued=50,
        window_start=52,
        window_end=60,
        score_start=60,
        score_end=70,
        zscore_ns=1,
        detect_end=71,
        triggered=True,
        dre_start=75,
        dre_end=80,
        executed=82,
        decided=80,
    )
    sidecar = SidecarTiming(callback=20, publish=25, published=30)
    stages = listing_stages(times, emitted=10, sidecar=sidecar)
    assert stages["end_to_end"] == 70
    tiled = (
        "socket_to_sidecar_callback",
        "sidecar_handler",
        "sidecar_publish_to_python",
        "parse",
        "parse_to_enqueue",
        "queue_wait",
        "consumer_routing",
        "window_write",
        "window_read_and_score",
        "trigger_and_handoff",
        "anomaly_pool_wait",
        "dre",
    )
    assert sum(stages[stage] or 0 for stage in tiled) == stages["end_to_end"]
    assert stages["dre_to_order"] == 2
    assert stages["sidecar_publish_ack"] == 5
    assert clock_order_violations(times, 10, sidecar) == 0
    assert clock_order_violations(times, 30, sidecar) == 1


def test_listing_stages_without_sidecar_or_dre():
    stages = listing_stages(_times(parse_start=15, decided=30), emitted=10, sidecar=None)
    assert stages["socket_to_python"] == 5
    assert "dre" not in stages
    assert stages["queue_wait"] is None
    assert stage_breakdown([stages])["end_to_end"]["count"] == 1


# --- Run ---


class FakeRedis:
    def __init__(self, keys: int = 0) -> None:
        self.keys = keys
        self.hashes: dict[str, dict] = {}

    async def dbsize(self) -> int:
        return self.keys

    async def scan_iter(self, count: int = 10):
        for index in range(self.keys):
            yield f"key{index}"

    async def hset(self, name: str, mapping: dict) -> None:
        self.hashes[name] = mapping


@pytest.mark.asyncio
async def test_prepare_redis_refuses_a_redis_with_keys():
    with pytest.raises(SystemExit, match=r"holds 3 keys \(key0, key1, key2\)"):
        await prepare_redis(FakeRedis(keys=3), _fixture().baselines)


@pytest.mark.asyncio
async def test_prepare_redis_loads_baselines_and_sticker_prices():
    cache = FakeRedis()
    baselines = BaselineSnapshot(baselines={AK: _baseline(1000, 900)}, sticker_prices={"Sticker | A": 25})
    await prepare_redis(cache, baselines)
    assert json.loads(cache.hashes[edge_baselines_key("skinport")][AK])["support_floor_cents"] == 900
    assert cache.hashes[edge_sticker_prices_key("skinport")] == {"Sticker | A": "25"}


@pytest.mark.asyncio
async def test_wait_for_decisions_times_out():
    recorder = probes.StageRecorder()
    await wait_for_decisions(recorder, 0, 1.0)
    with pytest.raises(TimeoutError):
        await wait_for_decisions(recorder, 1, 0.01)


@pytest.mark.asyncio
async def test_clock_offsets_count_readings_outside_the_bracket():
    class Server:
        def __init__(self) -> None:
            self.readings = iter([None, 0])

        async def send(self, command: str) -> None:
            pass

        async def expect(self, prefix: str, timeout: float) -> str:
            import time

            reading = next(self.readings, None)
            return f"CLOCK {time.perf_counter_ns() if reading is None else reading}"

    clock = await clock_offsets(Server())
    assert clock["probes"] == 50
    assert clock["outside_bracket"] == 1


def test_sidecar_timings_file(tmp_path: Path):
    path = tmp_path / "timings.json"
    assert _sidecar_timings(path) == ({}, {})
    path.write_text(json.dumps({"timings": [["5", "1", "2", "3"]], "max_rss_kb": 10}), encoding="utf-8")
    timings, usage = _sidecar_timings(path)
    assert timings == {"5": SidecarTiming(1, 2, 3)}
    assert usage == {"max_rss_kb": 10}


def test_summarize_counts_drops_repeats_and_approvals(tmp_path: Path):
    builder = ScenarioBuilder(_fixture(), seed=3)
    events = builder.schedule(_scenario(sales_per_event=2, sold_events=False, measured_listings=4))
    write_schedule(tmp_path / "schedule.jsonl", events)
    assert len((tmp_path / "schedule.jsonl").read_text(encoding="utf-8").splitlines()) == len(events)

    recorder = probes.StageRecorder()
    measured = [event for event in events if event.measured]
    emitted = {event.key: 1_000 for event in measured}
    product_ids = [product_id for event in measured for product_id in event.product_ids]
    for index, product_id in enumerate(product_ids[:-1]):
        times = recorder.times(product_id)
        assert times is not None
        times.parse_start = 2_000
        recorder.decide(times, 3_000 + index)
        if index == 0:
            recorder.decide(times, 9_000)
            times.executed = 3_500
    config = RunConfig(
        scenario=SCENARIOS["single"], fixture_dir=tmp_path, redis_url="redis://x", out_dir=tmp_path, seed=3, run_index=0
    )

    summary = summarize(config=config, events=events, recorder=recorder, emitted=emitted, sidecar_timings={}, run_dir=tmp_path)
    assert summary["measured_listings"] == 4
    assert summary["decided"] == 3
    assert summary["dropped"] == 1
    assert summary["duplicate_decisions"] == 1
    assert summary["approved"] == 1
    assert summary["clock_order_violations"] == 0
    assert summary["stages_ms"]["end_to_end"]["count"] == 3
    with gzip.open(tmp_path / "stages.jsonl.gz", "rt", encoding="utf-8") as raw:
        assert len(raw.readlines()) == 3


# --- CLI ---


def test_parser_and_aggregate(tmp_path: Path):
    args = build_parser().parse_args(["run", "--redis-url", "redis://x", "--runs", "2", "--scenarios", "single"])
    assert args.runs == 2
    assert args.probes == "e2e,full"

    run_dir = tmp_path / "single-full-run0"
    run_dir.mkdir()
    summary = {
        "scenario": "single",
        "probe_level": "full",
        "run_index": 0,
        "decided": 1,
        "dropped": 0,
        "duplicate_decisions": 0,
        "approved": 0,
        "clock_order_violations": 0,
        "throughput_listings_per_second": 10.0,
        "stages_ms": {"end_to_end": {"count": 1, "p50": 2.0}},
        "loop_lag": {"count": 0},
        "cpu_seconds_measured": {"listener": 1.0},
        "redis_cpu_seconds_measured": 0.1,
        "measured_seconds": 1.0,
        "resources": {"listener": {"peak_rss_bytes": 100}},
        "clock": {"outside_bracket": 0},
        "environment": {"cpu_count": 1},
        "decision_config": {},
    }
    (run_dir / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
    with gzip.open(run_dir / "stages.jsonl.gz", "wt", encoding="utf-8") as raw:
        raw.write(
            json.dumps({"listing": "1", "triggered": False, "dre_reason": None, "end_to_end": 2_000_000, "dre": None}) + "\n"
        )
    results = aggregate(tmp_path)
    group = results["groups"]["single/full"]
    assert group["pooled"]["end_to_end"]["p50"] == 2.0
    assert group["runs"][0]["peak_rss_bytes"] == {"listener": 100}

    report = markdown_report(results)
    assert "| single | 1 | 0 | 2.00 |" in report
    assert "| end_to_end | 1 | 2.000 | 100.0% |" in report


def test_decision_rate():
    assert decision_rate(3, 0, 2_000_000_000) == 1.0
    assert decision_rate(1, 0, 10) is None
    assert decision_rate(3, None, 10) is None
    assert decision_rate(3, 10, 10) is None
