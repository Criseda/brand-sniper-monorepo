"""
Replay CLI. From apps/listener:

    uv run python -m backtest run --fixture EVENTS.jsonl --baselines BASELINES.json --out decisions.jsonl
    uv run python -m backtest run --start 2026-09-24T00:00 --end 2026-09-25T00:00 --out decisions.jsonl
    uv run python -m backtest export --start 2026-09-24T22:00 --end 2026-09-24T23:00 --out-dir fixtures/replay

See docs/backtesting.md.
"""

from pathlib import Path

from shared_utils import setup_service_environment

# Detection thresholds are read from the environment at import, so load the same .env files as the
# listener service (root, then apps/listener) before importing anything that reads them. The package
# directory is passed so its parent, apps/listener, is treated as the service directory.
setup_service_environment(Path(__file__).resolve().parent)

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import IO, Any

from backtest.harness import TimingHook, open_log, run_replay
from backtest.sources import (
    BaselineSchedule,
    BaselineSnapshot,
    RecordedEvent,
    RecordedFeedEvent,
    RecordedSnapshot,
    load_fixture,
    sanitize_feed_payload,
    write_fixture,
)
from backtest.strategy import STRATEGIES, Decision
from models import MarketTick
from shared_utils import build_versioned_name, get_logger

logger = get_logger("listener.backtest")


def parse_utc(value: str) -> datetime:
    """ISO timestamp as naive UTC (the database convention); a value without an offset is taken as UTC."""
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _epoch_ms(naive_utc: datetime) -> int:
    return (naive_utc - datetime(1970, 1, 1)) // timedelta(milliseconds=1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m backtest", description="Deterministic replay of recorded market events.")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Replay events through a strategy and write a decision log.")
    source = run.add_mutually_exclusive_group(required=True)
    source.add_argument("--fixture", type=Path, help="Fixture file (JSON Lines) to replay")
    source.add_argument("--start", type=parse_utc, help="Database replay: range start (ISO, UTC)")
    run.add_argument("--end", type=parse_utc, help="Database replay: range end, exclusive (ISO, UTC)")
    run.add_argument(
        "--baselines",
        type=Path,
        help="Baseline file; required with --fixture. A database replay defaults to the dated builds in effect",
    )
    run.add_argument(
        "--warmup-hours",
        type=float,
        default=0.0,
        help="Database replay: also replay this many hours before --start to fill the price windows (not logged)",
    )
    run.add_argument("--log-from", type=parse_utc, help="Fixture replay: only log decisions from this time (ISO, UTC)")
    run.add_argument("--strategy", choices=sorted(STRATEGIES), default="zscore_dre")
    run.add_argument("--out", type=Path, required=True, help="Decision log to write (JSON Lines)")
    run.add_argument("--speed", type=float, help="Replay at recorded pace times this factor (omit to run flat out)")
    run.add_argument("--timings", type=Path, help="Also write per-decision latency (JSON Lines, not deterministic)")
    run.add_argument("--source", default="skinport", help="Venue to replay from the database")

    export = commands.add_parser("export", help="Write a sanitized fixture and baseline file from the database.")
    export.add_argument("--start", type=parse_utc, required=True)
    export.add_argument("--end", type=parse_utc, required=True)
    export.add_argument("--out-dir", type=Path, required=True)
    export.add_argument("--max-items", type=int, default=50, help="Keep the first N items seen in listed events")
    export.add_argument("--source", default="skinport")
    return parser


def _write_timing(timings: IO[str]) -> TimingHook:
    def record(tick: MarketTick, decision: Decision, elapsed_ns: int) -> None:
        timings.write(
            json.dumps(
                {
                    "market_hash_name": tick.market_hash_name,
                    "listing_id": tick.listing_id,
                    "approve": decision.approve,
                    "elapsed_ns": elapsed_ns,
                },
                sort_keys=True,
            )
            + "\n"
        )

    return record


async def run_command(args: argparse.Namespace) -> int:
    strategy = STRATEGIES[args.strategy]()
    events: Any
    baselines: BaselineSnapshot | BaselineSchedule
    if args.fixture is not None:
        if args.baselines is None:
            raise SystemExit("--baselines is required with --fixture")
        events = load_fixture(args.fixture)
        baselines = BaselineSnapshot.load(args.baselines)
        source_label = f"fixture:{args.fixture.name}"
        log_from_ms = _epoch_ms(args.log_from) if args.log_from is not None else None
    else:
        if args.end is None:
            raise SystemExit("--end is required with --start")
        from backtest.database import load_baseline_schedule, stream_recorded_events

        replay_start = args.start - timedelta(hours=args.warmup_hours)
        events = stream_recorded_events(replay_start, args.end, source=args.source)
        source_label = f"database:{args.source}:{args.start.isoformat()}/{args.end.isoformat()}"
        log_from_ms = _epoch_ms(args.start)
        if args.baselines:
            baselines = BaselineSnapshot.load(args.baselines)
            first_baseline_time = baselines.as_of
        else:
            baselines = await load_baseline_schedule(replay_start, args.end, venue=args.source)
            if not baselines.builds:
                raise SystemExit(
                    f"No {args.source} baseline builds before {args.end.isoformat()}; run apps/analytics/build_baselines.py"
                )
            first_baseline_time = baselines.builds[0].built_at
        if first_baseline_time is not None and parse_utc(first_baseline_time) > replay_start:
            logger.warning(
                "[BACKTEST] Baselines are from %s, after the replay start: decisions use later information.",
                first_baseline_time,
            )

    timings_file = args.timings.open("w", encoding="utf-8", newline="\n") if args.timings else None
    try:
        with open_log(args.out) as log:
            summary = await run_replay(
                events,
                strategy,
                baselines,
                log,
                source_label=source_label,
                log_from_ms=log_from_ms,
                speed=args.speed,
                on_timing=_write_timing(timings_file) if timings_file else None,
            )
    finally:
        if timings_file is not None:
            timings_file.close()

    logger.info(
        "[BACKTEST] %s: %d events, %d decisions, %d logged, %d approved -> %s",
        strategy.name,
        summary.events,
        summary.decisions,
        summary.logged,
        summary.approved,
        args.out,
    )
    return 0


def _versioned_sale_name(sale: dict[str, Any]) -> str | None:
    name = sale.get("marketHashName")
    return build_versioned_name(name, sale.get("version")) if isinstance(name, str) and name else None


def select_items(events: Sequence[RecordedEvent], max_items: int) -> list[str]:
    """The first `max_items` distinct items (versioned names) seen in listed events."""
    chosen: dict[str, None] = {}
    for event in events:
        if not isinstance(event, RecordedFeedEvent) or event.payload.get("eventType") != "listed":
            continue
        for sale in event.payload.get("sales") or []:
            name = _versioned_sale_name(sale) if isinstance(sale, dict) else None
            if name is not None and name not in chosen:
                chosen[name] = None
                if len(chosen) >= max_items:
                    return list(chosen)
    return list(chosen)


def build_fixture(
    events: Sequence[RecordedEvent], baselines: BaselineSnapshot, items: Sequence[str]
) -> tuple[list[RecordedEvent], BaselineSnapshot]:
    """Restrict events and baselines to `items`, sanitizing every kept feed payload."""
    item_set = set(items)
    kept: list[RecordedEvent] = []
    sticker_names: set[str] = set()
    for event in events:
        if isinstance(event, RecordedSnapshot):
            if event.market_hash_name in item_set:
                kept.append(event)
            continue
        payload = sanitize_feed_payload(event.payload, lambda sale: _versioned_sale_name(sale) in item_set)
        if payload is None:
            continue
        kept.append(RecordedFeedEvent(received_at_ms=event.received_at_ms, payload=payload))
        for sale in payload["sales"]:
            for sticker in sale.get("stickers") or []:
                if isinstance(sticker, dict) and sticker.get("name"):
                    sticker_names.add(sticker["name"])

    fixture_baselines = BaselineSnapshot(
        baselines={name: baselines.baselines[name] for name in sorted(item_set) if name in baselines.baselines},
        sticker_prices={
            name: baselines.sticker_prices[name] for name in sorted(sticker_names) if name in baselines.sticker_prices
        },
        as_of=baselines.as_of,
    )
    return kept, fixture_baselines


async def export_command(args: argparse.Namespace) -> int:
    from backtest.database import load_baseline_schedule, stream_recorded_events

    schedule = await load_baseline_schedule(args.start, args.end, venue=args.source)
    if not schedule.builds:
        raise SystemExit(
            f"No {args.source} baseline builds before {args.end.isoformat()}; run apps/analytics/build_baselines.py"
        )
    # A fixture holds one snapshot: the build in effect at the start of the range.
    first_build = schedule.builds[schedule.active_index(_epoch_ms(args.start))]
    events = [event async for event in stream_recorded_events(args.start, args.end, source=args.source)]
    items = select_items(events, args.max_items)
    kept, fixture_baselines = build_fixture(events, await schedule.load(first_build.build_id), items)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    count = write_fixture(args.out_dir / "events.jsonl", kept)
    fixture_baselines.write(args.out_dir / "baselines.json")
    logger.info("[BACKTEST] Exported %d events for %d items to %s", count, len(items), args.out_dir)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    command = run_command if args.command == "run" else export_command
    return asyncio.run(command(args))


if __name__ == "__main__":
    sys.exit(main())
