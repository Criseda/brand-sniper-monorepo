"""
Latency benchmark CLI (#175). From apps/listener, with an empty Redis of its own (never the edge Redis):

    uv run python -m latency_bench run --redis-url redis://127.0.0.1:6390 --runs 3
    uv run python -m latency_bench report ../../data/latency_bench/reference/results.json
    uv run python -m latency_bench fixture --start 2026-10-12 --end 2026-10-14 --out-dir latency_bench/fixtures/skinport_v1

The reference runs use deployments/latency-bench (Docker Compose). See docs/benchmarks/ingress_latency.md.
"""

from pathlib import Path

from shared_utils import setup_service_environment

# Detection thresholds are read from the environment at import, so load the same .env files as the listener
# (root, then apps/listener) before importing anything that reads them, as the replay CLI does. A run then
# points every Redis setting at the benchmark's own Redis (latency_bench.run).
setup_service_environment(Path(__file__).resolve().parent)

import argparse
import asyncio
import gzip
import json
import subprocess
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

LISTENER_DIR = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "skinport_v1"
DEFAULT_SEED = 175
PROBE_LEVELS = ("e2e", "full")


def _scenario_names() -> list[str]:
    from latency_bench.scenarios import SCENARIOS

    return list(SCENARIOS)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m latency_bench", description="Ingress to decision latency benchmark.")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="Every scenario, several runs each, in fresh processes; writes results.json.")
    once = commands.add_parser("once", help="One run of one scenario in this process.")
    for command in (run, once):
        command.add_argument("--redis-url", required=True, help="An empty Redis for the benchmark alone")
        command.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
        command.add_argument("--seed", type=int, default=DEFAULT_SEED)
        command.add_argument("--listings", type=int, help="Measured listings per scenario (default: the scenario's own)")
        command.add_argument("--profile", action="store_true", help="Also profile Python (cProfile) and Node (--cpu-prof)")
    run.add_argument("--scenarios", default=",".join(_scenario_names()), help="Comma separated scenario names")
    run.add_argument("--runs", type=int, default=3)
    run.add_argument("--probes", default=",".join(PROBE_LEVELS), help="Comma separated probe levels: e2e, full")
    run.add_argument("--out-dir", type=Path, help="Default: data/latency_bench/<UTC time> at the repository root")
    once.add_argument("--scenario", choices=_scenario_names(), required=True)
    once.add_argument("--probes", choices=PROBE_LEVELS, default="full")
    once.add_argument("--run-index", type=int, default=0)
    once.add_argument("--out-dir", type=Path, required=True)

    serve = commands.add_parser("serve", help="The stand-in feed server (started by `once`).")
    serve.add_argument("--schedule", type=Path, required=True)
    serve.add_argument("--emit-out", type=Path, required=True)
    serve.add_argument("--port", type=int, default=0)

    report = commands.add_parser("report", help="Markdown tables from a results.json, for the report.")
    report.add_argument("results", type=Path)

    fixture = commands.add_parser("fixture", help="Build the benchmark fixture from recorded feed events (needs DATABASE_URL).")
    fixture.add_argument("--start", type=datetime.fromisoformat, required=True, help="Range start (ISO, UTC)")
    fixture.add_argument("--end", type=datetime.fromisoformat, required=True, help="Range end, exclusive (ISO, UTC)")
    fixture.add_argument("--out-dir", type=Path, required=True)
    fixture.add_argument("--max-items", type=int, default=300, help="Items with a listed sale template")
    fixture.add_argument("--max-sold", type=int, default=100, help="Sold sale templates")
    fixture.add_argument("--venue", default="skinport")
    return parser


async def once_command(args: argparse.Namespace) -> int:
    from latency_bench.run import RunConfig, run_once
    from latency_bench.scenarios import SCENARIOS

    summary = await run_once(
        RunConfig(
            scenario=SCENARIOS[args.scenario],
            fixture_dir=args.fixture,
            redis_url=args.redis_url,
            out_dir=args.out_dir,
            seed=args.seed,
            run_index=args.run_index,
            probe_level=args.probes,
            measured_listings=args.listings,
            profile=args.profile,
        )
    )
    end_to_end = summary["stages_ms"].get("end_to_end", {})
    print(
        f"[LATENCY] {args.scenario} ({args.probes}, run {args.run_index}): {summary['decided']} decided, "
        f"{summary['dropped']} dropped, end to end p50 {end_to_end.get('p50', float('nan')):.2f} ms, "
        f"p99 {end_to_end.get('p99', float('nan')):.2f} ms",
        file=sys.stderr,
        flush=True,
    )
    return 1 if summary["timed_out"] or summary["dropped"] else 0


def run_command(args: argparse.Namespace) -> int:
    """Each run in a fresh process, its listener output in <scenario>-<probes>-run<n>.log through a pipe."""
    out_dir = args.out_dir or (
        LISTENER_DIR.parents[1] / "data" / "latency_bench" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    scenarios = [name for name in args.scenarios.split(",") if name]
    levels = [level for level in args.probes.split(",") if level]
    failures = 0
    for scenario in scenarios:
        for level in levels:
            for run_index in range(args.runs):
                log_path = out_dir / f"{scenario}-{level}-run{run_index}.log"
                command = [
                    sys.executable,
                    "-m",
                    "latency_bench",
                    "once",
                    "--redis-url",
                    args.redis_url,
                    "--fixture",
                    str(args.fixture),
                    "--seed",
                    str(args.seed),
                    "--scenario",
                    scenario,
                    "--probes",
                    level,
                    "--run-index",
                    str(run_index),
                    "--out-dir",
                    str(out_dir),
                ]
                if args.listings:
                    command += ["--listings", str(args.listings)]
                if args.profile:
                    command.append("--profile")
                # The run writes its output to a pipe, as the listener's container log does, and this process
                # copies it to the file. Writing the file directly blocked the listener's event loop on every
                # log line, for hundreds of milliseconds at a time when the results folder is a Docker Desktop
                # bind mount.
                with (
                    log_path.open("w", encoding="utf-8") as log,
                    subprocess.Popen(
                        command, cwd=LISTENER_DIR, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8"
                    ) as process,
                ):
                    assert process.stdout is not None
                    for line in process.stdout:
                        log.write(line)
                returncode = process.returncode
                summary_line = [
                    line
                    for line in log_path.read_text(encoding="utf-8").splitlines()
                    if "[LATENCY]" in line or line.startswith("Refusing")
                ]
                print(
                    summary_line[-1] if summary_line else f"{scenario} {level} run {run_index}: exit {returncode}",
                    flush=True,
                )
                failures += int(returncode != 0)
    results = aggregate(out_dir)
    (out_dir / "results.json").write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"Results: {out_dir / 'results.json'}", flush=True)
    return 1 if failures else 0


def aggregate(out_dir: Path) -> dict[str, Any]:
    """Every listing of every run of a scenario and probe level pooled, plus each run's own headline numbers."""
    from latency_bench.stats import distribution_ms

    groups: dict[str, dict[str, Any]] = {}
    for summary_path in sorted(out_dir.glob("*/summary.json")):
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        group = groups.setdefault(
            f"{summary['scenario']}/{summary['probe_level']}",
            {"scenario": summary["scenario"], "probe_level": summary["probe_level"], "runs": [], "pooled": {}},
        )
        end_to_end = summary["stages_ms"].get("end_to_end", {})
        group["runs"].append(
            {
                "run_index": summary["run_index"],
                "decided": summary["decided"],
                "dropped": summary["dropped"],
                "duplicate_decisions": summary["duplicate_decisions"],
                "approved": summary["approved"],
                "clock_order_violations": summary["clock_order_violations"],
                "throughput_listings_per_second": summary["throughput_listings_per_second"],
                "decision_rate_listings_per_second": summary.get("decision_rate_listings_per_second"),
                "end_to_end_ms": end_to_end,
                "loop_lag_ms": summary["loop_lag"],
                "cpu_seconds_measured": summary["cpu_seconds_measured"],
                "redis_cpu_seconds_measured": summary["redis_cpu_seconds_measured"],
                "system_cpu_percent_measured": summary.get("system_cpu_percent_measured", {}),
                "measured_seconds": summary["measured_seconds"],
                "peak_rss_bytes": {name: usage["peak_rss_bytes"] for name, usage in summary["resources"].items()},
                "clock": summary["clock"],
            }
        )
        group["environment"] = summary["environment"]
        group["decision_config"] = summary["decision_config"]
        with gzip.open(summary_path.parent / "stages.jsonl.gz", "rt", encoding="utf-8") as raw:
            for line in raw:
                row = json.loads(line)
                for stage, value in row.items():
                    if stage not in ("listing", "triggered", "dre_reason") and value is not None:
                        group["pooled"].setdefault(stage, []).append(value)
    for group in groups.values():
        group["pooled"] = {stage: distribution_ms(values) for stage, values in group["pooled"].items()}
    return {"generated_at": datetime.now(UTC).isoformat(), "groups": groups}


# Stages in path order, as listing_stages names them.
STAGE_ORDER = (
    "socket_to_sidecar_callback",
    "sidecar_handler",
    "sidecar_publish_to_python",
    "socket_to_python",
    "parse",
    "parse_to_enqueue",
    "queue_wait",
    "consumer_routing",
    "window_write",
    "window_read_and_score",
    "z_score",
    "trigger_and_handoff",
    "anomaly_pool_wait",
    "dre",
    "dre_to_order",
    "sidecar_publish_ack",
    "python_ingress_to_decision",
    "end_to_end",
)


def _ms(value: float | None) -> str:
    return "" if value is None else f"{value:.2f}"


def _mean(values: list[float]) -> float | None:
    return sum(values) / len(values) if values else None


def markdown_report(results: dict[str, Any]) -> str:
    """The headline, overhead and stage tables of one results.json."""
    groups = results["groups"]
    scenarios = list(dict.fromkeys(group["scenario"] for group in groups.values()))
    lines = [
        "| Scenario | Listings | Dropped | p50 ms | p95 ms | p99 ms | Max ms | Decided/s | Loop lag p99 ms "
        "| Listener CPU | Sidecar CPU | Redis CPU | Peak RSS listener / sidecar MB |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for scenario in scenarios:
        group = groups.get(f"{scenario}/e2e") or groups[f"{scenario}/full"]
        runs = group["runs"]
        end_to_end = group["pooled"]["end_to_end"]

        def cpu_share(name: str, runs: list[dict[str, Any]] = runs) -> str:
            shares = [run["cpu_seconds_measured"].get(name, 0.0) / run["measured_seconds"] for run in runs]
            return f"{100 * (_mean(shares) or 0.0):.0f}%"

        redis_share = _mean([run["redis_cpu_seconds_measured"] / run["measured_seconds"] for run in runs]) or 0.0
        rss = [max(run["peak_rss_bytes"].get(name, 0) for run in runs) / 2**20 for name in ("listener", "sidecar")]
        throughput = _mean([run.get("decision_rate_listings_per_second") or 0.0 for run in runs])
        lag = max(run["loop_lag_ms"].get("p99", 0.0) for run in runs)
        lines.append(
            f"| {scenario} | {end_to_end['count']} | {sum(run['dropped'] for run in runs)} | {_ms(end_to_end['p50'])} "
            f"| {_ms(end_to_end['p95'])} | {_ms(end_to_end['p99'])} | {_ms(end_to_end['max'])} | {throughput:.0f} | {_ms(lag)} "
            f"| {cpu_share('listener')} | {cpu_share('sidecar')} | {100 * redis_share:.0f}% | {rss[0]:.0f} / {rss[1]:.0f} |"
        )

    lines += ["", "| Scenario | e2e probes p50 / p99 ms | full probes p50 / p99 ms | Runs p50 ms (full) | Runs p99 ms (full) |"]
    lines.append("|---|---|---|---|---|")
    for scenario in scenarios:
        light = groups.get(f"{scenario}/e2e", {}).get("pooled", {}).get("end_to_end", {})
        full_group = groups.get(f"{scenario}/full", {})
        full = full_group.get("pooled", {}).get("end_to_end", {})
        per_run = full_group.get("runs", [])
        lines.append(
            f"| {scenario} | {_ms(light.get('p50'))} / {_ms(light.get('p99'))} "
            f"| {_ms(full.get('p50'))} / {_ms(full.get('p99'))} "
            f"| {', '.join(_ms(run['end_to_end_ms'].get('p50')) for run in per_run)} "
            f"| {', '.join(_ms(run['end_to_end_ms'].get('p99')) for run in per_run)} |"
        )

    for scenario in scenarios:
        group = groups.get(f"{scenario}/full")
        if group is None:
            continue
        pooled = group["pooled"]
        total = pooled["end_to_end"]["mean"]
        lines += ["", f"**{scenario}** (full probes, every run pooled)", ""]
        lines += ["| Stage | Listings | Mean ms | Share of mean | p50 ms | p99 ms | Max ms |", "|---|---|---|---|---|---|---|"]
        for stage in STAGE_ORDER:
            if stage not in pooled or not pooled[stage].get("count"):
                continue
            values = pooled[stage]
            lines.append(
                f"| {stage} | {values['count']} | {values['mean']:.3f} | {100 * values['mean'] / total:.1f}% "
                f"| {values['p50']:.3f} | {values['p99']:.3f} | {values['max']:.3f} |"
            )
    return "\n".join(lines)


async def fixture_command(args: argparse.Namespace) -> int:
    from backtest.database import _default_engine as default_engine
    from backtest.database import load_baseline_schedule, stream_feed_events
    from latency_bench.fixture import fixture_baselines, recorded_event_sizes, select_templates, write_fixture

    schedule = await load_baseline_schedule(args.start, args.end, venue=args.venue)
    if not schedule.builds:
        raise SystemExit(f"No {args.venue} baseline builds before {args.end.isoformat()}")
    build = schedule.builds[-1]
    baselines = await schedule.load(build.build_id)
    events = [event async for event in stream_feed_events(args.start, args.end, args.venue, default_engine())]
    listed, sold = select_templates(events, baselines, args.max_items, args.max_sold)
    source = {
        "venue": args.venue,
        "start": args.start.isoformat(),
        "end": args.end.isoformat(),
        "baseline_build": build.build_id,
    }
    write_fixture(args.out_dir, listed, sold, fixture_baselines(listed, baselines), recorded_event_sizes(events), source)
    print(f"Wrote {len(listed)} listed and {len(sold)} sold templates to {args.out_dir}", file=sys.stderr)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        from latency_bench.feed_server import serve

        asyncio.run(serve(args.schedule, args.emit_out, args.port))
        return 0
    if args.command == "run":
        return run_command(args)
    if args.command == "once":
        return asyncio.run(once_command(args))
    if args.command == "report":
        print(markdown_report(json.loads(args.results.read_text(encoding="utf-8"))))
        return 0
    return asyncio.run(fixture_command(args))


if __name__ == "__main__":
    sys.exit(main())
