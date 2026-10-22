"""
Draw the README's speed chart: how long the listener takes to decide on a listing (#175), next to times people
know and to how fast Skinport listings actually sell. Writes a light and a dark SVG for the README's <picture>.

    uv run python scripts/ingress_speed_chart.py

The listener's numbers come from the committed benchmark results for the Python ingress; the rest are constants
with their sources. A single listing comes from the reference runs. The 100 listing event comes from the back to back
rounds in comparison.json, because other load on the PC disturbed two of the three reference runs of that scenario
(docs/benchmarks/ingress_latency.md, part 2).
"""

import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = ROOT / "docs" / "benchmarks" / "ingress_latency"
REFERENCE = RESULTS_DIR / "python_reference.json"
COMPARISON = RESULTS_DIR / "comparison.json"
OUT_DIR = ROOT / "docs" / "images"

# Human Benchmark's reaction time test, "The average (median) reaction time is 273 milliseconds" (read 2026-10-22).
HUMAN_REACTION_MS = 273.0
# BioNumbers BNID 100706: a blink lasts 0.1 to 0.4 s (Schiffman, Sensation and Perception, 2001).
SHORTEST_BLINK_MS = 100.0
# feed_events, Skinport listings seen listed that later sold, 2026-09-24 to 2026-10-22 (12,324 listings).
FASTEST_SALE_MS = 9_098.0
MEDIAN_SALE_MS = 9_430_153.0

# The axis runs over whole decades of milliseconds, from 0.1 ms to 10,000 s.
AXIS_MIN_EXPONENT = -1
AXIS_MAX_EXPONENT = 7
TICK_LABELS = ["0.1 ms", "1 ms", "10 ms", "100 ms", "1 s", "10 s", "100 s", "1,000 s", "10,000 s"]

WIDTH = 900
LABEL_X = 16
VALUE_X = 330  # Right edge of the value column
PLOT_LEFT = 350
PLOT_RIGHT = 880
TOP = 78
ROW_HEIGHT = 34
DOT_RADIUS = 6
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
TITLE = "From the Skinport socket to the decision, against times people know"


@dataclass(frozen=True)
class Row:
    label: str
    milliseconds: float
    ours: bool


@dataclass(frozen=True)
class Theme:
    name: str
    text_primary: str
    text_secondary: str
    grid: str
    ours: str
    reference: str


LIGHT = Theme("light", "#0b0b0b", "#52514e", "#e4e3df", "#2a78d6", "#8a8984")
DARK = Theme("dark", "#f0efec", "#c3c2b7", "#30363d", "#3987e5", "#8f8e88")


def rows() -> list[Row]:
    groups = json.loads(REFERENCE.read_text(encoding="utf-8"))["groups"]
    single = groups["single/e2e"]["pooled"]["end_to_end"]
    comparison_runs = json.loads(COMPARISON.read_text(encoding="utf-8"))["runs"]
    large_p99 = statistics.median(
        run["end_to_end_ms"]["p99"] for run in comparison_runs if run["ingress"] == "python" and run["scenario"] == "large"
    )
    return [
        Row("Brand Sniper decision, median listing", single["p50"], ours=True),
        Row("Brand Sniper decision, 99th percentile", single["p99"], ours=True),
        Row("Brand Sniper, 100 listings at once (p99)", large_p99, ours=True),
        Row("Shortest blink of an eye", SHORTEST_BLINK_MS, ours=False),
        Row("Median human reaction time", HUMAN_REACTION_MS, ours=False),
        Row("Fastest Skinport listing to sell", FASTEST_SALE_MS, ours=False),
        Row("Median Skinport listing to sell", MEDIAN_SALE_MS, ours=False),
    ]


def duration(milliseconds: float) -> str:
    if milliseconds < 1_000:
        return f"{milliseconds:.1f} ms" if milliseconds < 10 else f"{milliseconds:.0f} ms"
    seconds = milliseconds / 1_000
    if seconds < 60:
        return f"{seconds:.1f} s"
    return f"{seconds / 3_600:.1f} h"


def x_of(milliseconds: float) -> float:
    share = (math.log10(milliseconds) - AXIS_MIN_EXPONENT) / (AXIS_MAX_EXPONENT - AXIS_MIN_EXPONENT)
    return PLOT_LEFT + share * (PLOT_RIGHT - PLOT_LEFT)


def text(x: float, y: float, content: str, fill: str, size: int = 13, anchor: str = "start", weight: str = "400") -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" fill="{fill}" font-size="{size}" font-weight="{weight}" '
        f'text-anchor="{anchor}">{escape(content)}</text>'
    )


def svg(theme: Theme, chart_rows: list[Row]) -> str:
    plot_bottom = TOP + ROW_HEIGHT * len(chart_rows)
    height = plot_bottom + 44
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{height}" viewBox="0 0 {WIDTH} {height}" '
        f'font-family="{FONT}" role="img" aria-labelledby="title description">',
        '<title id="title">How fast Brand Sniper decides on a listing</title>',
        '<desc id="description">'
        + escape(
            "; ".join(f"{row.label}: {duration(row.milliseconds)}" for row in chart_rows)
            + ". Log scale from 0.1 ms to 10,000 s."
        )
        + "</desc>",
        text(LABEL_X, 26, TITLE, theme.text_primary, 16, weight="600"),
        text(LABEL_X, 48, "Log scale: each gridline is ten times the one before", theme.text_secondary, 13),
    ]
    # Legend: identity never rests on color alone, each row also says what it is.
    legend_y = 48
    parts.append(f'<circle cx="{PLOT_RIGHT - 250}" cy="{legend_y - 4}" r="{DOT_RADIUS}" fill="{theme.ours}"/>')
    parts.append(text(PLOT_RIGHT - 238, legend_y, "Brand Sniper, measured", theme.text_secondary, 13))
    parts.append(f'<circle cx="{PLOT_RIGHT - 90}" cy="{legend_y - 4}" r="{DOT_RADIUS}" fill="{theme.reference}"/>')
    parts.append(text(PLOT_RIGHT - 78, legend_y, "For scale", theme.text_secondary, 13))

    for exponent, label in zip(range(AXIS_MIN_EXPONENT, AXIS_MAX_EXPONENT + 1), TICK_LABELS, strict=True):
        x = x_of(10.0**exponent)
        parts.append(f'<line x1="{x:.1f}" y1="{TOP - 6}" x2="{x:.1f}" y2="{plot_bottom}" stroke="{theme.grid}"/>')
        parts.append(text(x, plot_bottom + 20, label, theme.text_secondary, 12, anchor="middle"))

    for index, row in enumerate(chart_rows):
        y = TOP + ROW_HEIGHT * index + ROW_HEIGHT / 2
        weight = "600" if row.ours else "400"
        parts.append(text(LABEL_X, y + 4, row.label, theme.text_primary, 13, weight=weight))
        parts.append(text(VALUE_X, y + 4, duration(row.milliseconds), theme.text_secondary, 13, anchor="end"))
        fill = theme.ours if row.ours else theme.reference
        parts.append(f'<circle cx="{x_of(row.milliseconds):.1f}" cy="{y:.1f}" r="{DOT_RADIUS}" fill="{fill}"/>')
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def main() -> None:
    chart_rows = rows()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for theme in (LIGHT, DARK):
        path = OUT_DIR / f"ingress_speed_{theme.name}.svg"
        path.write_text(svg(theme, chart_rows), encoding="utf-8", newline="\n")
        print(f"Wrote {path.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
