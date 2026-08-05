#!/usr/bin/env python
"""Enforce per-module coverage targets defined in pyproject.toml.

Usage: run after `uv run coverage run -m pytest` so that .coverage exists.

Targets are configured under [tool.brand-sniper.coverage-targets] in pyproject.toml
(see issue #139). Exits non-zero when any tracked module falls below its target.
"""

import json
import sys
import tempfile
import tomllib
from pathlib import Path

import coverage

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
DATA_FILE = ROOT / ".coverage"


def load_targets() -> dict[str, int]:
    with open(PYPROJECT, "rb") as f:
        pyproject = tomllib.load(f)
    try:
        return dict(pyproject["tool"]["brand-sniper"]["coverage-targets"])
    except KeyError:
        print("ERROR: [tool.brand-sniper.coverage-targets] missing from pyproject.toml")
        sys.exit(2)


def measure(cov: coverage.Coverage, paths: list[Path]) -> dict[Path, float]:
    """Return branch-aware coverage percentages using Coverage.py's public API."""
    with tempfile.TemporaryDirectory() as temp_dir:
        report_path = Path(temp_dir) / "coverage.json"
        cov.json_report(morfs=[str(path) for path in paths], outfile=str(report_path))
        files = json.loads(report_path.read_text(encoding="utf-8"))["files"]

    return {path: files[str(path.relative_to(ROOT))]["summary"]["percent_covered"] for path in paths}


def main() -> int:
    if not DATA_FILE.exists():
        print(f"ERROR: {DATA_FILE} not found. Run `uv run coverage run -m pytest` first.")
        return 2

    targets = load_targets()
    cov = coverage.Coverage(data_file=str(DATA_FILE))
    cov.load()
    paths = [ROOT / rel_path for rel_path in targets]
    percentages = measure(cov, paths)

    failures = 0
    for rel_path, target in sorted(targets.items()):
        path = ROOT / rel_path
        pct = percentages[path]
        status = "PASS" if pct >= target else "FAIL"
        if status == "FAIL":
            failures += 1
        print(f"{status} {rel_path}: {pct:.1f}% (target >= {target}%)")

    if failures:
        print(f"ERROR: {failures} module(s) below coverage target.")
        return 1
    print(f"All {len(targets)} per-module coverage targets met.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
