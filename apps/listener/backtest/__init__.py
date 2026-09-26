"""
Deterministic replay and backtest harness.

Replays recorded feed events and REST snapshots through the listener's real decision path
(`detection`, `zscore`, `rules_engine`) against an in-memory edge store, and writes a decision log
per run. Run it with `uv run python -m backtest --help` from `apps/listener`; see docs/backtesting.md.
"""
