"""
CFO calibration experiment (#250): scores the Adversarial CFO against what the market actually did.

The trades are the approvals of a baseline scorecard decision log (#249) that carry a v1 outcome label. For each
one the CFO is asked, after the fact, how likely the trade is to be profitable. It only ever sees what was known
at the decision time: the decision itself, the listing's own details, and tools bound to a `MarketView` that holds
the baseline build in effect at that moment and the feed sales and listings received before it. Two system
prompts are compared on the same trades: the production adversarial one and a neutral one.

The CFO's answers are written to a verdicts file, one line per trade and prompt, so a run can stop and resume
without paying for a trade twice. The report scores them against the labels: Brier score, reliability by bin,
and the precision and fee-aware P&L of the trades it would keep and veto on top of the DRE, with an item
bootstrap of the paired difference against the DRE alone. CFO output is never used as a training label.

Two backends ask the questions. `llm` (the default) is the OpenAI compatible API in LLM_* with the tool loop above.
`claude-code` asks Claude through headless Claude Code, so a Claude subscription is enough and no API key is
needed. It runs in safe mode with no tools, so what fetch_market_floor and verify_float_value would return is put
in the prompt instead (the name search over other items is left out; giving Claude the same tools over MCP scored
the same on the #250 trades). Its verdicts are stored as `claude-code/<model>` so they are never mixed with an API
run of the same model. Claude Code still attaches the account email and environment details such as today's date;
neither says anything about a trade's outcome.

Usage (from apps/analytics):
    uv run python cfo_calibration.py run --decisions ../../data/scorecard/decisions.jsonl --as-of 2026-10-13T01:40:07 --dry-run
    uv run python cfo_calibration.py run --decisions ... --as-of ... --variants adversarial,neutral
    uv run python cfo_calibration.py run --decisions ... --as-of ... --backend claude-code
    uv run python cfo_calibration.py report --decisions ... --as-of ... --backend claude-code
    uv run python cfo_calibration.py report --decisions ... --as-of ... --input-usd-per-mtok 0.15 --output-usd-per-mtok 0.6

See docs/benchmarks/cfo_calibration.md.
"""

import argparse
import asyncio
import bisect
import json
import random
import shutil
import statistics
import sys
import tempfile
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from shared_utils import setup_script_environment

REPO_ROOT = setup_script_environment(__file__)

import evaluate_performance as cfo
from baseline_scorecard import (
    MIN_SAMPLE,
    OUTCOME_LABELED,
    TICK_KIND_LISTING,
    TICK_KIND_REST_SNAPSHOT,
    DecisionLog,
    Trade,
    apply_listing_label,
    collect_trades,
    dollars,
    fetch_listing_labels,
    git_commit,
    is_scorable,
    label_snapshot_trades,
    matures_by,
    mean_interval,
    parse_utc,
    percent,
    read_decision_log,
    scorable_start,
    wilson_interval,
)
from openai import pydantic_function_tool
from outcome_data import fetch_listing_events
from outcome_labels import LABEL_VENUE as VENUE
from outcome_labels import LABEL_VERSION, FeedSale, LabelConfig, SoldIndex
from pydantic import BaseModel, Field, ValidationError
from shared_utils import get_logger, parse_version_from_name
from shared_utils.db_connection import async_engine
from sqlalchemy import text
from tools import FetchMarketFloorArgs, VerifyFloatArgs, verify_float_value

logger = get_logger("analytics.cfo_calibration")

# Bump when the prompts, the tools or what the CFO is shown change: verdicts of another version are not reused.
PROMPT_VERSION = 1
VARIANT_ADVERSARIAL = "adversarial"
VARIANT_NEUTRAL = "neutral"
VARIANTS = (VARIANT_ADVERSARIAL, VARIANT_NEUTRAL)

RECENT_SALES_DAYS = 7
RECENT_LISTINGS_HOURS = 24
SEARCH_LIMIT = 10
MAX_TOOL_ROUNDS = 4
# Generous, because a free tier allows only a few trades a minute and every wait is short.
MAX_RATE_LIMIT_RETRIES = 20
SAMPLE_SEED = 250

# A trade is kept when the CFO's probability of profit is at least the threshold. 0.5 is the natural cut for a
# probability; 0.7 matches the production CFO, which approves a confidence score of 70 or more.
THRESHOLDS = (0.5, 0.7)
RELIABILITY_BINS = 5
PAIRED_RESAMPLES = 4000
PAIRED_SEED = 250

STATUS_OK = "ok"
STATUS_ERROR = "error"

# --- Prompts ---

# The production prompt (evaluate_performance._SYSTEM_INSTRUCTION) with its tool sentence describing the tools
# this experiment gives it. "Live" and "macro trends" would be false here, so they are the only words changed.
ADVERSARIAL_SYSTEM = """
You are the Adversarial CFO of an algorithmic trading firm.
Your automated trading bot just executed a paper trade.
The bot claims this is a highly profitable "snipe" based on its historical baselines.
Your job is to PROVE THE BOT WRONG.
Use the available tools to fetch the market floor and recent sales as of the trade, and to look up the
baselines of related items.
If the market floor is lower than the bot's baseline, or if there is a falling knife
trend, the bot made a bad trade.
"""

NEUTRAL_SYSTEM = """
You are a trading analyst reviewing a paper trade made by a rules based bot on the Skinport CS2 skin market.
Estimate, as a calibrated probability, whether the trade makes money. Be neither optimistic nor pessimistic:
across many trades, the trades you give a probability of 0.3 should turn out profitable about 30% of the time.
Use the available tools to check the market floor and recent sales as of the trade and the baselines of related
items. All data you can see ends at the moment of the trade.
"""

OUTCOME_DEFINITION = (
    "The trade counts as profitable when the item can be resold on Skinport after the 7 day trade hold at the median "
    "price of the item's sales between day 7 and day 14 after the purchase, and that price minus Skinport's seller fee "
    "(8%, or 6% from $1,000) is at least the purchase price."
)

FINAL_INSTRUCTION = (
    "Answer with JSON only, no other text: "
    '{"probability_of_profit": <number from 0 to 1>, "reasoning": "<two or three sentences>"}'
)


class CalibratedVerdict(BaseModel):
    probability_of_profit: float = Field(ge=0.0, le=1.0)
    reasoning: str


# The same answer as a JSON schema, which Claude Code enforces on its structured output.
VERDICT_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "probability_of_profit": {"type": "number", "minimum": 0, "maximum": 1},
        "reasoning": {"type": "string"},
    },
    "required": ["probability_of_profit", "reasoning"],
    "additionalProperties": False,
}

# Claude Code runs with every tool switched off, so the prompts say the market data comes with the trade instead.
_GIVEN_DATA_SENTENCE = "The market floor and recent sales as of the trade are given with the trade."
_TOOL_SENTENCES = {
    VARIANT_ADVERSARIAL: (
        "Use the available tools to fetch the market floor and recent sales as of the trade, and to look up the\n"
        "baselines of related items."
    ),
    VARIANT_NEUTRAL: (
        "Use the available tools to check the market floor and recent sales as of the trade and the baselines of related\n"
        "items."
    ),
}


def _without_tools(system: str, variant: str) -> str:
    tool_sentence = _TOOL_SENTENCES[variant]
    if system.count(tool_sentence) != 1:
        raise AssertionError(f"The {variant} prompt no longer names its tools as expected")
    return system.replace(tool_sentence, _GIVEN_DATA_SENTENCE)


SEALED_SYSTEMS = {
    VARIANT_ADVERSARIAL: _without_tools(ADVERSARIAL_SYSTEM, VARIANT_ADVERSARIAL),
    VARIANT_NEUTRAL: _without_tools(NEUTRAL_SYSTEM, VARIANT_NEUTRAL),
}


class SearchBaselinesArgs(BaseModel):
    query: str = Field(description="Part of an item name, e.g. 'Redline' or 'Sticker | Crown'. Not a news query.")


TOOL_DEFS: list[tuple[str, str, type[BaseModel]]] = [
    (
        "fetch_market_floor",
        "Market data for one item as of the trade: the Skinport baseline in effect (latest price, 30 and 90 day "
        "medians, support floor, volatility, sales per day), the recorded Skinport sales of the last 7 days and the "
        "listings of the last 24 hours. Nothing after the trade is included.",
        FetchMarketFloorArgs,
    ),
    (
        "search_item_baselines",
        "Search item names in the Skinport baseline build in effect at the trade and return up to 10 matches with "
        "their latest price, 30 day median, sales per day and drift. A name search over our own baselines, not news.",
        SearchBaselinesArgs,
    ),
    (
        "verify_float_value",
        "Evaluate if an item's float wear value carries a price premium. Returns a multiplier (1.0 = standard, "
        ">1.0 = premium) based on CS2 wear tier thresholds.",
        VerifyFloatArgs,
    ),
]

TOOL_SCHEMAS = [
    pydantic_function_tool(args_model, name=name, description=description) for name, description, args_model in TOOL_DEFS
]


# --- Point in time market data ---


@dataclass(frozen=True)
class BaselineSnapshot:
    """One baseline build: when its source data was fetched, and each item's baseline row."""

    build_id: int
    built_at: datetime
    rows: dict[str, dict[str, Any]]


def _by_name(sales: Iterable[FeedSale]) -> dict[str, list[FeedSale]]:
    grouped: dict[str, list[FeedSale]] = {}
    for sale in sorted(sales, key=lambda sale: (sale.seen_at, sale.listing_id)):
        grouped.setdefault(sale.market_hash_name, []).append(sale)
    return grouped


@dataclass
class MarketHistory:
    """Everything the CFO may be shown for any trade. `as_of` cuts it down to what one decision could see."""

    builds: list[BaselineSnapshot]
    sold: dict[str, list[FeedSale]] = field(default_factory=dict)
    listed: dict[str, list[FeedSale]] = field(default_factory=dict)

    @classmethod
    def build(cls, builds: Iterable[BaselineSnapshot], sold: Iterable[FeedSale], listed: Iterable[FeedSale]) -> "MarketHistory":
        return cls(builds=sorted(builds, key=lambda build: build.built_at), sold=_by_name(sold), listed=_by_name(listed))

    def as_of(self, moment: datetime) -> "MarketView":
        build = None
        for candidate in self.builds:
            if candidate.built_at <= moment:
                build = candidate
        return MarketView(moment=moment, build=build, history=self)


def _before(sales: Sequence[FeedSale], start: datetime, end: datetime) -> list[FeedSale]:
    """Sales seen in [start, end); `sales` is sorted by time."""
    times = [sale.seen_at for sale in sales]
    return list(sales[bisect.bisect_left(times, start) : bisect.bisect_left(times, end)])


def _age_hours(moment: datetime, seen_at: datetime) -> float:
    return round((moment - seen_at).total_seconds() / 3600, 1)


@dataclass(frozen=True)
class MarketView:
    """The market as one decision saw it: the newest build fetched at or before `moment`, and earlier events only."""

    moment: datetime
    build: BaselineSnapshot | None
    history: MarketHistory

    def baseline(self, market_hash_name: str) -> dict[str, Any] | None:
        if self.build is None:
            return None
        return self.build.rows.get(market_hash_name)

    def recent_sales(self, market_hash_name: str) -> list[FeedSale]:
        sales = self.history.sold.get(market_hash_name, [])
        return _before(sales, self.moment - timedelta(days=RECENT_SALES_DAYS), self.moment)

    def recent_listings(self, market_hash_name: str) -> list[FeedSale]:
        listings = self.history.listed.get(market_hash_name, [])
        return _before(listings, self.moment - timedelta(hours=RECENT_LISTINGS_HOURS), self.moment)

    def market_floor(self, market_hash_name: str) -> dict[str, Any]:
        baseline = self.baseline(market_hash_name)
        sales = self.recent_sales(market_hash_name)
        listings = self.recent_listings(market_hash_name)
        sale_prices = [sale.price_cents for sale in sales]
        return {
            "market_hash_name": market_hash_name,
            "as_of": self.moment.isoformat(),
            "baseline": baseline,
            "baseline_built_at": self.build.built_at.isoformat() if self.build and baseline else None,
            "sales_last_7_days": {
                "count": len(sales),
                "median_cents": int(statistics.median_low(sale_prices)) if sale_prices else None,
                "min_cents": min(sale_prices) if sale_prices else None,
                "max_cents": max(sale_prices) if sale_prices else None,
                "latest": [
                    {"price_cents": sale.price_cents, "hours_before_trade": _age_hours(self.moment, sale.seen_at)}
                    for sale in sales[-5:]
                ],
            },
            "listings_last_24_hours": {
                "count": len(listings),
                "lowest_prices_cents": sorted(listing.price_cents for listing in listings)[:5],
            },
        }

    def search_baselines(self, query: str) -> list[dict[str, Any]]:
        if self.build is None or not query.strip():
            return []
        needle = query.strip().lower()
        matches = sorted(name for name in self.build.rows if needle in name.lower())[:SEARCH_LIMIT]
        return [
            {
                "market_hash_name": name,
                "latest_price_cents": self.build.rows[name]["latest_price_cents"],
                "rolling_30d_avg_cents": self.build.rows[name]["rolling_30d_avg_cents"],
                "avg_volume_30d": self.build.rows[name]["avg_volume_30d"],
                "drift_percent": self.build.rows[name]["drift_percent"],
            }
            for name in matches
        ]


async def run_tool(view: MarketView, name: str, arguments: dict[str, Any]) -> str:
    """Runs one CFO tool call against the point in time view; a bad call returns an error the model can read."""
    try:
        if name == "fetch_market_floor":
            args = FetchMarketFloorArgs(**arguments)
            return json.dumps(view.market_floor(args.market_hash_name))
        if name == "search_item_baselines":
            search = SearchBaselinesArgs(**arguments)
            return json.dumps({"query": search.query, "results": view.search_baselines(search.query)})
        if name == "verify_float_value":
            float_args = VerifyFloatArgs(**arguments)
            return await verify_float_value(float_args.market_hash_name, float_args.float_value)
    except ValidationError as error:
        return json.dumps({"error": f"Invalid arguments for {name}: {error.errors()[0]['msg']}"})
    return json.dumps({"error": f"Unknown tool {name}"})


# --- Trades to score ---


@dataclass
class TradeCase:
    """One labeled trade and everything known about it at the decision time."""

    key: str
    trade: Trade
    features: dict[str, Any]
    listing: dict[str, Any] | None = None  # Float, pattern and stickers of a feed listing


def case_key(trade: Trade) -> str:
    if trade.tick_kind == TICK_KIND_LISTING:
        return f"listing:{trade.listing_id}"
    return f"rest:{trade.market_hash_name}:{trade.price_cents}:{trade.decided_at.isoformat()}"


def decision_features(log: DecisionLog, trades: Sequence[Trade]) -> dict[str, dict[str, Any]]:
    """The decision row behind each trade (its first approval, as `collect_trades` keeps it), keyed by case key."""
    wanted = {case_key(trade) for trade in trades}
    start = scorable_start(log)
    rows: dict[str, dict[str, Any]] = {}
    for decision in log.decisions:
        if not decision.row["approve"] or not is_scorable(decision, start):
            continue
        trade = Trade.from_decision(decision)
        key = case_key(trade)
        if key in wanted and key not in rows:
            rows[key] = {
                "z_score": decision.row["score"],
                "dre_rule": decision.row["reason"],
                "event_type": decision.row.get("event_type"),
                **(decision.row.get("features") or {}),
            }
    return rows


def sample_cases(cases: Sequence[TradeCase], max_trades: int | None) -> list[TradeCase]:
    """All cases, or a seeded random sample of them in decision order when the run is capped."""
    if max_trades is None or max_trades >= len(cases):
        return list(cases)
    chosen = set(random.Random(SAMPLE_SEED).sample(range(len(cases)), max_trades))
    return [case for index, case in enumerate(cases) if index in chosen]


def _cents(value: Any) -> str:
    return "unknown" if value is None else dollars(float(value))


def _trade_lines(case: TradeCase) -> list[str]:
    """The decision and the listing's own details, as the bot knew them when it decided."""
    trade = case.trade
    features = case.features
    seen_as = (
        "a new listing in the Skinport sale feed"
        if trade.tick_kind == TICK_KIND_LISTING
        else "the lowest ask of a Skinport REST poll"
    )
    lines = [
        f"The bot bought: {trade.market_hash_name}",
        f"Decision time (UTC): {trade.decided_at.isoformat(timespec='seconds')}",
        f"Seen as: {seen_as}",
        f"Purchase price: {dollars(trade.price_cents)}",
        f"Trigger Z score: {features.get('z_score')} (source: {features.get('z_source')}, "
        f"price window of {features.get('window_size')} prices with mean {_cents(features.get('mean_cents'))})",
        f"Approved by the rule: {features.get('dre_rule')}",
        f"Bot's estimated profit after the seller fee: {_cents(features.get('estimated_net_profit_cents'))}",
    ]
    if case.listing is not None:
        float_value = case.listing.get("float_value")
        lines.append(f"Float value: {float_value:.6f}" if float_value is not None else "Float value: none")
        lines.append(f"Pattern: {case.listing.get('pattern')}")
        stickers = [sticker.get("name") for sticker in case.listing.get("stickers") or [] if sticker.get("name")]
        lines.append(f"Stickers: {', '.join(stickers) if stickers else 'none'}")
    return lines


def render_case_prompt(case: TradeCase) -> str:
    """What the bot knew when it decided. No outcome, label or later price is part of it."""
    lines = _trade_lines(case)
    lines.append("")
    lines.append(OUTCOME_DEFINITION)
    lines.append(
        "Use the tools before you answer: fetch_market_floor once, verify_float_value if there is a float value, "
        "and search_item_baselines at most twice. Then estimate the probability that this trade is profitable."
    )
    lines.append(FINAL_INSTRUCTION)
    return "\n".join(lines)


def render_sealed_prompt(case: TradeCase, view: MarketView, float_check: str | None) -> str:
    """
    The same trade for a model without tools: what fetch_market_floor and verify_float_value would return is part
    of the prompt. The name search over other items has no fixed answer to give, so it is left out.
    """
    lines = _trade_lines(case)
    lines.append("")
    lines.append(
        "Market data as of the trade (the Skinport baseline in effect, the sales of the last 7 days and the listings "
        "of the last 24 hours; nothing after the trade is included):"
    )
    lines.append(json.dumps(view.market_floor(case.trade.market_hash_name)))
    if float_check is not None:
        lines.append(f"Float check: {float_check}")
    lines.append("")
    lines.append(OUTCOME_DEFINITION)
    lines.append("Estimate the probability that this trade is profitable.")
    lines.append(FINAL_INSTRUCTION)
    return "\n".join(lines)


# --- Asking the CFO ---


class QuotaExhaustedError(RuntimeError):
    """The provider's daily quota is used up. The run stops instead of switching models mid experiment."""


@dataclass
class Verdict:
    key: str
    variant: str
    model: str
    prompt_version: int
    status: str
    probability: float | None
    reasoning: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    llm_calls: int = 0
    tool_calls: list[str] = field(default_factory=list)


def parse_verdict(raw: str) -> CalibratedVerdict:
    """The JSON answer, also when the model wraps it in a code fence or a sentence."""
    text_value = raw.strip()
    start = text_value.find("{")
    end = text_value.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("no JSON object in the answer")
    return CalibratedVerdict(**json.loads(text_value[start : end + 1]))


_DAILY_QUOTA_MARKERS = ("tokens per day", "requests per day", "(tpd)", "(rpd)")


def is_daily_quota(error: BaseException) -> bool:
    """
    A daily limit, which waiting a minute will not lift. Stricter than `evaluate_performance._is_quota_error`, which
    also matches Groq's per minute limit because its message ends with "Upgrade to Dev Tier today".
    """
    message = str(error).lower()
    return any(marker in message for marker in _DAILY_QUOTA_MARKERS)


def _is_retryable(error: BaseException) -> bool:
    """Rate limits, server errors and connection failures; a rejected request (other 4xx) fails the same way again."""
    status_code = getattr(error, "status_code", None)
    return status_code is None or status_code == 429 or status_code >= 500


async def _create(**kwargs: Any) -> Any:
    """One chat completion, waiting out minute scale rate limits; a daily quota stops the run."""
    for attempt in range(1, MAX_RATE_LIMIT_RETRIES + 1):
        try:
            return await asyncio.to_thread(cfo._get_openai_client().chat.completions.create, **kwargs)
        # Broad on purpose, as in evaluate_performance: provider errors are duck typed.
        except Exception as error:
            if is_daily_quota(error):
                raise QuotaExhaustedError(str(error)) from error
            if attempt == MAX_RATE_LIMIT_RETRIES or not _is_retryable(error):
                raise
            delay = cfo._retry_delay_for(error)
            logger.warning("[CFO] Attempt %d failed, retrying in %.1fs: %s", attempt, delay, error)
            await cfo._sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover


async def ask_cfo(case: TradeCase, view: MarketView, variant: str, model: str) -> Verdict:
    """Runs the CFO's tool loop on one trade and returns its probability of profit."""
    system = ADVERSARIAL_SYSTEM if variant == VARIANT_ADVERSARIAL else NEUTRAL_SYSTEM
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system},
        {"role": "user", "content": render_case_prompt(case)},
    ]
    verdict = Verdict(
        key=case.key,
        variant=variant,
        model=model,
        prompt_version=PROMPT_VERSION,
        status=STATUS_ERROR,
        probability=None,
        reasoning="",
    )

    async def call(tool_choice: str | None) -> Any:
        """One completion; `tool_choice` None sends no tools at all."""
        tool_kwargs: dict[str, Any] = {} if tool_choice is None else {"tools": TOOL_SCHEMAS, "tool_choice": tool_choice}
        response = await _create(model=model, messages=messages, temperature=0, max_tokens=2048, **tool_kwargs)
        verdict.llm_calls += 1
        usage = getattr(response, "usage", None)
        verdict.prompt_tokens += int(getattr(usage, "prompt_tokens", 0) or 0)
        verdict.completion_tokens += int(getattr(usage, "completion_tokens", 0) or 0)
        message = response.choices[0].message
        messages.append(cfo._msg_dict(message))
        return response

    answer = ""
    try:
        for _ in range(MAX_TOOL_ROUNDS):
            try:
                response = await call("auto")
            except Exception as error:
                # gpt-oss sometimes answers by calling a tool named after the format ("JSON"). The provider
                # rejects that call, so ask for the answer again with tools switched off.
                if "tool_use_failed" not in str(error):
                    raise
                break
            message = response.choices[0].message
            if response.choices[0].finish_reason != "tool_calls" or not message.tool_calls:
                answer = message.content or ""
                break
            for tool_call in message.tool_calls:
                verdict.tool_calls.append(tool_call.function.name)
                try:
                    arguments = json.loads(tool_call.function.arguments or "{}")
                except json.JSONDecodeError:
                    arguments = {}
                result = await run_tool(view, tool_call.function.name, arguments)
                messages.append(
                    {"role": "tool", "tool_call_id": tool_call.id, "name": tool_call.function.name, "content": result}
                )
        try:
            parsed = parse_verdict(answer)
        except (ValueError, ValidationError):
            messages.append({"role": "user", "content": FINAL_INSTRUCTION})
            try:
                response = await call("none")
            except Exception as error:
                # Some models still call a tool with tool_choice none; without the tools they cannot.
                if "tool_use_failed" not in str(error):
                    raise
                response = await call(None)
            parsed = parse_verdict(response.choices[0].message.content or "")
    except QuotaExhaustedError:
        raise
    # Broad on purpose: one failed trade is recorded as an error and retried on the next run.
    except Exception as error:
        verdict.reasoning = f"Evaluation failed: {error}"
        return verdict
    verdict.status = STATUS_OK
    verdict.probability = parsed.probability_of_profit
    verdict.reasoning = parsed.reasoning.encode("ascii", "ignore").decode("ascii")
    return verdict


# --- Asking Claude through Claude Code ---

# Verdicts from Claude Code are stored under this prefix, because they come from the sealed prompt (no tool loop)
# and must never be mistaken for verdicts of the same model asked through an API with tools.
CLAUDE_CODE_MODEL_PREFIX = "claude-code/"
DEFAULT_CLAUDE_MODEL = "claude-sonnet-5-5"
CLAUDE_CODE_TIMEOUT_SECONDS = 600
# A Claude plan answers several calls at once; the free tier API of the `llm` backend wants one at a time.
CLAUDE_CODE_CONCURRENCY = 4
BACKEND_LLM = "llm"
BACKEND_CLAUDE_CODE = "claude-code"
# Claude Code ends a run with one of these when the plan's usage limit is reached; waiting minutes will not lift it.
_CLAUDE_CODE_LIMIT_MARKERS = ("usage limit", "limit reached", "hit your", "out of extra usage")


def claude_code_command(executable: str, model: str, system: str) -> list[str]:
    """
    One headless Claude Code call as the CFO: safe mode (no CLAUDE.md, hooks, plugins or MCP servers), no tools,
    no saved session, and an answer that must match the verdict schema. The prompt goes on stdin.
    """
    return [
        executable,
        "--print",
        "--safe-mode",
        "--model",
        model,
        "--tools",
        "",
        "--strict-mcp-config",
        "--no-session-persistence",
        "--system-prompt",
        system,
        "--json-schema",
        json.dumps(VERDICT_JSON_SCHEMA),
        "--output-format",
        "json",
    ]


def _claude_code_workdir() -> Path:
    """An empty directory to run Claude Code in, so no project files or instructions are near it."""
    workdir = Path(tempfile.gettempdir()) / "cfo_calibration_claude_code"
    workdir.mkdir(exist_ok=True)
    return workdir


def is_usage_limit(message: str) -> bool:
    lowered = message.lower()
    return any(marker in lowered for marker in _CLAUDE_CODE_LIMIT_MARKERS)


async def run_claude_code(prompt: str, system: str, model: str) -> dict[str, Any]:
    """Runs one headless Claude Code call and returns its JSON result; the plan's usage limit stops the run."""
    executable = shutil.which("claude")
    if executable is None:
        raise RuntimeError("Claude Code (claude) is not on PATH")
    process = await asyncio.create_subprocess_exec(
        *claude_code_command(executable, model, system),
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=_claude_code_workdir(),
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(prompt.encode("utf-8")), timeout=CLAUDE_CODE_TIMEOUT_SECONDS
        )
    except TimeoutError:
        process.kill()
        await process.wait()
        raise RuntimeError(f"Claude Code did not answer within {CLAUDE_CODE_TIMEOUT_SECONDS} seconds") from None
    output = stdout.decode("utf-8", errors="replace")
    try:
        result = json.loads(output)
    except json.JSONDecodeError:
        message = (stderr.decode("utf-8", errors="replace") or output).strip()
        if is_usage_limit(message):
            raise QuotaExhaustedError(message) from None
        raise RuntimeError(f"Claude Code exited with {process.returncode}: {message[:300]}") from None
    if result.get("is_error"):
        message = str(result.get("result") or result.get("subtype") or "unknown error")
        if is_usage_limit(message):
            raise QuotaExhaustedError(message)
        raise RuntimeError(f"Claude Code failed: {message[:300]}")
    return result


async def ask_claude_code(case: TradeCase, view: MarketView, variant: str, model: str) -> Verdict:
    """
    Asks Claude through Claude Code, so the run uses a Claude subscription instead of an API key. `model` is the
    stored name, `claude-code/<model id>`. The model gets the sealed prompt and can call no tool, so it can only
    answer from what was known at the decision time.
    """
    verdict = Verdict(
        key=case.key,
        variant=variant,
        model=model,
        prompt_version=PROMPT_VERSION,
        status=STATUS_ERROR,
        probability=None,
        reasoning="",
    )
    try:
        float_check = None
        float_value = case.listing.get("float_value") if case.listing is not None else None
        if float_value is not None:
            float_check = await verify_float_value(case.trade.market_hash_name, float_value)
        prompt = render_sealed_prompt(case, view, float_check)
        result = await run_claude_code(prompt, SEALED_SYSTEMS[variant], model.removeprefix(CLAUDE_CODE_MODEL_PREFIX))
        verdict.llm_calls = 1
        parsed = _read_claude_code_answer(result, verdict)
    except QuotaExhaustedError:
        raise
    # Broad on purpose: one failed trade is recorded as an error and retried on the next run.
    except Exception as error:
        verdict.reasoning = f"Evaluation failed: {error}"
        return verdict
    verdict.status = STATUS_OK
    verdict.probability = parsed.probability_of_profit
    verdict.reasoning = parsed.reasoning.encode("ascii", "ignore").decode("ascii")
    return verdict


def _read_claude_code_answer(result: dict[str, Any], verdict: Verdict) -> CalibratedVerdict:
    """Adds the call's token use to the verdict and returns the answer, from the structured output if there is one."""
    usage = result.get("usage") or {}
    verdict.prompt_tokens = sum(
        int(usage.get(name) or 0) for name in ("input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens")
    )
    verdict.completion_tokens = int(usage.get("output_tokens") or 0)
    answer = result.get("structured_output")
    return CalibratedVerdict(**answer) if isinstance(answer, dict) else parse_verdict(str(result.get("result") or ""))


def verdict_cache_key(variant: str, key: str, model: str) -> tuple[str, str, str, int]:
    return (variant, key, model, PROMPT_VERSION)


def read_verdicts(path: Path) -> list[Verdict]:
    if not path.exists():
        return []
    verdicts = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            verdicts.append(Verdict(**json.loads(line)))
    return verdicts


def latest_verdicts(verdicts: Iterable[Verdict]) -> dict[tuple[str, str, str, int], Verdict]:
    """The last verdict per variant, trade, model and prompt version; a later success replaces an earlier error."""
    latest: dict[tuple[str, str, str, int], Verdict] = {}
    for verdict in verdicts:
        cache_key = verdict_cache_key(verdict.variant, verdict.key, verdict.model)
        if verdict.prompt_version == PROMPT_VERSION and (cache_key not in latest or latest[cache_key].status != STATUS_OK):
            latest[cache_key] = verdict
    return latest


type AskFunction = Callable[[TradeCase, MarketView, str, str], Awaitable[Verdict]]


async def run_verdicts(
    cases: Sequence[TradeCase],
    history: MarketHistory,
    variants: Sequence[str],
    model: str,
    out_path: Path,
    ask: AskFunction | None = None,
    concurrency: int = 1,
) -> dict[str, int]:
    """
    Asks the CFO about every case and variant not already answered, appending each verdict as it arrives.

    Cases go in a seeded random order with every variant asked about one case before the next, so a run that stops
    at a daily quota leaves a random sample of trades answered by both prompts, not the first days of the range.
    Up to `concurrency` questions are open at once; a quota stops new ones, and the run raises once the open ones end.
    """
    ask = ask or ask_cfo
    done = {key for key, verdict in latest_verdicts(read_verdicts(out_path)).items() if verdict.status == STATUS_OK}
    counts = {"asked": 0, "skipped": 0, "errors": 0}
    ordered = list(cases)
    random.Random(SAMPLE_SEED).shuffle(ordered)
    pending = []
    for case in ordered:
        for variant in variants:
            if verdict_cache_key(variant, case.key, model) in done:
                counts["skipped"] += 1
            else:
                pending.append((case, variant))
    slots = asyncio.Semaphore(concurrency)
    quota_errors: list[QuotaExhaustedError] = []
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as out:

        async def answer(case: TradeCase, variant: str) -> None:
            async with slots:
                if quota_errors:
                    return
                try:
                    verdict = await ask(case, history.as_of(case.trade.decided_at), variant, model)
                except QuotaExhaustedError as error:
                    quota_errors.append(error)
                    return
                out.write(json.dumps(asdict(verdict), sort_keys=True) + "\n")
                out.flush()
                counts["asked"] += 1
                counts["errors"] += verdict.status != STATUS_OK
                logger.info(
                    "[CFO] %s %s p=%s tokens=%d+%d",
                    variant,
                    case.key,
                    verdict.probability,
                    verdict.prompt_tokens,
                    verdict.completion_tokens,
                )

        await asyncio.gather(*(answer(case, variant) for case, variant in pending))
    if quota_errors:
        raise quota_errors[0]
    return counts


def estimate_tokens(cases: Sequence[TradeCase], history: MarketHistory) -> dict[str, float]:
    """A rough size of one run per variant: the prompt and the expected tool results, at 4 characters a token."""
    schema_chars = len(json.dumps(TOOL_SCHEMAS))
    per_case = []
    for case in cases:
        view = history.as_of(case.trade.decided_at)
        tool_chars = len(json.dumps(view.market_floor(case.trade.market_hash_name)))
        tool_chars += len(json.dumps(view.search_baselines(parse_version_from_name(case.trade.market_hash_name)[0][:12])))
        prompt_chars = len(NEUTRAL_SYSTEM) + len(render_case_prompt(case)) + schema_chars
        # About three calls per trade, each one resending the conversation so far.
        per_case.append((3 * prompt_chars + 2 * tool_chars) / 4)
    return {
        "trades": len(cases),
        "input_tokens_per_trade": statistics.mean(per_case) if per_case else 0.0,
        "input_tokens_per_variant": sum(per_case),
    }


# --- Loading from the database ---

_LISTING_DETAILS_QUERY = """
SELECT DISTINCT ON (listing_id) listing_id, float_value, pattern, stickers
FROM live_market_ticks
WHERE venue = :venue AND event_type = 'listed' AND listing_id = ANY(:listing_ids)
ORDER BY listing_id, inserted_at
"""

_BUILDS_QUERY = """
SELECT id, built_at FROM baseline_builds WHERE venue = :venue AND built_at <= :until ORDER BY built_at
"""

_BASELINE_ROWS_QUERY = """
SELECT build_id, market_hash_name, latest_price_cents, rolling_30d_avg_cents, rolling_90d_avg_cents, volatility_cents,
       support_floor_cents, avg_volume_30d, drift_percent, median_24h_cents, volume_24h, median_7d_cents, volume_7d,
       min_30d_cents, volume_30d
FROM venue_baselines
WHERE build_id = ANY(:build_ids)
"""

_BASELINE_COLUMNS = (
    "latest_price_cents",
    "rolling_30d_avg_cents",
    "rolling_90d_avg_cents",
    "volatility_cents",
    "support_floor_cents",
    "avg_volume_30d",
    "drift_percent",
    "median_24h_cents",
    "volume_24h",
    "median_7d_cents",
    "volume_7d",
    "min_30d_cents",
    "volume_30d",
)


async def fetch_listing_details(listing_ids: Sequence[str]) -> dict[str, dict[str, Any]]:
    if not listing_ids:
        return {}
    async with async_engine.connect() as conn:
        result = await conn.execute(text(_LISTING_DETAILS_QUERY), {"venue": VENUE, "listing_ids": list(listing_ids)})
        return {
            listing_id: {"float_value": float_value, "pattern": pattern, "stickers": stickers}
            for listing_id, float_value, pattern, stickers in result.fetchall()
        }


async def fetch_baseline_snapshots(until: datetime) -> list[BaselineSnapshot]:
    """Every build whose source data was fetched by `until`; a later build could leak prices into a decision."""
    async with async_engine.connect() as conn:
        builds = (await conn.execute(text(_BUILDS_QUERY), {"venue": VENUE, "until": until})).fetchall()
        rows: dict[int, dict[str, dict[str, Any]]] = {build_id: {} for build_id, _ in builds}
        if builds:
            result = await conn.execute(text(_BASELINE_ROWS_QUERY), {"build_ids": list(rows)})
            for row in result.fetchall():
                rows[row[0]][row[1]] = dict(zip(_BASELINE_COLUMNS, row[2:], strict=True))
    return [BaselineSnapshot(build_id=build_id, built_at=built_at, rows=rows[build_id]) for build_id, built_at in builds]


async def load_cases(log: DecisionLog, as_of: datetime) -> tuple[list[TradeCase], MarketHistory]:
    """The labeled trades of a decision log, as the scorecard labels them, and the market data before each one."""
    config = LabelConfig()
    trades = collect_trades(log.decisions, scorable_start(log))
    listing_trades = [trade for trade in trades if trade.tick_kind == TICK_KIND_LISTING]
    snapshot_trades = [trade for trade in trades if trade.tick_kind == TICK_KIND_REST_SNAPSHOT]

    labels = await fetch_listing_labels([str(trade.listing_id) for trade in listing_trades], LABEL_VERSION)
    for trade in listing_trades:
        apply_listing_label(trade, labels.get(str(trade.listing_id)), config, as_of)
    mature = [trade for trade in snapshot_trades if matures_by(trade.decided_at, config, as_of)]
    sold_index = SoldIndex()
    if mature:
        names = sorted({trade.market_hash_name for trade in mature})
        last = max(trade.decided_at for trade in mature)
        first = min(trade.decided_at for trade in mature)
        horizon_end = last + timedelta(seconds=config.horizon_seconds)
        sold_index = SoldIndex.build(await fetch_listing_events(config.venue, "sold", first, horizon_end, names=names))
    label_snapshot_trades(snapshot_trades, sold_index, config, as_of)

    labeled = [trade for trade in trades if trade.outcome == OUTCOME_LABELED]
    features = decision_features(log, labeled)
    details = await fetch_listing_details([str(trade.listing_id) for trade in labeled if trade.listing_id is not None])
    cases = [
        TradeCase(
            key=case_key(trade),
            trade=trade,
            features=features.get(case_key(trade), {}),
            listing=details.get(str(trade.listing_id)) if trade.listing_id is not None else None,
        )
        for trade in labeled
    ]
    if not cases:
        return [], MarketHistory(builds=[])

    # Market data ends at the last decision. Each MarketView then cuts it at its own decision time.
    first_decision = min(case.trade.decided_at for case in cases)
    last_decision = max(case.trade.decided_at for case in cases)
    names = sorted({case.trade.market_hash_name for case in cases})
    sold = await fetch_listing_events(
        VENUE, "sold", first_decision - timedelta(days=RECENT_SALES_DAYS), last_decision, names=names
    )
    listed = await fetch_listing_events(
        VENUE, "listed", first_decision - timedelta(hours=RECENT_LISTINGS_HOURS), last_decision, names=names
    )
    builds = await fetch_baseline_snapshots(last_decision)
    return cases, MarketHistory.build(builds, sold, listed)


# --- Scoring ---


def brier_score(probabilities: Sequence[float], outcomes: Sequence[bool]) -> float:
    return sum((probability - float(outcome)) ** 2 for probability, outcome in zip(probabilities, outcomes, strict=True)) / len(
        outcomes
    )


def roc_auc(probabilities: Sequence[float], outcomes: Sequence[bool]) -> float | None:
    """Chance that a profitable trade gets a higher probability than an unprofitable one (ties count half)."""
    positives = [probability for probability, outcome in zip(probabilities, outcomes, strict=True) if outcome]
    negatives = [probability for probability, outcome in zip(probabilities, outcomes, strict=True) if not outcome]
    if not positives or not negatives:
        return None
    wins = sum(
        1.0 if positive > negative else 0.5 if positive == negative else 0.0 for positive in positives for negative in negatives
    )
    return wins / (len(positives) * len(negatives))


def reliability(probabilities: Sequence[float], outcomes: Sequence[bool], bins: int = RELIABILITY_BINS) -> list[dict[str, Any]]:
    """Equal width probability bins with the mean forecast and the observed profitable rate in each."""
    rows = []
    for index in range(bins):
        low = index / bins
        high = (index + 1) / bins
        members = [
            (probability, outcome)
            for probability, outcome in zip(probabilities, outcomes, strict=True)
            if low <= probability < high or (index == bins - 1 and probability == 1.0)
        ]
        row: dict[str, Any] = {"low": low, "high": high, "trades": len(members), "mean_forecast": None, "observed": None}
        if members:
            profitable = sum(1 for _, outcome in members if outcome)
            interval = wilson_interval(profitable, len(members))
            row.update(
                mean_forecast=sum(probability for probability, _ in members) / len(members),
                observed={"value": profitable / len(members), "low": interval[0], "high": interval[1]},
            )
        rows.append(row)
    return rows


def group_summary(trades: Sequence[Trade]) -> dict[str, Any]:
    """Counts and totals always; intervals only at `MIN_SAMPLE` trades."""
    margins = [int(trade.net_margin_cents or 0) for trade in trades]
    profitable = sum(1 for trade in trades if trade.is_profitable)
    cell: dict[str, Any] = {
        "trades": len(trades),
        "profitable": profitable,
        "net_pnl_cents": sum(margins),
        "precision": None,
        "mean_pnl": None,
    }
    if len(trades) >= MIN_SAMPLE:
        low, high = wilson_interval(profitable, len(trades))
        mean_low, mean_high, method = mean_interval(margins)
        cell["precision"] = {"value": profitable / len(trades), "low": low, "high": high}
        cell["mean_pnl"] = {"value": sum(margins) / len(margins), "low": mean_low, "high": mean_high, "interval": method}
    return cell


def paired_veto_difference(trades: Sequence[Trade], vetoed: set[int]) -> dict[str, float]:
    """
    Net P&L of the DRE with the CFO's vetoes minus the DRE alone (the negated P&L of the vetoed trades), with an
    item bootstrap: items are resampled because the trades of one item move together, as in #307 and #309.
    """
    by_item: dict[str, int] = {}
    for index, trade in enumerate(trades):
        difference = -int(trade.net_margin_cents or 0) if index in vetoed else 0
        by_item[trade.market_hash_name] = by_item.get(trade.market_hash_name, 0) + difference
    items = list(by_item.values())
    rng = random.Random(PAIRED_SEED)
    totals = sorted(sum(rng.choices(items, k=len(items))) for _ in range(PAIRED_RESAMPLES))
    return {
        "difference_cents": sum(items),
        "low_cents": totals[int(PAIRED_RESAMPLES * 0.025)],
        "high_cents": totals[int(PAIRED_RESAMPLES * 0.975) - 1],
        "items": len(items),
    }


def score_variant(cases: Sequence[TradeCase], verdicts: dict[str, Verdict]) -> dict[str, Any]:
    """Calibration and the veto comparison for one prompt. A failed verdict keeps the trade, as the DRE alone would."""
    trades = [case.trade for case in cases]
    answered = [(case.trade, verdicts[case.key].probability) for case in cases if case.key in verdicts]
    scored = [(trade, probability) for trade, probability in answered if probability is not None]
    probabilities = [float(probability) for _, probability in scored]
    outcomes = [bool(trade.is_profitable) for trade, _ in scored]
    result: dict[str, Any] = {
        "trades": len(cases),
        "answered": len(scored),
        "errors": sum(1 for case in cases if case.key in verdicts and verdicts[case.key].probability is None),
        "missing": sum(1 for case in cases if case.key not in verdicts),
    }
    if not scored:
        return result
    base_rate = sum(outcomes) / len(outcomes)
    brier = brier_score(probabilities, outcomes)
    reference = brier_score([base_rate] * len(outcomes), outcomes)
    result.update(
        base_rate=base_rate,
        mean_forecast=sum(probabilities) / len(probabilities),
        brier=brier,
        brier_base_rate=reference,
        brier_skill=1 - brier / reference if reference > 0 else None,
        auc=roc_auc(probabilities, outcomes),
        reliability=reliability(probabilities, outcomes),
        dre_alone=group_summary(trades),
    )
    probability_by_key = {case.key: verdicts[case.key].probability for case in cases if case.key in verdicts}
    gates = []
    for threshold in THRESHOLDS:
        vetoed = {
            index
            for index, case in enumerate(cases)
            if probability_by_key.get(case.key) is not None and float(probability_by_key[case.key] or 0.0) < threshold
        }
        gates.append(
            {
                "threshold": threshold,
                "kept": group_summary([trade for index, trade in enumerate(trades) if index not in vetoed]),
                "vetoed": group_summary([trade for index, trade in enumerate(trades) if index in vetoed]),
                "paired": paired_veto_difference(trades, vetoed),
            }
        )
    result["gates"] = gates
    return result


def build_report(
    cases: Sequence[TradeCase],
    verdicts: Iterable[Verdict],
    model: str,
    meta: dict[str, Any],
    usd_per_mtok: tuple[float, float] | None = None,
) -> dict[str, Any]:
    latest = latest_verdicts(verdicts)
    # Score only the trades every prompt has answered (an error counts as an answer), so the prompts are compared
    # on the same trades while a run is still in progress. Once it is done this is every trade.
    present = {variant for (variant, _, m, _) in latest if m == model}
    answered_by = [{key for (v, key, m, _) in latest if v == variant and m == model} for variant in sorted(present)]
    complete = set.intersection(*answered_by) if answered_by else set()
    total = len(cases)
    cases = [case for case in cases if case.key in complete]
    report: dict[str, Any] = {
        "prompt_version": PROMPT_VERSION,
        "model": model,
        "git_commit": git_commit(),
        **meta,
        "thresholds": list(THRESHOLDS),
        "paired_resamples": PAIRED_RESAMPLES,
        "paired_seed": PAIRED_SEED,
        "trades": len(cases),
        "trades_in_range": total,
        "tick_kinds": {
            kind: sum(1 for case in cases if case.trade.tick_kind == kind)
            for kind in (TICK_KIND_LISTING, TICK_KIND_REST_SNAPSHOT)
        },
        "variants": {},
    }
    for variant in VARIANTS:
        by_key = {key: verdict for (v, key, m, _), verdict in latest.items() if v == variant and m == model}
        if not by_key:
            continue
        scored = score_variant(cases, by_key)
        prompt_tokens = sum(verdict.prompt_tokens for verdict in by_key.values())
        completion_tokens = sum(verdict.completion_tokens for verdict in by_key.values())
        scored["usage"] = {
            "llm_calls": sum(verdict.llm_calls for verdict in by_key.values()),
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "tool_calls": sum(len(verdict.tool_calls) for verdict in by_key.values()),
        }
        if usd_per_mtok is not None:
            cost = (prompt_tokens * usd_per_mtok[0] + completion_tokens * usd_per_mtok[1]) / 1_000_000
            scored["usage"]["cost_usd"] = cost
            scored["usage"]["cost_usd_per_trade"] = cost / len(by_key)
        report["variants"][variant] = scored
    return report


# --- Rendering ---


def _interval_text(cell: dict[str, Any] | None, money: bool = False) -> str:
    if cell is None:
        return "too few"
    show = dollars if money else percent
    return f"{show(cell['value'])} ({show(cell['low'])} to {show(cell['high'])})"


def _group_row(name: str, cell: dict[str, Any]) -> str:
    return (
        f"| {name} | {cell['trades']} | {cell['profitable']} | {_interval_text(cell['precision'])} | "
        f"{dollars(cell['net_pnl_cents'])} | {_interval_text(cell['mean_pnl'], money=True)} |"
    )


def render_markdown(report: dict[str, Any]) -> str:
    """Tables of the run, for the write up in docs/benchmarks/cfo_calibration.md."""
    lines = [
        f"Model `{report['model']}`, prompt version {report['prompt_version']}, label `{report.get('label_version')}` "
        f"read as of {report.get('as_of')}, {report['trades']} of {report['trades_in_range']} labeled trades scored "
        f"({report['tick_kinds'][TICK_KIND_LISTING]} feed listings, "
        f"{report['tick_kinds'][TICK_KIND_REST_SNAPSHOT]} REST snapshots).",
        "",
        "| Prompt | Answered | Errors | Base rate | Mean forecast | Brier | Brier, base rate forecast | Skill | AUC "
        "| Calls | Tokens in / out | Cost | Cost per trade |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for variant, scored in report["variants"].items():
        if "brier" not in scored:
            lines.append(f"| {variant} | {scored['answered']} | {scored['errors']} | | | | | | | | | | |")
            continue
        usage = scored["usage"]
        cost = f"${usage['cost_usd']:.2f}" if "cost_usd" in usage else "unknown"
        per_trade = f"${usage['cost_usd_per_trade']:.4f}" if "cost_usd_per_trade" in usage else "unknown"
        skill = "" if scored["brier_skill"] is None else f"{scored['brier_skill']:.3f}"
        auc = "" if scored["auc"] is None else f"{scored['auc']:.3f}"
        lines.append(
            f"| {variant} | {scored['answered']} | {scored['errors']} | {percent(scored['base_rate'])} | "
            f"{percent(scored['mean_forecast'])} | {scored['brier']:.3f} | {scored['brier_base_rate']:.3f} | {skill} | {auc} | "
            f"{usage['llm_calls']} | {usage['prompt_tokens']} / {usage['completion_tokens']} | {cost} | {per_trade} |"
        )
    for variant, scored in report["variants"].items():
        if "brier" not in scored:
            continue
        lines += [
            "",
            f"### {variant}: reliability",
            "",
            "| Forecast | Trades | Mean forecast | Observed profitable (95% CI) |",
            "|---|---|---|---|",
        ]
        for row in scored["reliability"]:
            observed = _interval_text(row["observed"]) if row["observed"] else ""
            mean = percent(row["mean_forecast"]) if row["mean_forecast"] is not None else ""
            lines.append(f"| {percent(row['low'])} to {percent(row['high'])} | {row['trades']} | {mean} | {observed} |")
        lines += [
            "",
            f"### {variant}: as a gate on top of the DRE",
            "",
            "| Trades | Count | Profitable | Precision (95% CI) | Net P&L | Per trade (95% CI) |",
            "|---|---|---|---|---|---|",
            _group_row("DRE alone", scored["dre_alone"]),
        ]
        for gate in scored["gates"]:
            lines.append(_group_row(f"kept at {gate['threshold']}", gate["kept"]))
            lines.append(_group_row(f"vetoed at {gate['threshold']}", gate["vetoed"]))
        lines += ["", "| Threshold | Vetoed | Paired with the DRE alone (95% CI, item bootstrap) |", "|---|---|---|"]
        for gate in scored["gates"]:
            paired = gate["paired"]
            lines.append(
                f"| {gate['threshold']} | {gate['vetoed']['trades']} | {dollars(paired['difference_cents'])} "
                f"({dollars(paired['low_cents'])} to {dollars(paired['high_cents'])}) |"
            )
    return "\n".join(lines) + "\n"


# --- Command line ---


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("run", "report"):
        command = commands.add_parser(name)
        command.add_argument("--decisions", type=Path, required=True, help="Scorecard decision log (zscore_dre_sweep)")
        command.add_argument("--as-of", type=parse_utc, required=True, help="Read labels as of this UTC time")
        command.add_argument("--out", type=Path, default=REPO_ROOT / "data" / "cfo_calibration")
        command.add_argument("--max-trades", type=int, default=None, help="Seeded random sample of the labeled trades")
        command.add_argument(
            "--backend",
            choices=(BACKEND_LLM, BACKEND_CLAUDE_CODE),
            default=BACKEND_LLM,
            help="llm: the OpenAI compatible API in LLM_* with the tool loop; claude-code: Claude through Claude Code "
            "(a Claude subscription, no API key) with the sealed prompt",
        )
        command.add_argument("--claude-model", default=DEFAULT_CLAUDE_MODEL, help="Model for --backend claude-code")
    run = commands.choices["run"]
    run.add_argument("--variants", default=",".join(VARIANTS))
    run.add_argument("--dry-run", action="store_true", help="Estimate the size of the run without calling the LLM")
    run.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help=f"Questions open at once (default: 1, or {CLAUDE_CODE_CONCURRENCY} with claude-code)",
    )
    report = commands.choices["report"]
    report.add_argument("--model", default=None, help="Stored model whose verdicts to score (default: the backend's)")
    report.add_argument("--input-usd-per-mtok", type=float, default=None)
    report.add_argument("--output-usd-per-mtok", type=float, default=None)
    return parser


def stored_model(args: argparse.Namespace) -> str:
    """The model name the backend's verdicts are stored under."""
    if args.backend == BACKEND_CLAUDE_CODE:
        return CLAUDE_CODE_MODEL_PREFIX + args.claude_model
    return cfo._get_current_model()


async def run(args: argparse.Namespace) -> int:
    log = read_decision_log(args.decisions)
    cases, history = await load_cases(log, args.as_of)
    cases = sample_cases(cases, args.max_trades)
    verdicts_path = args.out / "verdicts.jsonl"
    meta = {"label_version": LABEL_VERSION, "as_of": args.as_of.isoformat(), "decision_log_sha256": log.sha256}

    if args.command == "run":
        variants = [variant.strip() for variant in args.variants.split(",") if variant.strip()]
        unknown = sorted(set(variants) - set(VARIANTS))
        if unknown:
            raise SystemExit(f"Unknown variants: {', '.join(unknown)}")
        if args.dry_run:
            print(json.dumps({**estimate_tokens(cases, history), "variants": variants}, indent=2))
            return 0
        claude_code = args.backend == BACKEND_CLAUDE_CODE
        concurrency = args.concurrency
        if concurrency is None:
            concurrency = CLAUDE_CODE_CONCURRENCY if claude_code else 1
        if concurrency < 1:
            raise SystemExit("--concurrency must be at least 1")
        if claude_code and shutil.which("claude") is None:
            raise SystemExit("--backend claude-code needs Claude Code (claude) on PATH, logged in")
        ask = ask_claude_code if claude_code else ask_cfo
        try:
            counts = await run_verdicts(cases, history, variants, stored_model(args), verdicts_path, ask, concurrency)
        except QuotaExhaustedError as error:
            logger.error("[CFO] Quota exhausted, stopping; rerun later to resume: %s", error)
            return 2
        print(json.dumps(counts))
        return 0

    model = args.model or stored_model(args)
    prices = None
    if args.input_usd_per_mtok is not None and args.output_usd_per_mtok is not None:
        prices = (args.input_usd_per_mtok, args.output_usd_per_mtok)
    report = build_report(cases, read_verdicts(verdicts_path), model, meta, prices)
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "report.json").write_text(json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8")
    (args.out / "report.md").write_text(render_markdown(report), encoding="utf-8")
    print(render_markdown(report))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    return asyncio.run(run(build_parser().parse_args(argv)))


if __name__ == "__main__":  # pragma: no cover - entrypoint glue, covered via unit tests
    from shared_utils import validate_required_env

    validate_required_env(["DATABASE_URL"])
    sys.exit(main())
