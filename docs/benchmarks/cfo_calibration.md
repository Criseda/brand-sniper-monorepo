# CFO calibration

How well the CFO, the LLM that audits each paper trade, predicts whether a trade makes money (#250). I scored it against the `v1` outcome labels of the [baseline scorecard](baseline_scorecard.md) (#249) and compared it with simple rules that need no LLM.

## How I measured it

The trades are the 202 labeled approvals of the published scorecard: Skinport, decisions from 2026-09-26T17:30 to 2026-09-29T15:01 UTC, labels read as of 2026-10-13T01:40:07.555120. 48.5% of them were profitable and together they lost $290.44, or $1.44 per trade.

For each trade the CFO sees what the bot knew when it decided: the decision, the listing's float, pattern and stickers, and market data from the baseline build in effect and the feed events received before the decision. The tests check that nothing later reaches it. It answers with a probability of profit and two or three sentences of reasoning. I asked every trade twice, once with the production adversarial prompt and once with a neutral prompt that asks for a calibrated probability.

I ran three setups.

| Setup | What it is | Verdicts |
|---|---|---|
| `gpt-oss-120b` | The production setup: Groq free tier, the CFO's three tools (market floor, name search over our baselines, float check) | 162 of 404, both prompts on 81 trades, before the daily token limit stopped it |
| Sonnet 5.5, sealed | Claude through Claude Code on my Claude Pro plan, no tools; the market floor and the float check are in the prompt, the name search is left out | 404 of 404 |
| Sonnet 5.5 with tools | Claude through Claude Code with the same three tools, served from a local MCP server | 404 of 404 |

Nothing cost money. Groq's free tier is free and Claude Code uses my plan. A sealed Sonnet verdict used about 3,200 tokens in and 370 out, and all 404 took about 15 minutes with 4 questions open at once. With tools Sonnet took 4.6 turns and about 7,500 tokens in and 530 out per verdict. `gpt-oss-120b` made 2.6 calls and used about 2,900 tokens in and 680 out per verdict.

## Results

All 202 trades, against the DRE alone. A Brier score of 0.250 is what always forecasting the base rate gets, and an AUC of 0.5 is a coin flip. A trade is kept when its probability is at least 0.7, the cut the production CFO uses.

| Setup | Prompt | Brier | AUC | Kept at 0.7 | Profitable | Net P&L |
|---|---|---|---|---|---|---|
| DRE alone | | | | 202 | 48.5% | -$290.44 |
| Sonnet sealed | adversarial | 0.197 | 0.764 | 52 | 86.5% | $58.14 |
| Sonnet sealed | neutral | 0.192 | 0.793 | 46 | 84.8% | $51.39 |
| Sonnet with tools | adversarial | 0.193 | 0.784 | 40 | 85.0% | $41.82 |
| Sonnet with tools | neutral | 0.193 | 0.800 | 46 | 84.8% | $51.09 |

On the 81 trades that `gpt-oss-120b` answered with both prompts:

| Setup | Adversarial Brier / AUC | Neutral Brier / AUC |
|---|---|---|
| `gpt-oss-120b` | 0.222 / 0.689 | 0.179 / 0.829 |
| Sonnet sealed | 0.173 / 0.835 | 0.205 / 0.748 |
| Sonnet with tools | 0.191 / 0.783 | 0.191 / 0.796 |

The CFO has real skill. Every setup separates winners from losers, its forecasts of 80% and up came true 87% to 92% of the time, and as a gate at 0.7 it keeps about a quarter of the trades at about 85% precision. The paired saving against the DRE alone is $332 to $349 per setup, with item bootstrap intervals from about $150 to $550.

Which setup or prompt I use makes no measurable difference. Every paired interval includes zero:

| Comparison | Trades | Brier difference (95% CI) | AUC difference (95% CI) |
|---|---|---|---|
| Sonnet with tools against sealed, neutral | 202 | +0.001 (-0.012 to +0.013) | +0.007 (-0.033 to +0.049) |
| Sonnet with tools against sealed, adversarial | 202 | -0.004 (-0.020 to +0.013) | +0.020 (-0.025 to +0.063) |
| Sonnet with tools, neutral against adversarial | 202 | -0.001 (-0.009 to +0.008) | +0.016 (-0.008 to +0.043) |
| Sonnet with tools against `gpt-oss-120b`, neutral | 81 | +0.012 (-0.012 to +0.037) | -0.033 (-0.116 to +0.047) |
| Sonnet with tools against `gpt-oss-120b`, adversarial | 81 | -0.030 (-0.069 to +0.007) | +0.094 (-0.006 to +0.201) |

Sonnet used the name search 400 times when it had it, and its answers still correlate 0.87 with the sealed run. The better prompt flips between models, so I read that as noise. Because the tools changed nothing, I kept only the sealed Claude Code backend in the code. It needs no local server and runs Claude Code in safe mode.

## Against rules without an LLM

I also ranked the same 202 trades by a few numbers the bot already has, with the direction fixed before looking.

| Signal | AUC |
|---|---|
| Latest baseline price / purchase price | 0.819 |
| Median sale of the last 7 days / purchase price (187 trades with sales) | 0.736 |
| The bot's estimated net profit | 0.703 |
| Minus the Z score | 0.620 |
| Sales in the last 7 days | 0.561 |

The plain ratio of the latest baseline price to the purchase price ranks the trades at least as well as any CFO setup. Sonnet with tools and the neutral prompt trails it by 0.019 (-0.061 to +0.023), and sealed Sonnet with the adversarial prompt trails it by 0.055 (-0.108 to -0.002). Keeping the same number of trades, the ratio does as well or better.

| Filter | Kept | Profitable | Net P&L |
|---|---|---|---|
| Sonnet with tools, neutral, at 0.7 | 46 | 84.8% | $51.09 |
| Top 46 by the ratio | 46 | 87.0% | $59.35 |
| Sonnet sealed, adversarial, at 0.7 | 52 | 86.5% | $58.14 |
| Top 52 by the ratio | 52 | 86.5% | $61.20 |
| Ratio at least 1.20 | 53 | 84.9% | $61.18 |
| Ratio at least 1.15 and Sonnet at least 0.5 | 78 | 73.1% | $47.28 |

The CFO and the ratio only correlate 0.55, so the CFO looks at other things too, but adding it to the ratio did not help.

I picked the ratio and its cut after seeing these trades, so I wrote the rules down and checked them on the 207 labeled approvals that came after (2026-09-29T15:02 to 2026-10-09T20:11, the current rules with #269, #308 and #312, labels read on 2026-10-23). These trades were harder: 41.1% profitable and -$584.85 for the DRE alone. The ratio still ranked them at 0.753 (0.685 to 0.818), against 0.592 for the Z score and 0.669 for the estimated net profit. Keeping a ratio of at least 1.20 kept 73 trades, 64.4% profitable, for $34.17 ($7.12 to $62.03). A cut at 1.15 did not make money reliably ($8.46, -$74.93 to $69.03).

Part of the reason is the fee. None of the DRE's rules checks Skinport's 8% seller fee, so it approves listings that lose money even when resold at the latest baseline price. That is #331.

## Recommendation

I am not using the CFO as a filter for now. It works, but the ratio does the same job for free and instantly, and much of what the CFO catches is deals that fail the fee, which the DRE should reject itself (#331). I am also not moving the production CFO to Claude, since it scored no better than `gpt-oss-120b`. The CFO can stay as the explainer in alerts, and I will measure it again on top of a DRE that respects the fee (#334). The ratio goes to the student model as a feature (#333), and a margin for the 7 day trade hold is #332.

## Limits

- 202 trades from three days, and 207 more from the ten days after, all in one market period.
- `v1` labels are paper outcomes. A trade counts as profitable when comparable items sold high enough between day 7 and day 14, not because I sold one.
- `gpt-oss-120b` only answered 81 trades, so the comparison with it is wide.
- Claude Code tells the model today's date and my account email. Neither says anything about a trade's outcome.
- I made many comparisons here, so a single interval that just misses zero means little.

## How to run it

From the repository root, `make cfo-calibration` asks `gpt-oss-120b` on the Groq free tier and resumes where the daily limit stopped it. `make cfo-calibration CFO_BACKEND=claude-code` asks Claude through Claude Code on a Claude plan instead, with no API key, and `CFO_CLAUDE_MODEL` picks the model. Verdicts are stored per model, so both can share `data/cfo_calibration`, and the report lands next to them in `report.md`.
