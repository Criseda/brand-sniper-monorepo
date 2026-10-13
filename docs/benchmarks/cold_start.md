# Decisions right after a restart

How I chose what the listener does while an item's price window is still too short to score (#308), measured
with the restart replay of [price_window.md](price_window.md) (#269). Everything here is in sample: the labels
cover two restarts in one week, and I report the rule on the same labels I chose it with.

## The question

The edge Redis only lives in RAM, so after my PC has been off every price window is empty. Until a window holds
`MIN_HISTORY_POINTS` (4) prices, `calculate_z_score` compares the price with the baseline alone (the macro Z
score). That is the DRE's own `macro_sigma` test, so a macro Z score under the threshold is approved almost by
construction, and here every one of them was approved by the `support_floor` rule.

In the #269 restart replay about half of the approvals were macro Z approvals. 90% of them came on the first
REST poll after a restart: every item's lowest ask is scored once against an empty window, and an ask that sits
under the support floor looks like a bargain even when it has been there for a while. Without the restart the
same asks are scored against a full window and only 3 are approved in the whole range.

## How I measured it

- Data and labels as in [price_window.md](price_window.md): Skinport, decisions from 2026-09-26T17:30 to
  2026-10-13T01:27 UTC with 2 hours of warm up, the shipped #269 window rule, replayed with `zscore_dre_sweep`
  and scored with `baseline_scorecard.py`.
- Restarts as in #269: at every gap of more than 30 minutes in the recorded events (7 of them, from 2.5 hours to
  8 days long) the dedup state is emptied, and the windows are emptied or warmed depending on the candidate.
- Labels read as of 2026-10-13T13:50 UTC. They reach 2026-09-29 12:50, so they cover the 7.5 hours after the
  restart at 2026-09-28 16:04 and the first 1.7 hours after the one at 2026-09-29 11:08. The later restarts are
  not labeled yet. A REST snapshot is labeled from the sale feed when the scorecard runs, so these labels hold
  more REST trades than the #269 tables, which were read at 01:40.
- Paired differences match trades by venue, item, price, tick kind and UTC day, and resample items (4,000
  resamples), because the trades of one item move together.

The candidates:

| Candidate | What happens while a window holds fewer than 4 prices |
|---|---|
| status quo | the macro Z score decides, as today |
| abstain | no approval: the macro Z score is logged but never triggers the DRE |
| warm, unbounded | windows keep every price they had before the restart (the best a warm start could do) |
| warm, 24 or 6 hours before shutdown | windows keep the prices from the last 24 or 6 hours before the PC went off |
| warm, 24 or 6 hours before restart | windows keep the prices from the 24 or 6 hours before the restart |
| macro threshold Z | a macro Z score triggers only below Z (-2.5, -3, -4 or -6) instead of -2 |

A warm start would read recent `live_market_ticks` from PostgreSQL when the listener starts. A lookback counted
back from the restart finds nothing after most of my restarts, because the PC was off for longer than that, so
I also counted it back from the last tick before the shutdown.

## Results

Cells read trades / labeled, precision, P&L per trade. A cell needs 30 labeled trades; below that it says too few.

| Candidate | Trades | Labeled | Precision (95% CI) | Per trade (95% CI) | Net P&L | Recall | Macro Z share of approvals | Paired with status quo (95% CI) |
|---|---|---|---|---|---|---|---|---|
| status quo | 3134 | 195 | 47.2% (40.3% to 54.2%) | -$1.33 (-$2.38 to -$0.38) | -$259.67 | 22.1% | 49% | |
| **abstain (shipped)** | 1573 | 135 | 58.5% (50.1% to 66.5%) | -$0.40 (-$1.40 to $0.43) | -$53.79 | 21.8% | 0% | +$205.88 (+$52.29 to +$405.78) |
| warm, unbounded | 3000 | 182 | 50.5% (43.3% to 57.7%) | -$1.14 (-$2.01 to -$0.31) | -$206.89 | 22.5% | 0% | +$52.78 (-$88.06 to +$215.64) |
| warm, 24 hours before shutdown | 2969 | 183 | 50.3% (43.1% to 57.4%) | -$1.06 (-$1.95 to -$0.26) | -$193.38 | 22.5% | 12% | +$66.29 (-$76.06 to +$220.57) |
| warm, 6 hours before shutdown | 2863 | 178 | 52.2% (44.9% to 59.5%) | -$1.04 (-$1.94 to -$0.23) | -$185.85 | 22.9% | 12% | +$73.82 (-$61.49 to +$229.76) |
| warm, 24 hours before restart | 2815 | 180 | 48.9% (41.7% to 56.1%) | -$1.18 (-$2.18 to -$0.28) | -$212.00 | 22.1% | 37% | +$47.67 (-$36.86 to +$148.27) |
| warm, 6 hours before restart | 3115 | 195 | 47.2% (40.3% to 54.2%) | -$1.33 (-$2.38 to -$0.38) | -$259.67 | 22.1% | 46% | $0.00 |
| macro threshold -2.5 | 2431 | 155 | 56.1% (48.3% to 63.7%) | -$0.71 (-$1.75 to $0.22) | -$109.35 | 22.1% | 35% | +$150.32 (+$53.21 to +$277.12) |
| macro threshold -3 | 2125 | 144 | 57.6% (49.5% to 65.4%) | -$0.87 (-$2.00 to $0.15) | -$125.44 | 22.1% | 26% | +$134.23 (+$29.12 to +$265.60) |
| macro threshold -4 | 1818 | 140 | 57.9% (49.6% to 65.7%) | -$0.87 (-$2.05 to $0.12) | -$122.32 | 21.8% | 13% | +$137.35 (+$32.28 to +$268.02) |
| macro threshold -6 | 1657 | 138 | 57.2% (48.9% to 65.2%) | -$0.90 (-$2.10 to $0.09) | -$123.68 | 21.8% | 5% | +$135.99 (+$30.54 to +$267.24) |
| no restarts at all (for reference) | 2944 | 182 | 50.5% (43.3% to 57.7%) | -$1.14 (-$2.01 to -$0.31) | -$206.89 | 22.5% | 0% | |

| Candidate | Feed listings | REST snapshots | Hybrid Z | Macro Z |
|---|---|---|---|---|
| status quo | 965 / 105, 57.1%, -$0.50 | 2169 / 90, 35.6%, -$2.30 | 1572 / 135, 58.5%, -$0.40 | 1562 / 60, 21.7%, -$3.43 |
| **abstain (shipped)** | 944 / 104, 56.7%, -$0.51 | 629 / 31, 64.5%, -$0.02 | 1573 / 135, 58.5%, -$0.40 | none |
| warm, unbounded | 1005 / 109, 56.0%, -$0.73 | 1995 / 73, 42.5%, -$1.75 | 2997 / 182, 50.5%, -$1.14 | 3 / 0, too few |
| warm, 24 hours before shutdown | 986 / 109, 56.0%, -$0.73 | 1983 / 74, 41.9%, -$1.54 | 2625 / 183, 50.3%, -$1.06 | 344 / 0, too few |
| macro threshold -3 | 948 / 105, 57.1%, -$0.50 | 1177 / 39, 59.0%, -$1.86 | 1573 / 135, 58.5%, -$0.40 | 552 / 9, too few |

| Candidate | under $5 | $5 to $50 | $50 to $500 | $500 and over |
|---|---|---|---|---|
| status quo | 976 / 57, 64.9%, $0.45 | 1364 / 104, 44.2%, -$0.12 | 743 / 32, 25.0%, -$7.81 | 51 / 2, too few |
| **abstain (shipped)** | 478 / 38, 76.3%, $0.31 | 636 / 76, 56.6%, $0.54 | 421 / 19, too few | 38 / 2, too few |
| warm, unbounded | 858 / 47, 80.9%, $0.32 | 1255 / 100, 46.0%, $0.30 | 799 / 33, 21.2%, -$6.97 | 88 / 2, too few |

The macro Z approvals lose money after both labeled restarts, on their own. After 2026-09-28 16:04 the status
quo makes 27 labeled macro trades that abstaining drops, 4 of them profitable, -$123.12 in total over 27 items.
After 2026-09-29 11:08 it makes 33, 9 profitable, -$82.76 over 32 items. The first restart was all I could
label when I started, so the second one is the closest thing to a fresh check I have. Their worst single
trades are -$43.94 and -$34.57, so no one trade carries the result.

Abstaining costs almost no recall (one profitable listing, 22.1% to 21.8%) because nearly every macro approval
is a REST lowest ask, and 90% of the feed listings it skips come in the first 10 minutes after a restart. It halves the
number of trades, and the trades it drops are the losing ones.

Warming the windows does not help much. With every old price kept, the macro approvals disappear, but the
hybrid Z score then compares today's asks with prices from before the PC went off, often days earlier, and the
trades it adds lose too (47 labeled, 31.9% profitable, -$2.06 per trade). None of the warm starts is clearly
better than the status quo. A lookback counted from the restart keeps nothing after most restarts and changes
almost nothing.

A stricter macro threshold helps, but at every threshold I tried it does worse than not using the macro Z score
at all, and the macro trades it keeps are too few to say whether any threshold is safe.

## What I chose

Abstain. A macro Z score is still computed and written to the decision log (reason `macro_only`), but it never
triggers the DRE. `zscore.source_can_trigger` holds the rule, and the live listener and the replay both call it.
`MACRO_ZSCORE_TRIGGERS=true` brings the old behaviour back, and the replay header records the setting as
`macro_zscore_triggers`.

I checked that the change does what the experiment did. Replayed with restarts, it approves exactly the 1,745
ticks the filtered status quo log approves. Replayed without restarts it drops 3 approvals, all macro Z, none
of them labeled, and leaves the scorecard unchanged.

With the #269 window rule an item's window fills from every poll until it holds 4 prices, so for an item the
REST poller sees, abstaining lasts about three polls, 15 minutes, after a restart. An item seen only in the
feed stays on the macro Z score until it has had 3 earlier prices in the feed.

The rules are still not profitable, and the result rests on about 60 labeled trades over two restarts. The next
scorecard runs will label the five later restarts in this range (2026-09-30 to 2026-10-12), which is where this
has to hold before I trust it.
