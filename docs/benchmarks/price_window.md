# What the price window holds

How I chose what goes into the listener's price window (#269), measured against the [baseline scorecard](baseline_scorecard.md) (#249).
Everything here is in sample: I chose the rule on the same labeled week I report it on.

## The question

Each item has a window of its last 20 prices on one venue, and the Z score compares a new price with it. Until
#269 every REST poll put the item's lowest ask into the window, even when it had not changed since the last poll.
A quiet item's window then held 20 copies of one price taken 5 minutes apart, its local variance went to about
zero, the hybrid Z score leaned on the macro prior, and a listing a little below that price looked like an
outlier. Keeping only price changes starves the window instead and moves scoring to the macro Z score, which is
the same test as the DRE's `macro_sigma` rule.

## How I measured it

- Data: Skinport, decisions from 2026-09-26T17:30 to 2026-10-13T01:27 UTC with 2 hours of warm up, the range of
  the published baseline. Labels read as of 2026-10-13T01:40:07.555120 UTC, so every rule is scored against the
  same labels. Rescoring the replay of the old rule gives the published scorecard exactly.
- Each rule was replayed with `zscore_dre_sweep` and scored with `baseline_scorecard.py`. The replay of the rule
  I shipped makes exactly the decisions of its experiment run (3,150 approvals).
- All labels come from 2026-09-26 17:30 to 2026-09-29 00:27, one weekly slice. Most trades are still pending.

The rules only differ in what happens to an unchanged REST lowest ask (one at the same price as the item's
previous poll). It is never scored again (#265) in any of them. Feed listings and changed asks always enter.

| Rule | An unchanged REST ask enters the window |
|---|---|
| every poll | always (the #249 baseline) |
| changes only | never |
| distinct prices | never, and a price already in the window is replaced by the new one |
| feed listings only | never, and REST asks leave the window once they are scored |
| once per N | when no REST ask of the item entered the window in the last N |
| hourly, filled first | while the window holds fewer than `MIN_HISTORY_POINTS` (4) prices, or when no REST ask entered it in the last hour |

## Results

Trades count each purchase once, as live trading buys it. Recall covers feed listings only. A cell needs 30
labeled trades; below that it says too few. Cells read trades / labeled, precision, P&L per trade.

| Rule | Trades | Labeled | Precision (95% CI) | Per trade (95% CI) | Net P&L | Recall | Median window at approval | Macro Z share of approvals |
|---|---|---|---|---|---|---|---|---|
| every poll (#249 baseline) | 3155 | 202 | 48.5% (41.7% to 55.4%) | -$1.44 (-$2.37 to -$0.65) | -$290.44 | 25.8% | 20 | 0% |
| changes only | 2612 | 149 | 50.3% (42.4% to 58.3%) | -$1.10 (-$2.13 to -$0.16) | -$163.40 | 19.9% | 8 | 17% |
| distinct prices | 2602 | 150 | 51.3% (43.4% to 59.2%) | -$1.09 (-$2.11 to -$0.19) | -$164.21 | 20.3% | 7 | 19% |
| feed listings only | 3248 | 174 | 47.7% (40.4% to 55.1%) | -$1.31 (-$2.37 to -$0.41) | -$228.45 | 21.8% | 2 | 74% |
| once per 15 min | 3039 | 194 | 49.0% (42.0% to 56.0%) | -$1.19 (-$2.06 to -$0.48) | -$231.71 | 24.7% | 20 | 0% |
| once per 30 min | 3026 | 174 | 51.7% (44.3% to 59.0%) | -$1.09 (-$1.99 to -$0.28) | -$190.17 | 23.6% | 20 | 0% |
| once an hour | 2896 | 157 | 55.4% (47.6% to 63.0%) | -$0.99 (-$2.00 to -$0.15) | -$155.93 | 22.1% | 20 | 0% |
| once per 2 hours | 2768 | 149 | 54.4% (46.4% to 62.2%) | -$1.05 (-$2.13 to -$0.17) | -$156.34 | 21.8% | 19 | 2% |
| once per 3 hours | 2726 | 149 | 54.4% (46.4% to 62.2%) | -$1.08 (-$2.06 to -$0.16) | -$160.61 | 21.4% | 16 | 3% |
| once per 6 hours | 2651 | 146 | 52.1% (44.0% to 60.0%) | -$1.20 (-$2.26 to -$0.30) | -$175.14 | 20.7% | 12 | 9% |
| **hourly, filled first (shipped)** | 2944 | 162 | 53.1% (45.4% to 60.6%) | -$1.00 (-$1.89 to -$0.22) | -$162.39 | 22.5% | 20 | 0% |

| Rule | Feed listings | REST snapshots | Hybrid Z | Macro Z |
|---|---|---|---|---|
| every poll (#249 baseline) | 1095 / 131, 53.4%, -$1.26 | 2060 / 71, 39.4%, -$1.76 | 3152 / 202, 48.5%, -$1.44 | 3 / 0, too few |
| changes only | 896 / 95, 56.8%, -$0.69 | 1716 / 54, 38.9%, -$1.81 | 2168 / 107, 56.1%, -$0.85 | 444 / 42, 35.7%, -$1.71 |
| distinct prices | 906 / 95, 57.9%, -$0.69 | 1696 / 55, 40.0%, -$1.80 | 2101 / 97, 57.7%, -$0.93 | 501 / 53, 39.6%, -$1.40 |
| feed listings only | 963 / 106, 55.7%, -$0.82 | 2285 / 68, 35.3%, -$2.08 | 835 / 38, 50.0%, -$0.76 | 2413 / 136, 47.1%, -$1.47 |
| once per 15 min | 1047 / 128, 52.3%, -$0.95 | 1992 / 66, 42.4%, -$1.67 | 3036 / 194, 49.0%, -$1.19 | 3 / 0, too few |
| once per 30 min | 1008 / 117, 54.7%, -$0.87 | 2018 / 57, 45.6%, -$1.56 | 3023 / 174, 51.7%, -$1.09 | 3 / 0, too few |
| once an hour | 989 / 107, 56.1%, -$0.73 | 1907 / 50, 54.0%, -$1.57 | 2892 / 157, 55.4%, -$0.99 | 4 / 0, too few |
| once per 2 hours | 942 / 101, 58.4%, -$0.61 | 1826 / 48, 45.8%, -$1.97 | 2718 / 138, 55.1%, -$0.88 | 50 / 11, too few |
| once per 3 hours | 930 / 99, 58.6%, -$0.64 | 1796 / 50, 46.0%, -$1.94 | 2635 / 133, 56.4%, -$0.91 | 91 / 16, too few |
| once per 6 hours | 915 / 96, 58.3%, -$0.66 | 1736 / 50, 40.0%, -$2.23 | 2414 / 120, 57.5%, -$0.70 | 237 / 26, too few |
| **hourly, filled first (shipped)** | 1005 / 109, 56.0%, -$0.73 | 1939 / 53, 47.2%, -$1.57 | 2941 / 162, 53.1%, -$1.00 | 3 / 0, too few |

| Rule | under $5 | $5 to $50 | $50 to $500 | $500 and over |
|---|---|---|---|---|
| every poll (#249 baseline) | 842 / 51, 76.5%, $0.25 | 1375 / 116, 45.7%, $0.06 | 862 / 34, 17.6%, -$8.01 | 76 / 1, too few |
| changes only | 765 / 45, 68.9%, $0.59 | 1097 / 75, 48.0%, $0.25 | 672 / 28, too few | 78 / 1, too few |
| distinct prices | 778 / 47, 70.2%, $0.59 | 1078 / 73, 47.9%, $0.22 | 668 / 29, too few | 78 / 1, too few |
| feed listings only | 1003 / 55, 67.3%, $0.52 | 1432 / 88, 42.0%, -$0.10 | 758 / 30, 30.0%, -$7.01 | 55 / 1, too few |
| once per 15 min | 821 / 51, 74.5%, $0.26 | 1299 / 111, 45.9%, $0.19 | 841 / 31, 19.4%, -$7.37 | 78 / 1, too few |
| once per 30 min | 818 / 44, 75.0%, $0.27 | 1290 / 99, 50.5%, $0.37 | 834 / 30, 23.3%, -$6.66 | 84 / 1, too few |
| once an hour | 808 / 42, 81.0%, $0.29 | 1223 / 86, 53.5%, $0.53 | 778 / 28, too few | 87 / 1, too few |
| once per 2 hours | 786 / 43, 74.4%, $0.27 | 1161 / 77, 53.2%, $0.53 | 738 / 28, too few | 83 / 1, too few |
| once per 3 hours | 782 / 45, 73.3%, $0.30 | 1135 / 75, 53.3%, $0.47 | 725 / 28, too few | 84 / 1, too few |
| once per 6 hours | 766 / 43, 69.8%, $0.28 | 1108 / 75, 52.0%, $0.33 | 692 / 27, too few | 85 / 1, too few |
| **hourly, filled first (shipped)** | 819 / 42, 81.0%, $0.31 | 1241 / 91, 49.5%, $0.42 | 796 / 28, too few | 88 / 1, too few |

Every rule loses less than the baseline and none is profitable. The intervals overlap with the baseline's.
The once per N rules are smooth in N: the gain grows from 15 minutes to an hour, stays about level up to 3
hours and fades towards changes only. Changes only, distinct prices and feed listings only move approvals to the
macro Z score, and those approvals do worse than the hybrid ones.

For the expensive tier, which drives the baseline's loss, the raw numbers below the minimum sample are: the
baseline $50 and over has 35 labeled trades, 6 profitable, -$8.88 per trade; the shipped rule has 29, 7
profitable, -$7.36 per trade. Below $50 the shipped rule makes +$51.04 on 133 labeled trades.

## Paired with the baseline

The two runs share most of their trades, so I compared them trade by trade (same venue, item, price, tick kind
and UTC day). The shipped rule drops 743 baseline trades (50 labeled, 34.0% profitable, -$2.53 per trade) and
adds 532 (10 labeled, 50.0% profitable, +$0.14 per trade). Resampling items, because the trades of one item
move together, the shipped rule's labeled P&L is $128.05 better (95% interval $39.00 to $243.84, 4,000
resamples over 166 items).

## After a restart

A replay keeps every window across the gaps in the data, but live the edge Redis only lives in RAM, so after my
PC has been off every window starts empty. Until a window has 4 prices the item is scored with the macro Z score.
To see what that does, I also replayed each rule with the windows and the dedup state emptied at every gap of
more than 30 minutes (7 restarts in this range).

| Rule | Trades | Labeled | Precision (95% CI) | Per trade (95% CI) | Net P&L | Recall | Median window at approval | Macro Z share of approvals |
|---|---|---|---|---|---|---|---|---|
| every poll | 3435 | 202 | 48.0% (41.2% to 54.9%) | -$1.62 (-$2.65 to -$0.70) | -$326.74 | 26.2% | 16 | 44% |
| once an hour | 3123 | 157 | 50.3% (42.6% to 58.0%) | -$0.90 (-$2.10 to $0.19) | -$140.87 | 20.7% | 2 | 61% |
| **hourly, filled first (shipped)** | 3134 | 157 | 51.0% (43.2% to 58.7%) | -$1.17 (-$2.22 to -$0.24) | -$183.53 | 22.1% | 5 | 49% |

| Rule | Feed listings | REST snapshots | Hybrid Z | Macro Z |
|---|---|---|---|---|
| every poll | 1078 / 132, 53.8%, -$1.24 | 2357 / 70, 37.1%, -$2.32 | 1873 / 175, 53.1%, -$1.16 | 1562 / 27, too few |
| once an hour | 943 / 103, 54.4%, -$0.59 | 2180 / 54, 42.6%, -$1.49 | 1224 / 107, 59.8%, -$0.33 | 1899 / 50, 30.0%, -$2.12 |
| **hourly, filled first (shipped)** | 965 / 105, 57.1%, -$0.50 | 2169 / 52, 38.5%, -$2.51 | 1572 / 130, 58.5%, -$0.46 | 1562 / 27, too few |

| Rule | under $5 | $5 to $50 | $50 to $500 | $500 and over |
|---|---|---|---|---|
| every poll | 1037 / 53, 73.6%, $0.50 | 1515 / 111, 46.8%, $0.05 | 829 / 37, 16.2%, -$8.68 | 54 / 1, too few |
| once an hour | 990 / 46, 69.6%, $0.54 | 1360 / 82, 46.3%, $0.09 | 725 / 27, too few | 48 / 2, too few |
| **hourly, filled first (shipped)** | 976 / 45, 71.1%, $0.58 | 1364 / 87, 48.3%, $0.19 | 743 / 24, too few | 51 / 1, too few |

With restarts the baseline already makes 44% of its approvals with the macro Z score, all in the first polls
after a restart. Once an hour without filling makes that worse: a quiet item needs three hours to reach 4 prices,
and 61% of approvals are macro (30.0% profitable, -$2.12 per trade). Filling a short window from every poll
keeps the macro approvals exactly as they are today (the same 1,562 trades) and improves the hybrid ones.
Paired with the baseline under restarts, it is $143.21 better (95% interval $46.73 to $269.35).

## What I chose

Once an hour, filled first (`unchanged_snapshot_hourly_once_filled`, in `shared_utils/price_window.py`). It
keeps the window full (median 20 at approval) and keeps decisions on the hybrid Z score, so the change does not
approve more trades through the macro test. Without restarts it is as good as once an hour, and with restarts it
does not add macro decisions. It also writes about 91% fewer unchanged asks to the edge Redis (1.36 million
instead of 15.2 million over the range).

It does not make the rules profitable. The expensive tier still loses, and so do the macro decisions after a
restart, which are a separate problem from what the window holds. I chose the hour and the rule on this week of
labels, so the improvement has to hold on the weeks that are still pending before I trust it. The next scorecard
runs, once 2026-10-03 to 2026-10-10 is labeled, will show that.
