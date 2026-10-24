# The fee floor

What happened when the DRE stopped approving deals that lose money after the seller fee (#331), measured against the [baseline scorecard](baseline_scorecard.md) (#249).

## The problem

The DRE approves a flagged listing when it is at or under the item's support floor, far under its 30 day average, or a sticker deal. None of these rules looks at Skinport's 8% seller fee, and the only money check before them is a fixed $0.50 of savings against the price window. Reselling at the baseline's latest price only breaks even when that price is at least 1.087 times what I paid, so the DRE approved listings that its own fee aware estimate already said would lose money. The estimate went into the paper trade and the alert, after the approval.

The CFO calibration (#250, [cfo_calibration.md](cfo_calibration.md)) found this. Of 425 labeled approvals, 94 were priced like that, 14% of them were profitable, and they lost $626 of the $737 the DRE lost.

## The rule

An approval by `support_floor` or `macro_sigma` whose estimate, the baseline's latest price less the seller fee less the price, is zero or less is rejected. The replay logs it as `below_fee_floor`, and the listener counts it in `listener_anomalies_rejected_total{reason="below_fee_floor"}`. It is neither alerted nor paper traded. Without a baseline price there is no estimate, and the floor does not apply.

The sticker rules are left alone. The estimate values the item at its baseline price and ignores stickers, and those rules pay up to or above that price for the stickers on top, so the floor would reject every one of them by construction. None of them approved anything in this range anyway.

The 1.087 line follows from the fee. I did not fit it to the labeled trades. `DRE_FEE_FLOOR=false` turns it off.

## How I measured it

- Data: Skinport, decisions from 2026-09-26T17:30 to 2026-10-23T00:00 UTC with 2 hours of warm up, replayed twice with `zscore_dre_sweep` under the current rules (#269, #308, #312). One run had `DRE_FEE_FLOOR=false` and one had the floor. Both made the same 324,807 decisions up to the DRE.
- Both logs scored with `baseline_scorecard.py`, labels `v1` read as of 2026-10-23T21:00 UTC. Labels end around 2026-10-09: later trades are still pending, and 321 approved listings from early October have no label yet because the labeler has not run over them.
- The paired difference is the P&L of the trades the floor removes, with items resampled, because the trades of one item move together.

## Results

| Run | Approvals | Trades | Labeled | Precision (95% CI) | Per trade (95% CI) | Net P&L | Max drawdown | Recall |
|---|---|---|---|---|---|---|---|---|
| Without the floor | 5,186 | 4,903 | 424 | 46.5% (41.8% to 51.2%) | -$1.74 (-$2.64 to -$0.97) | -$739.63 | $745.29 | 27.8% |
| With the floor | 3,845 | 3,642 | 325 | 55.4% (49.9% to 60.7%) | -$0.38 (-$1.08 to $0.25) | -$122.57 | $140.48 | 26.4% |

The floor removed 100 labeled trades, and every one of them had an estimate of zero or less. 17 of them were profitable, and together they lost $624.43. Paired with the run without the floor, the floor is worth $624.43 ($334.99 to $974.74, 304 items). One labeled trade appears only with the floor: an earlier listing of it is no longer bought, so the buy once rule takes a later one.

| Price of the removed trades | Trades | Net P&L |
|---|---|---|
| under $5 | 10 | $2.20 |
| $5 to $50 | 52 | -$61.48 |
| $50 to $500 | 31 | -$330.32 |
| $500 and over | 7 | -$234.83 |

Most of the loss it removes is in the expensive items, where 8% of the price is a lot of money.

By week, with the floor, the first week (2026-09-26 to 2026-10-03, 251 labeled) was 62.9% profitable at +$3.61, and the second (2026-10-03 to 2026-10-10, 74 labeled) was 29.7% at -$126.18.

## What it means

The floor removes trades that were expected to lose, and the labels agree: it cuts the loss by about five sixths and the worst drawdown by about four fifths, for a small drop in recall. It does not make the DRE profitable. With the floor it still loses $0.38 per trade, and the interval only just includes zero. The second week shows that deals which clear the fee at the baseline price can still lose during the 7 day trade hold. Covering that hold with a margin above break even is #332.

## Limits

- One market period of about two weeks of labels, and the second week has few of them.
- `v1` labels are paper outcomes, measured on comparable sales between day 7 and day 14.
- The before and after runs share every decision up to the DRE, so the difference is only the floor, but both carry the same label gaps.
