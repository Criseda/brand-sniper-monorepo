# Student model threshold

How `apps/analytics/train_student.py` picks the student model's operating threshold, and the comparison
behind it (#234).

## The problem

The first exploratory report ranked listings well (ROC-AUC 0.968) but lost money at its own threshold:
273 trades, 44.0% precision, -$1.35 per trade. At the rules' volume (83 trades) the same model made $0.42
per trade. The threshold was the weak part.

It was picked on the newest 20% of each fold's training rows, the same rows that fitted the Platt scaling,
as the probability with the most cumulative net P&L that still approves at least 30 of them. In the fold
that carries the evaluation (cutoff 2026-09-28T17:00, 2117 training listings, 4158 validation listings)
that slice held 424 rows. Approving 30 of 424 is an approval rate of 7%, while the rules approve about 1.2%
of listings. The 30 row floor, not the P&L curve, set how low the threshold could go, and the validation
window is ten times the slice, so the model took several times the rules' volume.

I keep the 30 row floor. The promotion verdict needs 30 labeled trades on each side, and shadow mode (#236)
needs approvals to evaluate. What I changed is how many rows the floor is counted on.

## What I compared

Every rule ran on the decision log of `student_model.md` (sha256 `a0c0c45f...`) with `v1` labels as of
2026-10-13T17:00 UTC. All numbers below are out of sample, pooled over the evaluated folds.

| Threshold rule | Trades | Precision | Net P&L | Per trade | Model at the rules' volume, per trade |
|---|---|---|---|---|---|
| Before: argmax of net P&L on the newest 20% | 273 | 44.0% | -$368.11 | -$1.35 | $0.42 |
| Expected value rule on the newest 20% | 171 | 50.9% | -$44.94 | -$0.26 | $0.42 |
| Cap at the rules' approval rate, newest 20% | 171 | 50.9% | -$44.94 | -$0.26 | $0.42 |
| Separate slices: Platt on 60% to 80%, threshold on the newest 20% | 215 | 50.2% | $0.38 | $0.00 | $0.58 |
| Argmax on the newest 40% | 142 | 59.2% | $79.35 | $0.56 | $0.58 |
| Inner walk forward, argmax (chosen) | 114 | 64.9% | $61.30 | $0.54 | $0.37 |
| Inner walk forward, expected value rule or cap | 76 | 72% | $43.32 | $0.57 | $0.37 |

The expected value rule approves when p times the mean profitable margin beats (1 minus p) times the mean
loss, both from the training rows. In this fold the mean win is $1.50 and the mean loss $4.09, so the break
even probability is 0.73. That lies above the probability of the 30th best calibration row, so the floor
decides and the rule gives the same threshold as the cap. With the 30 row floor on a small slice, neither
rule can act.

Making the newest slice larger helps, but it takes rows from the fit, and the result swings with the share:
-$0.59 per trade at 30%, $0.56 at 40%, $0.48 at 50%, $0.14 at 60%. I would be picking a share from one
fold's noise.

The inner walk forward makes the threshold rows larger without taking anything from the fit. The training
rows are cut into five blocks in time. Each block after the first is scored by a model fitted on the blocks
before it, so about 80% of the training rows (1693 in the main fold) get an out of sample score. Those scores
fit the Platt scaling and pick the threshold, and the 30 row floor is then an approval rate of 1.8%, close to
the rules' 1.2%. The deployed model is fitted on every training row, which also improves the ranking
(ROC-AUC 0.974 against 0.968). Every row used is from before the fold cutoff, so nothing looks ahead.

## Is it stable

One fold carries 4158 of the 4525 validation listings, so I checked the chosen rule with other fold lengths
and block counts. Per trade, at the threshold against at the rules' volume:

| Setup | Before | Inner walk forward, argmax | Inner walk forward, cap |
|---|---|---|---|
| 3 hour folds, 5 blocks | -$0.72 vs $0.59 | $0.83 vs $0.79 (81 trades) | $0.81 (57 trades) |
| 6 hour folds, 5 blocks | -$1.35 vs $0.42 | $0.54 vs $0.37 (114 trades) | $0.57 (76 trades) |
| 12 hour folds, 5 blocks | -$1.48 vs $0.16 | $0.53 vs $0.35 (114 trades) | $0.39 (79 trades) |
| 6 hour folds, 3 blocks | | $0.76 vs $0.52 (99 trades) | |
| 6 hour folds, 4 blocks | | $0.61 vs $0.51 (116 trades) | |
| 6 hour folds, 8 blocks | | $0.35 vs $0.52 (118 trades) | |
| 6 hour folds, 10 blocks | | $0.52 vs $0.37 (116 trades) | |

The argmax on inner walk forward scores was at least as good per trade as the model at the rules' volume in
every setup but one (8 blocks). The cap gives about the same per trade with fewer trades and less total P&L.
I chose the argmax: it keeps the existing `choose_threshold`, makes the most net P&L, and leaves more trades
for the verdict and for shadow mode.

## What this does not show

This is an exploratory run. A strict run is not possible yet: final labels as of 2026-10-14 cover 2063 of
these listings, and every one of them became available after the fold cutoffs (Sept 26 to 28), so a strict
fold has no training rows until the log spans more than the 14 day label horizon. The training labels also
overlap the validation window. The comparison checks the threshold rule; it does not say the model beats the
rules. The calibration above 0.5 is still uneven (the 0.5 to 0.6 bin holds 21 listings, 9.5% profitable),
which the strict run should check again.
