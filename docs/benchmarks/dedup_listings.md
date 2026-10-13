# Duplicate feed listings

Why the listener now scores every feed listing and keeps the 300 second dedup rule for REST snapshots only
(#312). Everything here is in sample, on the labels that were there when I measured it.

## The question

Until #312 `detection.is_duplicate` dropped any tick at the same price as the item's previous tick on the same
venue within 300 seconds. I wrote that rule for REST snapshots, but it dropped feed listings too. A feed listing
has its own `productId`, so a second listing of the item at the same price is a second offer I could buy.
Neither the live listener nor the replay scored it, so the rules could never approve it, and the student
model (#234) had no features for it. Building the #234 training set showed the size of it: of the 6,654
decisive v1 labels for listings first seen in the range below, 4,388 belong to listings whose first sighting
was dropped.

## How I measured it

- Skinport, decisions from 2026-09-26T17:30 to 2026-10-13T16:00 UTC with 2 hours of warm up, replayed with
  `zscore_dre_sweep` and scored with `baseline_scorecard.py`, labels read as of 2026-10-13T18:00 UTC.
- The replay with the old rule reproduces the #234 run exactly (59,045 logged decisions, 3,388 approvals).
- For every dropped listing I recorded whether the item's previous tick was a REST snapshot or another listing,
  and whether it was the same listing seen again.

## What the old rule dropped

26,923 listing ticks (26,127 listings) were dropped, about as many as were scored (27,890 listings).

| Previous tick | Dropped ticks | Decisive labels | Profitable | Mean net margin |
|---|---:|---:|---:|---:|
| Another listing of the item | 19,622 | 4,155 | 0.8% | -$1.34 |
| A REST snapshot | 6,172 | 251 | 10.0% | -$0.84 |
| All | 26,923 | 4,404 | 1.3% | -$1.31 |

15,306 of the listings after another listing came in the same second as it: one seller listing several copies
of a cheap item at once. 1,129 dropped ticks were the same listing seen again at the same price. For
comparison, the listings that were scored were 9.8% profitable (224 of 2,291), with a mean net margin of
-$6.17. Of the 271 listings labeled profitable, 50 were never scored.

## The candidates

- **Old rule**: a listing at the item's previous price within 300 seconds is a duplicate.
- **Listing ID**: a listing is a duplicate only when the same listing was already seen at the same price. Every
  other listing enters the price window and is scored, like any listing.
- **Scored, not windowed**: the same, except that a listing at the item's previous price is scored but kept out
  of the price window and the dedup state, so the windows stay as they were.

REST snapshots keep the 300 second rule and the unchanged snapshot rule (#265, #269) in all three.

| Rule | Trades (feed listings) | Labeled | Precision (95% CI) | P&L per trade (95% CI) | Net P&L | Recall |
|---|---|---:|---|---|---:|---|
| Old rule | 3,161 (1,081) | 190 | 50.0% (43.0% to 57.0%) | -$1.17 (-$2.06 to -$0.38) | -$222.85 | 22.5% |
| Listing ID | 3,275 (1,191) | 193 | 50.3% (43.3% to 57.2%) | -$1.15 (-$1.99 to -$0.34) | -$221.05 | 22.9% |
| Scored, not windowed | 3,283 (1,202) | 191 | 50.3% (43.2% to 57.3%) | -$1.16 (-$2.00 to -$0.38) | -$220.90 | 22.9% |

| Rule | Labeled listings whose first sighting was scored | Profitable ones scored |
|---|---:|---:|
| Old rule | 2,266 of 6,654 | 221 of 271 |
| Listing ID | 6,642 of 6,654 | 271 of 271 |
| Scored, not windowed | 6,642 of 6,654 | 271 of 271 |

The 12 listings still not scored under the new rules were first seen during the warm up, so their first logged
sighting is the repeat.

## What I chose

Listing ID. The trading numbers barely move: about 110 more feed listing trades over the whole range, nearly
all of them still pending, and no change outside the intervals. The newly scored listings mostly repeat the
price of a listing that was just scored, so they get the same Z score and the rules decide the same way. What
changes is coverage. Almost every labeled listing now has a scored first sighting, so the scorecard's recall
and the student model see the listings the old rule hid. Keeping these listings out of the window gave the
same numbers with more code, so I did not ship it.

A listing seen again at the same price is still a duplicate, however much later. It is the same offer, and
since #271 it can only be bought once anyway. Seen at a new price it is scored again. The listener remembers
the last price of up to `DEDUP_CACHE_MAX_SIZE` listings per venue, so after a restart, or once that many newer
listings have arrived, a repeat is scored once more. The buy once rule still stops a second purchase.
