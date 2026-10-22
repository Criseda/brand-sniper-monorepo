# Ingress to decision latency

How long the listener takes from the moment Skinport writes a listing to the socket until the DRE has decided on
it, where that time goes, and what it costs in CPU and memory (#175). Part 1 measured the path as it was (a
Node.js sidecar, Redis Pub/Sub and the Python listener) and proposed a budget. Part 2 replaced the sidecar and Pub/Sub
with the listener reading the feed itself, measured that against the same scenarios, and records the decision.

## Decision

I keep the listener in Python and read Skinport's feed in Python too. No Rust or Go component, and the Node.js
sidecar and the Redis Pub/Sub relay are gone (`apps/listener/scrapers/skinport_socket.py`).

- Neither path is held back by Python. On the old path the Python work for a listing was about 0.16 ms of 1.3 ms;
  the rest was the hop through Node.js and Pub/Sub and the five Redis round trips of the price window. A compiled
  runtime would speed up the part that is already small, so #32 (a Rust or Go rewrite) is not planned.
- The Python ingress is faster than the sidecar: run back to back in three interleaved rounds, a single listing
  took 1.09 to 1.18 ms at the median against 1.51 to 1.64 ms, and 1.84 to 1.92 ms at p99 against 2.31 to 2.54 ms.
- It loses nothing when the listener falls behind. The sidecar path lost 56% of the `overload` listings without
  a word, because Redis closes a Pub/Sub subscriber that falls 32 MB behind (#326). Without Pub/Sub the feed waits
  in the socket and every listing is decided.
- It is less to run: one process instead of two, no Node.js in the listener image, no npm dependencies, and a lost
  connection shows up in `listener_feed_connected` and `listener_feed_reconnects_total`.
- It meets every target of the budget below, with the same headroom as before.

Both paths read the same events: connected to the live feed next to the production sidecar on 2026-10-22, the
Python client received every event the sidecar received while both were connected, in the same second. The tests
check the decoding, including the MessagePack extensions Skinport's encoder uses for JavaScript `undefined` and
dates, and the backtest replays recorded events through the same parser.

## Part 1: the sidecar path

The sidecar path decided on a single listing in 1.3 ms at the median and 2.7 ms at p99, from the moment the
frame leaves the socket to the DRE's answer. Of that time, 44% is the hop through the Node.js sidecar and Redis
Pub/Sub, 45% is the five Redis round trips of the price window, and 11% (about 0.16 ms) is Python work: parsing,
queueing, the Z score and the trigger. The listener sustains about 1,500 decisions a second, 14 times the busiest
second Skinport has sent us, and the fastest Skinport listing I have recorded sold 9.1 seconds after it appeared on
the feed.

That architecture met the budget below with wide headroom. No measured stage pointed at the Python runtime or a
decoder, so nothing justified a Rust or Go component. Part 2 measured a Python only ingress against the same
scenarios, since the sidecar hop was a large share of a small number.

## Part 1 results

Reference machine: an Intel Core i9-12900K (24 threads) with 16 GB given to Docker Desktop (WSL2, Linux 6.18),
Python 3.12.15, Node.js 24.22.1, Redis 8.10.2. The edge stack runs on this same PC, so it is also the target host.
Every scenario ran three times with the stage probes and three times with only the end to end probes, each run in
fresh processes on an emptied Redis. The tables pool every listing of the runs with only the end to end probes;
the stage tables use the runs with every probe. The raw summaries are in
[`ingress_latency/reference.json`](ingress_latency/reference.json).

| Scenario | Listings | Dropped | p50 ms | p95 ms | p99 ms | Max ms | Decided/s | Listener CPU | Sidecar CPU | Redis CPU | Peak RSS listener / sidecar MB |
|---|---|---|---|---|---|---|---|---|---|---|---|
| single | 30,000 | 0 | 1.30 | 1.96 | 2.65 | 85.8 | 100 | 10% | 4% | 2% | 229 / 120 |
| burst | 30,000 | 0 | 8.84 | 34.8 | 74.7 | 146 | 90 | 7% | 2% | 1% | 233 / 125 |
| large | 30,000 | 0 | 39.4 | 70.4 | 84.4 | 144 | 101 | 7% | 0% | 1% | 197 / 132 |
| saturation | 30,000 | 0 | 3,674 | 6,012 | 6,365 | 6,495 | 1,507 | 95% | 20% | 14% | 306 / 125 |
| overload | 60,000 | 33,408 | 3,460 | 5,601 | 5,815 | 5,956 | 1,454 | 6% | 2% | 1% | 359 / 129 |

CPU is the share of one core over the measured phase. The run to run spread is small: the medians of the three
single runs were 1.36, 1.30 and 1.26 ms, and of the three large runs 38.8, 41.1 and 38.3 ms.

### Where a single listing's time goes

| Stage | Mean ms | Share | p50 ms | p99 ms |
|---|---|---|---|---|
| Socket to sidecar callback (Socket.IO, MessagePack) | 0.309 | 20.9% | 0.284 | 0.648 |
| Sidecar handler | 0.043 | 2.9% | 0.039 | 0.112 |
| Publish to Python (Redis Pub/Sub) | 0.297 | 20.1% | 0.203 | 0.551 |
| Parse the envelope | 0.056 | 3.8% | 0.050 | 0.132 |
| Parse to enqueue | 0.013 | 0.9% | 0.011 | 0.041 |
| Tick queue | 0.060 | 4.0% | 0.027 | 0.110 |
| Consumer routing (dedup, batch record) | 0.023 | 1.6% | 0.020 | 0.089 |
| Window write (ZADD, ZCARD, ZREMRANGEBYRANK) | 0.356 | 24.2% | 0.322 | 0.773 |
| Window read and score (ZRANGE, HGET baseline) | 0.310 | 21.1% | 0.267 | 0.657 |
| Z score | 0.008 | 0.6% | 0.008 | 0.020 |
| Trigger and handoff to the DRE | 0.004 | 0.2% | 0.002 | 0.071 |
| End to end | 1.474 | 100% | 1.290 | 2.570 |

The 372 approved listings spent another 0.02 ms waiting for the anomaly pool, 0.08 ms in the DRE and 0.07 ms
until the order was placed.

### Bursts and large events

The consumer decides on one listing at a time, and each takes about 0.75 ms, mostly the Redis round trips, which
it awaits one after another. Listings that arrive together therefore queue behind each other. In `large`, where
100 listings arrive in one event, the median listing waits 29 ms in the tick queue and the slowest about 80 ms,
which is where the 39 ms median and 84 ms p99 come from. In `burst` the queue accounts for 74% of the mean. The
work per listing does not change with the burst; only the waiting does.

### Throughput and the profiles

At saturation the listener decides on about 1,500 listings a second with its event loop at 95% of a core, so
Python is the limit there. The cProfile of that run
([`ingress_latency/saturation_python_profile.txt`](ingress_latency/saturation_python_profile.txt)) puts 7.9 of
its 13.4 seconds in the Redis client (building commands, writing them to the socket and parsing the replies), 0.3
seconds in decoding JSON and very little in the Z score itself. The Node.js profile of the same run shows the
sidecar idle 91% of the time, with MessagePack decoding at 1.4%. The half second listings spend before the
sidecar callback at saturation is the stand in feed server's own send backlog, not Node.js. Under the profilers
everything runs about twice as slowly, so the profiled runs are only used to attribute time
([`ingress_latency/profile.json`](ingress_latency/profile.json)).

### Overload

When the listener falls far enough behind, Redis closes its Pub/Sub subscription: the output buffer for a
subscriber is limited to 32 MB, about 13,000 live sized sales. The listener subscribes again after ten seconds,
and everything published in between is lost without an error. In `overload` that lost 56% of the 20,000
listings in every run. The saturation runs, with 10,000 listings sent back to back, lost none. The busiest ten
seconds Skinport has sent us held 294 sales, so this needs about 45 times that backlog, but it is a silent loss and
the listener should notice it. A Python only ingress would not go through Pub/Sub at all, and part 2 removed it.

### One CPU

Limited to one CPU ([`ingress_latency/cpus1.json`](ingress_latency/cpus1.json)), as a stand in for a smaller edge
host, single listings take 1.60 ms at the median and 2.55 ms at p99, large events 39.3 and 94.8 ms, and the
listener sustains 1,104 decisions a second. Below saturation the limit makes almost no difference, because the
listener uses a tenth of a core.

### What the probes cost

The runs with only the end to end probes and the runs with every probe agree within the run to run spread: single
1.30 against 1.29 ms at the median, burst 8.84 against 9.17 ms, large 39.4 against 38.5 ms. The stage probes add
no cost I can measure.

## Budget

Traffic, from every Skinport feed event recorded between 2026-09-24 and 2026-10-22 (107 hours with the listener
running, since my PC is often off): 28,150 events with 138,587 sales, about 0.07 events and 0.36 sales a second on
average. At most 108 listings arrived in one second, the largest event held 108 sales and the busiest ten seconds
held 294.

What a fast decision is worth: across the 12,324 listings I recorded between 2026-09-24 and 2026-10-22 that later
sold, the fastest sold 9.1 seconds after the listing appeared on the feed, 0.1% sold within 11 seconds and 1%
within 14.5 seconds. I cannot see when a buyer clicked, only when Skinport sent the sold event, so a buyer may be
faster than this. Even so, a race for a listing is decided by the purchase request and Skinport's checkout, which
take far longer than the decision and are not changed by how the listener is built.

| Budget | Target | Measured | Why |
|---|---|---|---|
| Single listing, ingress to decision | p95 under 5 ms, p99 under 10 ms | 1.96 ms, 2.65 ms | well under a thousandth of the fastest sale |
| Every listing of the largest event | p99 under 250 ms | 84 ms (100 listings) | the largest event had 108 listings |
| Sustained throughput | at least 1,080 a second | 1,507 a second (1,104 on one CPU) | ten times the busiest second |
| Dropped listings | none below ten times the busiest ten seconds | none at 10,000 back to back | a listing missed is a trade missed |
| Listener CPU at live rates | under a quarter of a core | a tenth of a core at 100 a second | the edge PC runs other services |
| Listener memory | under 512 MB | 240 MB, 360 MB under overload | the container limit is 1 GB |

The sidecar path met every target, and the Python ingress does too (part 2). The p99 of a single listing is a quarter of its target, throughput is 14
times the busiest second (10 times on one CPU), and even a p99 decision takes less than a thousandth of the time
the fastest listing took to sell.

If more throughput is ever needed, the profile points at the Redis round trips, not the language: sending the
five window commands in one pipeline would remove most of the 0.67 ms per listing they cost now.

## Part 2: the Python ingress

The listener now speaks Engine.IO v4 over an aiohttp websocket and decodes the MessagePack Socket.IO packets itself,
the way the Waxpeer listener already did. Every `saleFeed` payload goes straight to the parser and the tick queue;
nothing goes through another process or Redis before the consumer. The benchmark measures this path with the same
fixture, scenarios and runs as part 1. The raw summaries are in
[`ingress_latency/python_reference.json`](ingress_latency/python_reference.json).

| Scenario | Listings | Dropped | p50 ms | p95 ms | p99 ms | Max ms | Decided/s | Listener CPU | Redis CPU | Peak RSS listener MB |
|---|---|---|---|---|---|---|---|---|---|---|
| single | 30,000 | 0 | 1.21 | 1.69 | 2.08 | 82.7 | 100 | 12% | 2% | 209 |
| burst | 30,000 | 0 | 8.59 | 49.1 | 100 | 308 | 90 | 10% | 2% | 227 |
| large | 30,000 | 0 | 45.7 | 120 | 153 | 212 | 101 | 10% | 2% | 195 |
| saturation | 30,000 | 0 | 3,484 | 4,734 | 4,850 | 4,919 | 1,479 | 94% | 14% | 292 |
| overload | 60,000 | 0 | 4,521 | 5,160 | 5,471 | 5,617 | 1,408 | 94% | 14% | 354 |

The PC was in use during these runs, and it shows: one `single` run with every probe stalled for 1.3 seconds while
the whole machine's CPU went to 49% (its p99 was 1.4 seconds; the other five runs stayed under 2.9 ms), and two of
the `large` runs had p99s of 256 and 274 ms where the third had 77 ms. In those runs the extra time is all tick
queue wait behind slower Redis round trips, which this change does not touch. To compare the two paths fairly I ran
them back to back instead, three rounds of `single` and `large` with the sidecar image (main at the time) and then
the Python one, so any background load falls on both
([`ingress_latency/comparison.json`](ingress_latency/comparison.json)):

| Round | single p50, sidecar / Python | single p99 | large p50 | large p99 |
|---|---|---|---|---|
| 1 | 1.51 / 1.09 ms | 2.31 / 1.84 ms | 36.8 / 31.2 ms | 73.6 / 67.0 ms |
| 2 | 1.57 / 1.14 ms | 2.33 / 1.86 ms | 36.6 / 31.3 ms | 74.2 / 72.9 ms |
| 3 | 1.64 / 1.18 ms | 2.54 / 1.92 ms | 38.8 / 33.2 ms | 78.6 / 77.6 ms |

The Python ingress was faster in every round: about 0.45 ms off a single listing at the median and at p99, and 5 ms
off the median listing of a large event.

### Where a single listing's time goes now

From the two `single` runs with every probe that the busy PC left alone:

| Stage | Mean ms | Share | p50 ms | p99 ms |
|---|---|---|---|---|
| Socket to parser (websocket read, Engine.IO, MessagePack) | 0.398 | 24.1% | 0.226 | 0.487 |
| Parse the payload | 0.047 | 2.8% | 0.044 | 0.113 |
| Parse to enqueue (includes the per event log line) | 0.113 | 6.8% | 0.107 | 0.222 |
| Tick queue | 0.146 | 8.8% | 0.047 | 0.137 |
| Consumer routing | 0.034 | 2.1% | 0.032 | 0.084 |
| Window write | 0.519 | 31.3% | 0.480 | 1.051 |
| Window read and score | 0.390 | 23.5% | 0.350 | 0.778 |
| Z score | 0.011 | 0.7% | 0.010 | 0.028 |
| Trigger and handoff to the DRE | 0.005 | 0.3% | 0.003 | 0.086 |
| End to end | 1.656 | 100% | 1.323 | 2.690 |

The hop from the socket to the parser now takes 0.23 ms at the median. Through the sidecar it took 0.52 ms: 0.28 ms
to the sidecar's handler, 0.04 ms in it and 0.20 ms through Pub/Sub. The Redis round trips of the price window are
now the larger part of the time, as they were before the hop was counted.

### Overload and backpressure

Nothing is lost when the listener falls behind. The tick queue holds 10,000 items; when it is full, the listener
stops reading the socket until the consumer catches up, and the feed waits in the socket's buffers. In `overload`
all 20,000 listings were decided in every run, at 1,408 a second. On the sidecar path Redis closed the subscription
instead and 56% were lost (#326).

The benchmark's feed server waits for the socket too, so under overload a listing's emit time is when the server
could write it, and the latencies of `saturation` and `overload` are a little shorter than a feed that never waits
would see. Whether Skinport's server waits for a slow reader or drops it, I cannot test without falling behind on
the live feed; at 1,400 decisions a second against a busiest second of 108, the listener is far from it. If the
server does drop the connection, the listener now reconnects after a second, logs it and counts it.

### Throughput, CPU and memory

At saturation the listener decides on 1,479 listings a second (1,507 on the sidecar path), with its event loop at
94% of a core. The cProfile of that run
([`ingress_latency/python_saturation_profile.txt`](ingress_latency/python_saturation_profile.txt)) puts 0.33 of its
12.9 seconds in decoding the MessagePack frames, about what decoding the sidecar's JSON envelope cost before, and
most of the rest in the Redis client as in part 1. What is gone is the sidecar's own cost: at saturation it used 20%
of a core and 125 MB of memory. The listener's own memory is unchanged (209 MB at live rates, 354 MB under
overload).

Limited to one CPU ([`ingress_latency/python_cpus1.json`](ingress_latency/python_cpus1.json)), single listings take
1.20 ms at the median and 1.96 ms at p99, large events 33.0 and 80.0 ms, and the listener sustains 1,423 decisions a
second, more than the sidecar path managed on one CPU (1,104), since the listener no longer shares it with Node.js.

## The path I measure

```
feed server (stand-in for Skinport)
  -> websocket, Engine.IO v4, Socket.IO packets in MessagePack
  -> listener: scrapers/skinport_socket.py decodes the packets, SkinportScraper.listen_websocket_stream parses
     each saleFeed payload into a FeedEvent and MarketTicks
  -> tick queue -> main.tick_consumer: dedup, batch record, price window write (ZADD, ZCARD, ZREMRANGEBYRANK)
  -> score_window (ZRANGE, HGET baseline, Z score) -> should_trigger_anomaly
  -> anomaly pool -> DRE (dre_approval_reason) -> executor
```

In part 1 a Node.js sidecar sat between the socket and the listener: socket.io-client decoded the packets, the
`saleFeed` handler built a JSON envelope and published it on Redis Pub/Sub (`skinport:sale_feed`), and the listener
subscribed to it.

Everything after the feed server is the code that runs live. `latency_bench.run` starts it with the listener's own
functions (`main.websocket_subscriber_producer`, `main.tick_consumer`), on a Redis of its own with the same image and settings as the edge Redis. Two things are stood in for, because they are network
calls to my server and not part of the decision: a full batch is acknowledged instead of being sent to the
backend, and the executor notes the order instead of posting the paper trade. The executor still runs the real
`ExecutionService.execute`, so the once only purchase rule is in the path.

The listener logs a line for every feed event, and that write happens on the event loop. Live, it goes to a pipe
that Docker reads. In the benchmark it goes to a pipe too, and a separate process copies it to the run's log file.
My first runs wrote the log file directly, into a folder that Docker Desktop mounts from Windows, and every write
could hold the event loop for hundreds of milliseconds. A single listing then took 150 to 270 ms at the median
instead of under 2 ms, so none of those runs are used here.

Stage boundaries, per listing, all on one clock:

| Boundary | Where it is taken |
|---|---|
| emitted | feed server, just before the frame is written to the socket |
| parse start and end | wrapper around `parse_sale_feed_event` |
| enqueued, dequeued | the tick queue, a subclass of `asyncio.Queue` |
| window, score, Z score | wrappers around `push_to_window`, `score_window` and `calculate_z_score` |
| decided | `update_window_and_detect` returned without an anomaly, or the DRE returned |
| order | `place_order` of the benchmark executor, for approved listings |

The wrappers are installed from outside (`latency_bench.probes`), so no listener code changed for this.
`SKINPORT_WS_URL` points the listener at the stand-in; Skinport stays the default. In part 1 the sidecar also
wrote opt in timings (entering its `saleFeed` handler, before and after `PUBLISH`), which is where its stages come
from.

## Clocks

The feed server and the listener are separate processes (in part 1 the sidecar was a third), so I never subtract
one process's relative clock from another's. Python's `time.perf_counter_ns()` and Node's `process.hrtime.bigint()`
both read the system monotonic clock (`CLOCK_MONOTONIC` on Linux, `QueryPerformanceCounter` on Windows), so
readings from every process are on one timeline. Each run checks that:

- 50 clock probes ask the feed server for its clock between two of mine. Every reading has to fall inside my
  bracket. The narrowest bracket bounds how far apart two readings can be.
- For every listing the boundaries have to be in order across processes (emitted, the sidecar's callback and
  publish in part 1, parse start, decided). The run counts any that are not.

Across the 35 reference and one CPU runs of part 1, no clock probe fell outside its bracket, the narrowest bracket
per run was between 0.063 and 0.126 ms, and not one listing had its boundaries out of order. The 35 of part 2 are
the same: no reading outside its bracket, the narrowest bracket per run between 0.068 and 0.188 ms, no listing out
of order. Comparing a reading in one
process with a reading in another is good to about a tenth of a millisecond, well below every stage I report
across processes.

## Scenarios

The fixture (`apps/listener/latency_bench/fixtures/skinport_v1`) holds 300 listed and 100 sold sales recorded on
2026-10-12 and 2026-10-13, one listed sale per item, with the baselines of those items from build 8, and the
number of sales in every listed and sold event of those two days (5,044 listed events with 23,838 sales, 3,980
sold events with 16,031 sales). The replay
fixtures keep only the fields the listener reads, about a tenth of a live sale, and parsing cost grows with the
payload. So this fixture keeps every field and replaces only the values that point at a person or one asset
(seller Steam ID, inspect link, asset and class IDs, screenshots, name tag) with values of the same length and
type. A fixture sale averages 2.5 KB of JSON, as live ones do.

Each scenario is built from the fixture with a fixed seed (175). Every listing gets its own productId, so none is
a duplicate. Prices are drawn around the item's latest sale price with a 3% spread, and 1% of listings are priced
at half of it, below the support floor, so the DRE approves them and the order path is measured too. Before the
measured events every item gets 6 normally priced listings, so every price window holds enough prices for the
window Z score. The run waits until the listener has decided on all of the warm up before it starts measuring.

| Scenario | Measured listings | Shape |
|---|---|---|
| single | 10,000 | one listing per event, 100 events a second |
| burst | 10,000 | event sizes drawn from the recorded listed events, sold events mixed in at the recorded ratio, bursts of 10 events 10 ms apart every 500 ms |
| large | 10,000 | 100 listings per event (the largest recorded event had 108), one event a second |
| saturation | 10,000 | recorded event sizes and sold events, sent back to back, to find the sustained throughput |
| overload | 20,000 | as saturation with twice the listings, to see what happens when the listener falls far behind |

Every scenario runs three times, each in fresh processes on an emptied Redis, and once more with only the end to
end probes, to bound what the stage probes cost.

## Running it

```bash
make latency-bench
```

That builds the listener image with its dev dependencies and runs `python -m latency_bench run` in its own
Compose project (`deployments/latency-bench`) next to its own Redis, so it never touches the edge Redis or the
backend. Results go to `data/latency_bench/reference`, and the `results.json` files of the runs in this report are
copied to `docs/benchmarks/ingress_latency/`. `LATENCY_BENCH_CPUS=1 LATENCY_BENCH_LABEL=cpus1 make
latency-bench` limits the container to one CPU. The tables in this file come from `python -m latency_bench report
<results.json>`.

The run refuses a Redis that holds any keys, because it publishes on the live sale feed channel and empties the
database when it is done. Outside Docker it needs an empty Redis of its own, for example
`docker run --rm -p 127.0.0.1:6390:6379 redis:8.10.2-alpine` and `--redis-url redis://127.0.0.1:6390`. Run from
Git Bash, set `MSYS_NO_PATHCONV=1` before passing container paths such as `/results`.

REST polling is not measured. It does not go through the websocket, and a poll arrives every five minutes.
