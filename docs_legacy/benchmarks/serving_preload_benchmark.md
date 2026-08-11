# Serving preload benchmark — moving the request path off BigQuery

**Claim (blog/resume-ready):** materializing the serving lookup layer as three
small dbt tables and reading it once at startup took a single-flight prediction
from **6 BigQuery queries and 2.31 GB scanned to zero**, and from a **5.1-second
median to 13 ms — a 390× latency reduction**. At on-demand pricing that is
**$0.01315 → $0 per prediction**, or **$1,315 → $0 per 100,000 predictions**.
A 1,500-flight airport batch went from 11.2 s to 194 ms (58×). Same model, same
artifacts, same features — the only difference is where the lookups live.

See also [`README.md`](README.md) — the `fact_flights` partition/cluster
benchmark, which is about scan volume for the analytics layer. This one is about
the inference path.

## Before / after (executed-job statistics, cache off)

Per `/predict` call, median of 3 runs:

| | Before (query per request) | After (preload) | Improvement |
|---|---|---|---|
| BigQuery queries | 6 | **0** | — |
| Bytes processed | 2,311,725,056 (2.31 GB) | **0** | — |
| Bytes billed | 2,314,207,232 (2.31 GB) | **0** | — |
| Cost per prediction ($6.25/TiB) | $0.01315 | **$0** | — |
| Cost per 100,000 predictions | $1,315 | **$0** | −$1,315 |
| Median latency, 1 flight | 5,078 ms | **13 ms** | **390×** |
| Median latency, 10 flights | 7,278 ms | 14 ms | 520× |
| Median latency, 100 flights | 10,700 ms | 25 ms | 428× |
| Median latency, 1,500 flights | 11,207 ms | 194 ms | **58×** |

Process startup, median of 3 runs:

| | Before | After | Improvement |
|---|---|---|---|
| BigQuery queries | 5 | 4 | — |
| Bytes billed | 3,028,287,488 (3.03 GB) | 41,943,040 (42 MB) | **72×** |
| Cost per cold start | $0.01721 | $0.00024 | 72× |
| Wall clock | 7,871 ms | 7,675 ms | ~unchanged |

**The startup wall clock barely moves, and that is worth saying plainly.** It is
dominated by loading ~730 MB of model artifacts, not by the queries. The 42 MB
billed is BigQuery's 10 MB per-table minimum × 4 tables — the tables themselves
are a few MB. What the preload buys at startup is cost, not speed.

## Why the before-numbers looked the way they did

`assemble_features()` issued five lookups per call — four `hist_*` grains plus
route distance — and a sixth for the density estimate when a flight's
`(origin, hour, weekday)` had not been seen by that process. Every one of them
aggregated `ml_flight_features` (20,240,662 rows) with **no `flight_date`
predicate**, so partition pruning never applied and `route` is not a clustering
key. The cost was therefore flat in batch size: a 1-flight request and a
1,500-flight request scanned the same 2.31 GB, because both were fetching a few
thousand constants out of a 20M-row table.

Those constants change only when the mart is rebuilt.

## What replaced it

Three dbt models in `dbt/models/gold/ml/`, built once per `dbt build`:

| Model | Grain | Rows |
|---|---|---|
| `serving_entity_profile` | (entity_level, entity_key) | 8,316 |
| `serving_density_profile` | (origin, crs_dep_hour, day_of_week) | 34,979 |
| `serving_typical_rotation` | one row | 1 |

`ml/serving.py` reads all three at startup into plain dicts. The request path
does dict lookups. The category vocabulary — previously a four-way `GROUP BY`
union over the mart — falls out of `serving_entity_profile` for free, since the
training vocabulary at a level *is* the set of entity keys present at that level.

## Correctness: this had to change nothing, and mostly didn't

The hist values are read from the mart via `any_value(...)` **specifically** so
the smoothing formula lives in exactly one place (`int_historical_delay_rates`)
and serving reproduces training values byte-for-byte. Materializing the same
query preserves that; rewriting the arithmetic would have destroyed it.

Verified with the golden-vector harness committed as `ml/parity.py`: 184 requests
spanning both rotation paths, both density paths, and four deliberately-unknown
entities, scored before and after.

```
uv run --extra ml --extra serve --extra ingestion python -m ml.parity capture before.json
uv run --extra ml python -m ml.parity compare before.json after.json --expect-medians-change
```

- **28 of 28 requests that supplied both rotation context and density are
  bit-identical** — every one of the 51 features, the calibrated probability, and
  the expected delay minutes. Those requests touch neither the typical profile
  nor the density medians, so they are the clean test of the refactor.
- The remaining 156 requests depend on values that changed deliberately (below).
  151 of them were unchanged anyway; 5 moved, and the **only** features that
  differed across the whole run were the four the change touches:
  `inbound_distance`, `sched_turnaround_min`, `sched_turnaround_slack_min`,
  `origin_dep_density_hour`. Mean |Δp| 0.00049, max 0.0668.

The harness enforces both halves rather than printing them: it exits non-zero on
any difference by default, and under `--expect-medians-change` it still fails if
a request that supplied its own context moved, or if a moved feature is one the
medians cannot reach (a `hist_route_*` value, say). Verified against doctored
fixtures in all three directions.

*Harness note:* the first version of this harness sampled its flights with
`any_value()` per column over a `(carrier, origin, dest, dep_time)` group. That
is the same defect described below — a group spans many dates, so each column
could come from a different flight, and the picks could differ between two
captures of identical code, producing false regressions. It now takes every
field from one deterministically-chosen row (`row_number()` over a total order).
The numbers above are from the corrected harness; two captures of identical code
are bit-identical.

## The bug this surfaced: the old typical profile was not deterministic

The medians behind the "typical rotation profile" — used by **every** prediction
made without rotation context, which is every consumer request — were computed
with `approx_quantiles(x, 2)[offset(1)]`. That is an approximation whose result
depends on how BigQuery shards the scan, and it ran **at process startup**.

The same query on identical data was observed returning four different values for
`inbound_distance`: **666, 674, 663, 651**. The exact median is **667**.
`sched_turnaround_min` came back 63 against an exact 64.

So two processes serving the same request could return different probabilities.
The largest divergence measured in the golden set was a flight moving from
**0.4300 to 0.4968** — a 6.7-point swing attributable entirely to approximation
noise in a lookup.

The three models use exact `percentile_disc` instead, and two consecutive full
rebuilds were verified to produce byte-identical tables. A related fix:
`min(distance)` replaces `any_value(distance)` for route distance, because 85 of
7,539 routes carry two distinct distances (a 1-mile rounding split in the BTS
source) and `any_value` picked arbitrarily per call.

This is a behaviour change, deliberately made, and it is why the parity result
above is reported in two parts rather than as a single "bit-identical" claim.

## Method

- **Executed-job statistics only** — `job.total_bytes_processed` /
  `total_bytes_billed` read off each completed `QueryJob`. No dry-run estimates.
- **Cache off and asserted**: every job is issued with
  `use_query_cache=False`, and the harness asserts `job.cache_hit` is false. This
  mattered — an earlier run of the baseline reported 0 bytes on repeat
  measurements because the key sets saturated and BigQuery served identical SQL
  from its 24-hour result cache, which would have flattered the baseline by
  ~2.3 GB per call.
- **Novel-request cost**: the pre-change in-process density cache is cleared
  before each repetition, so every measurement is a flight the process has not
  seen — the consumer case. A repeat request under the old code saved exactly one
  of the six queries.
- **Isolation**: all flights depart one origin (so the NWS grid is fetched once
  and HTTP time never lands inside a timed call) and the flight date is ~60 days
  out, beyond the NDFD horizon, so every row takes the weather NULL path
  deterministically. Weather code is unchanged by this work.
- 3 repetitions per cell at N = 1, 10, 100, 1500; medians reported. Run
  2026-08-09 against `ml_flight_features` at 20,240,662 rows.
- Both variants ran from the same commit with only `ml/serving.py` swapped, so
  the artifacts, the feature registry and the models are identical.

## Honest caveats

- **The dollar figures are per-prediction BigQuery cost, not total cost of
  ownership.** The dbt models cost one build per mart rebuild (~37 s, a few cents
  of scan) and the predictor still pays a ~7.7 s cold start dominated by artifact
  loading.
- **$0 per prediction is exact, not rounded.** The request path issues no query
  at all, which is why the improvement is reported as a query count going to zero
  rather than as a ratio.
- The latency figures include model inference. At N=1,500 the 194 ms is almost
  entirely XGBoost plus response assembly; there is no BigQuery in it.
