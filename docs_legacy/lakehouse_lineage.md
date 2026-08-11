# One gold layer, two consumers — lineage and the boundary that makes it safe

**What this document proves.** The lakehouse serves an analytical consumer
(the Streamlit dashboard) and an ML consumer (the classifier + regressor) from
the **same curated gold layer, with no data or logic duplicated between them** —
and the correctness constraint that makes sharing safe, the pre-departure /
train-test boundary of CLAUDE.md §9, is enforced **in the pipeline** rather than
in either consumer.

Companion documents: [`leakage_discipline.md`](leakage_discipline.md) is the
authority on the boundary itself; [`benchmarks/`](benchmarks/) covers the
partition/cluster benchmark; [`../ml/README.md`](../ml/README.md) reports the
held-out metrics.

---

## Figure 1 — the shared spine and its two shapes

```mermaid
flowchart TD
    subgraph BRONZE["BRONZE — immutable raw"]
        B1["BTS on-time CSV in GCS<br/>external table"]
        B2["NOAA ISD hourly<br/>NDJSON access layer, external table"]
        B3["NOAA GSOD<br/>public dataset, read in place"]
        B4["seeds: airports + tz, holidays"]
    end

    subgraph SILVER["SILVER — cleaned, typed, conformed"]
        S1["silver_flights"]
        S2["silver_isd_hourly<br/>50.8M decoded observations"]
        S3["airport_station_map"]
    end

    subgraph GOLD["GOLD — ONE curated layer"]
        G0["stg_gold__flights<br/><b>single conformed flight spine</b>"]
        SH["int_historical_delay_rates<br/>int_aircraft_rotation<br/><b>SHARED — the only definition<br/>of a delay rate / a rotation chain</b>"]

        subgraph ANA["analytical shape"]
            A1["fact_flights<br/>dim_airport / dim_carrier / dim_date"]
            A2["mart_delays_by_airport / carrier / route<br/>mart_delays_by_schedule / monthly"]
            A3["dash_* views"]
        end

        subgraph MLS["ML shape"]
            M1["ml_flight_features<br/>wide, flat, 1 row per flight<br/>20,240,662 rows"]
        end
    end

    C1["Streamlit dashboard<br/><b>non-technical consumer</b>"]
    C2["ml/train.py + ml/api.py<br/><b>classifier + regressor</b>"]

    CUT{{"var train_test_cutoff_date = 2024-07-01<br/>dbt_project.yml — the ONLY date filter"}}
    GUARDS{{"3 standing dbt guards<br/>schema allowlist · weather obs-before-departure<br/>rotation schedule-only"}}

    B1 --> S1
    B4 --> S1
    B2 --> S2
    B3 --> S3
    B4 --> S3

    S1 --> G0
    G0 --> SH
    G0 --> A1
    A1 --> A2
    SH --> A2
    A2 --> A3
    A3 --> C1

    G0 --> M1
    SH --> M1
    S2 --> M1
    S3 --> M1
    M1 --> C2

    CUT -.gates.-> SH
    CUT -.sets is_training_row.-> M1
    GUARDS -.block the build.-> M1

    classDef bronze fill:#f3e2c7,stroke:#a16207,color:#111827
    classDef silver fill:#e4e7eb,stroke:#4b5563,color:#111827
    classDef gold fill:#f9e7a8,stroke:#a16207,color:#111827
    classDef shared fill:#cdeedd,stroke:#047857,stroke-width:3px,color:#111827
    classDef consumer fill:#d9e6fb,stroke:#1d4ed8,color:#111827
    classDef control fill:#fbdcdc,stroke:#b91c1c,color:#111827

    class B1,B2,B3,B4 bronze
    class S1,S2,S3 silver
    class G0,A1,A2,A3,M1 gold
    class SH shared
    class C1,C2 consumer
    class CUT,GUARDS control
```

**Read the figure at the green node.** Both branches pass through
`int_historical_delay_rates`. That is not a stylistic convergence — it is the
no-duplication guarantee expressed as graph topology, and it is the reason the
dashboard and the model can never disagree about "the delay rate for ORD."

**Read it again at the red nodes.** One dbt variable is the entire train/test
firewall for *both* consumers, and three build-blocking tests sit on the ML
mart. The boundary is a property of the warehouse, not a convention two Python
codebases are each trusted to honor.

## What "no duplication" means here, mechanically

Four separate mechanisms, none of them convention:

1. **Topology.** `int_historical_delay_rates` is `ref()`'d by
   `mart_delays_by_airport`, `mart_delays_by_carrier`, `mart_delays_by_route`
   **and** `ml_flight_features`. Its header states the rule: *"Both the analytics
   marts and the ML feature mart must ref() this model; nothing recomputes these
   rates."* `int_aircraft_rotation` is likewise defined once and read by the
   rates model and the ML mart. The two marts that carry no rates
   (`mart_delays_by_schedule`, `mart_delays_monthly`) store **additive counts and
   sums only**, so no rate is ever defined a second time to begin with.
2. **One cutoff.** `var('train_test_cutoff_date')` filters the shared rates model
   ([`int_historical_delay_rates.sql:59`](../dbt/models/gold/shared/int_historical_delay_rates.sql#L59))
   and derives the split column
   ([`ml_flight_features.sql:167`](../dbt/models/gold/ml/ml_flight_features.sql#L167)).
   `dbt_project.yml` states the invariant: *"Change it HERE only — no model may
   inline a cutoff literal."*
3. **One aggregation macro.** `delay_measures()` supplies the identical
   measure block to all three entity-grain marts, so even the arithmetic is
   written once.
4. **Serving reads the mart, not a reimplementation.** `ml/serving.py` pulls
   `hist_*` out of `ml_flight_features` with `ANY_VALUE` (verified
   constant-within-entity) rather than re-deriving the smoothing formula in
   Python, so inference reproduces training values byte-for-byte.

The claim is checked, not asserted: `dashboard/verify.py` recomputes every
dashboard rate directly in BigQuery as an independent `GROUP BY` oracle and
asserts the app's arithmetic matches — **9 checks, 0.0 difference**.

## Figure 2 — the full dbt DAG

Every model, as built. Figure 1 is this graph with the staging detail collapsed.

```mermaid
flowchart LR
    src_bts[("bronze.bts_on_time_performance<br/>external")]
    src_isd[("bronze.isd_hourly<br/>external")]
    src_gsod[("noaa_gsod.gsod2022-2024<br/>+ stations")]
    seed_air[/"seed: airports"/]
    seed_hol[/"seed: holidays"/]

    stg_bts["stg_bts_flights"]
    stg_air["stg_airports"]
    stg_hol["stg_holidays"]
    stg_wx["stg_weather"]
    stg_ws["stg_weather_stations"]
    asm["airport_station_map"]
    sflights["silver_flights"]
    sisd["silver_isd_hourly"]

    gflights["stg_gold__flights"]
    rot["int_aircraft_rotation"]
    hist["int_historical_delay_rates"]

    fact["fact_flights"]
    dair["dim_airport"]
    dcar["dim_carrier"]
    ddate["dim_date"]

    m_air["mart_delays_by_airport"]
    m_car["mart_delays_by_carrier"]
    m_rt["mart_delays_by_route"]
    m_sch["mart_delays_by_schedule"]
    m_mon["mart_delays_monthly"]

    d_air["dash_airport_reliability"]
    d_car["dash_carrier_reliability"]
    d_rt["dash_route_drilldown"]
    d_tm["dash_delays_by_time"]
    d_mon["dash_monthly_trend"]

    mlmart["ml_flight_features"]

    src_bts --> stg_bts --> sflights
    seed_air --> stg_air --> sflights
    seed_hol --> stg_hol
    src_gsod --> stg_wx --> asm
    src_gsod --> stg_ws --> asm
    stg_air --> asm
    src_isd --> sisd

    sflights --> gflights
    gflights --> rot --> hist
    gflights --> hist
    gflights --> fact
    gflights --> dcar
    gflights --> ddate
    stg_hol --> ddate
    stg_air --> dair
    asm --> dair

    fact --> m_air
    fact --> m_car
    fact --> m_rt
    fact --> m_sch
    fact --> m_mon
    dair --> m_air
    dcar --> m_car
    hist --> m_air
    hist --> m_car
    hist --> m_rt

    m_air --> d_air
    m_car --> d_car
    m_rt --> d_rt
    dair --> d_rt
    m_sch --> d_tm
    m_mon --> d_mon

    gflights --> mlmart
    rot --> mlmart
    hist --> mlmart
    sisd --> mlmart
    asm --> mlmart
    stg_hol --> mlmart

    classDef shared fill:#cdeedd,stroke:#047857,stroke-width:3px,color:#111827
    classDef mart fill:#f9e7a8,stroke:#a16207,color:#111827
    class rot,hist shared
    class mlmart,fact mart
```

Note what the ML branch does **not** do: it never reads `fact_flights` or any
`dim_*`. The two shapes are siblings descending from `stg_gold__flights`, which
is exactly CLAUDE.md §4 — *"do not make ML consumers join dimensions at train
time."* Sharing the layer is not the same as sharing the shape.

---

## Why the **gold** layer feeds the ML pipeline

The deep dive requires defending which layer feeds ML. The answer is gold, and
the reason is not that gold is "the cleanest" — it is that **the ML feature mart
is where a correctness constraint becomes a build artifact.**

### 1. The boundary has to be testable, and only gold can test it

The pre-departure rule and the train/test cutoff are correctness constraints
that decide whether *every* reported metric is honest. In gold they are SQL, so
they are checkable by the build:

- `assert_ml_features_no_leakage` diffs the built mart's columns against an
  audited allowlist — a renamed outcome column or a new un-audited feature
  fails **by default**.
- `assert_ml_weather_obs_before_departure` proves, value-level over the full
  table, that every weather observation sits at or before scheduled departure
  and inside the staleness ceiling.
- `assert_ml_rotation_schedule_only` independently rebuilds the entire rotation
  feature set from schedule columns and compares to the mart — a chain
  accidentally built on actual times diverges massively and fails.

Move the mart into Python and all three become unwritable. There is no artifact
to diff, no build to block, and the boundary degrades to a code-review promise.
`ml/audit.py` then re-asserts the same allowlist against `INFORMATION_SCHEMA` at
train time, so the contract is enforced on both sides of the handoff.

### 2. The shared-definition requirement forces it

The `hist_*` delay rates are needed by the BI marts *and* by the model. Exactly
one of two things can be true: either they are computed once in gold and both
consumers read them, or the model recomputes them in pandas. The second option
guarantees drift between the dashboard's ORD delay rate and the model's — and
guarantees it **silently**, because nothing compares the two. Feeding ML from
gold is what makes the single definition possible; feeding it from silver makes
duplication mandatory.

### 3. Silver has no train/test concept and none of the expensive joins

Silver is conformed but flat: it has no cutoff, no historical rates, no rotation
chain, no as-of weather. Everything the model needs beyond raw columns is
built in the gold layer, and built as set-based SQL — a 50.8M-row ISD decode and
an as-of join reduced to a `(station_id, obs_date)` equi-key so it hash-joins
about two days per flight instead of a station's full three-year history. That
is warehouse work by CLAUDE.md §5, and it is why the mart builds in ~14 s
instead of becoming a per-training-run Python job.

### 4. Bronze is not a candidate

Bronze is all-`STRING` external CSV, plus packed ISD fields with
per-station heterogeneous columns. Training off bronze means re-implementing
the entire decode on every run, in Python, with no test coverage and a 9.3 GB
re-scan each time.

### What this choice costs — stated, not hidden

A gold-mart-as-contract is not free, and the defense is stronger for naming the
bill:

- **Iteration is a rebuild.** A new feature is a dbt change plus a mart rebuild
  plus a retrain, not a notebook cell. That is the deliberate trade: slower
  edits in exchange for a boundary that cannot be bypassed by accident.
- **The shared rates aggregate the whole pre-cutoff window.** A validation slice
  carved from inside the training window therefore carries slightly leaky
  `hist_*` values. This is a direct consequence of requiring a *single*
  definition rather than per-flight as-of-date rates. It is measured, not
  assumed — the leak shifts XGBoost's validation PR-AUC by +0.00079 vs
  LightGBM's +0.00020, enough to distort a close ranking, and it did not flip
  the current selection. See [`leakage_discipline.md`](leakage_discipline.md)
  rule 10; re-derive fit-window rates for any closer or wider selection.

### The payoff, in the numbers that matter

The split is exact and time-ordered — 16,678,880 training rows
(2022-01-01 … 2024-06-30) and 3,561,782 held-out rows (2024-07-01 … 2024-12-31),
asserted disjoint at train time and never re-derived in Python — and the
held-out result is **ROC 0.7389 / PR-AUC 0.4652** for the classifier and
**RMSE 49.26 / MAE 18.99** for the regressor, reproducible byte-identically
across full mart rebuilds. Those numbers are trustworthy *because* of where the
mart lives, not in spite of it.
