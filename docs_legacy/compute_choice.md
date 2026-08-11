# Why BigQuery + dbt, and not a cluster

**The decision (CLAUDE.md §5):** every silver→gold transform is BigQuery SQL
orchestrated by dbt Core. Python is confined to extract/load (`ingestion/`) and
model fitting (`ml/`). No Spark, no Dataproc, no Databricks, no pandas in the
transformation path.

**Stated honestly up front:** this was adopted as a binding architectural
decision at the start of the project, **not** chosen by running a bake-off
against Spark. What follows is the justification measured *against the workload
we actually built* — a defense of the decision, not a trade study we performed.
Where the choice would have been wrong is in the last section.

---

## The workload, measured

All figures below are measured from the live project on 2026-08-06 —
BigQuery `__TABLES__` metadata and `gsutil du` — not estimated.

| Layer | Object | Rows | Size |
|---|---|---:|---:|
| Bronze (GCS) | BTS on-time CSV | 20,656,085 | 8.69 GiB |
| Bronze (GCS) | NOAA ISD hourly, raw station-year CSV | — | 24.54 GiB |
| Bronze (GCS) | ISD NDJSON access layer | — | 619.4 MiB |
| Bronze (GCS) | **total** | | **33.84 GiB** |
| Silver (BQ) | `silver_isd_hourly` (decoded observations) | 50,762,385 | 3.80 GiB |
| Silver (BQ) | `silver_flights` | 20,656,085 | 11.17 GiB |
| Silver (BQ) | `stg_weather` (GSOD, read in place from public data) | 11,964,865 | 1.89 GiB |
| Gold (BQ) | `stg_gold__flights` — the conformed spine | 20,656,085 | 4.31 GiB |
| Gold (BQ) | `fact_flights` | 20,656,085 | 2.97 GiB |
| Gold (BQ) | `ml_flight_features` | 20,240,662 | 6.75 GiB |
| Gold (BQ) | `int_historical_delay_rates` (shared) | 7,772 | <1 MiB |
| Gold (BQ) | BI marts (`mart_delays_*`) | 17 – 7,559 | <1 MiB each |

**Shape of the work.** The heaviest single transform is `ml_flight_features`:
it joins the 20.7M-row flight spine against the 50.8M-row hourly observation
table as an as-of lookup, alongside a rotation self-join over the same spine.
Everything downstream of that collapses hard — the entire BI serving surface is
five tables totalling well under 1 MiB.

**Cadence.** One scheduled run per month (`monthly_refresh`, cron `0 6 10 * *`)
— BTS publishes 2–3 months in arrears, so there is nothing to ingest more
often. This is a batch pipeline with a long idle period, not a continuously
running one.

## Why this workload is warehouse-shaped, not cluster-shaped

**1. The volume is small for a cluster and large for a laptop — exactly the
range a serverless warehouse owns.** Tens of millions of rows and tens of GiB
sit far below where distributed shuffle earns its keep, and far above what you
want to pull into a single Python process. BigQuery covers this range with no
provisioning at all: the ML mart, the largest build in the project, completes in
seconds.

**2. Monthly cadence makes any standing cluster almost entirely idle.** A
Dataproc cluster sized for this job would spend ~99.9% of the month doing
nothing, or need ephemeral create/destroy orchestration whose startup latency
exceeds the query time it saves. BigQuery on-demand bills per byte scanned and
costs exactly zero between runs. The same reasoning drives the orchestration
deployment choice — Cloud Run Job + Scheduler over an always-on VM (see
[`../orchestration/README.md`](../orchestration/README.md)).

**3. The transforms are relational, so SQL is the native expression.** Casts,
dedupes, conformance joins, window functions for the rotation chain, group-bys
for the delay rates, an as-of join for weather. Rewriting these as PySpark
DataFrame code would add a translation layer and buy nothing — and it would
cost the thing that actually matters here: **dbt's test framework is where the
leakage boundary lives.** The three standing guards
(`assert_ml_features_no_leakage`, `assert_ml_weather_obs_before_departure`,
`assert_ml_rotation_schedule_only`) are SQL assertions against built tables,
surfaced as blocking Dagster asset checks. That safety net has no equivalent in
an ad-hoc Spark job. See [`lakehouse_lineage.md`](lakehouse_lineage.md).

**4. The one genuinely heavy step is already SQL and already fast.** Decoding
50.8M ISD observations from packed fields (`TMP "-0194,5"` → value, QC code) is
pure set-based string work, and the as-of weather join was reduced from a range
join to a `(station_id, obs_date)` equi-key via `generate_date_array`, so it
hash-joins ~2 days of observations per flight instead of scanning a station's
full three-year history. The expensive-looking step is the one SQL handles best.

**5. The cost lever turned out to be layout, not engine.** Partitioning
`fact_flights` by month and clustering by origin cut a representative dashboard
query from 554.0 MB to 16.4 MB scanned — **33.8×**, $315.31 → $9.54 per 100,000
queries ([`benchmarks/`](benchmarks/README.md)). No compute engine change
available to this project comes close to that, which is direct evidence that
engine choice was not the binding constraint on cost or latency.

## Why the ML step doesn't need a cluster either

The obvious argument for Spark is "20M rows won't fit in memory." It does.
[`ml/data.py`](../ml/data.py) reads the mart through the **BigQuery Storage
API** and downcasts to `float32` / `int8` / `category`, which brings the full
20,240,662-row training frame comfortably onto one machine. XGBoost then trains
on it directly. Spark MLlib would impose a distributed training path on a
problem that a single box handles, and the repo's determinism guarantee —
byte-identical `metrics.json` across full rebuilds — is materially easier to
hold on one machine than across a shuffle.

The serving path is smaller still: single-flight and batch scoring over a
FastAPI endpoint, reading `hist_*` straight from the mart.

## The dashboard: push the compute down, not out

The Streamlit app never scans `fact_flights`. Each `dash_*` view is a thin skin
over a materialized mart of at most 7,559 rows, so a full page load reads well
under 1 MB, and a cached client with a 1-hour TTL prevents re-billing identical
queries. Pre-aggregate once in dbt, serve thin — a scaling decision made in the
model layer rather than by adding compute.

The app is served from **Cloud Run** (`flight-delay-dashboard`, us-central1),
built by Cloud Build on every push to `main` (`Dockerfile`, `cloudbuild.yaml`)
and authenticating to BigQuery with the runtime service account's ADC — no key
file, per CLAUDE.md §2. Cloud Run scales to zero between visits, which makes the
serving tier the **third** component to take the same posture as the other two:

| Component | Runtime | Idle cost |
|---|---|---|
| Transforms (bronze→silver→gold) | BigQuery on-demand, per byte scanned | none |
| Orchestration (monthly refresh) | Cloud Run Job + Cloud Scheduler | none |
| Dashboard (serving) | Cloud Run service, scale-to-zero | none |

That consistency is the practical payoff of the compute decision: at this data
volume and cadence, **no part of the lakehouse needs a machine that is always
on.** A cluster-based transformation tier would have been the only component
breaking that property, and it would have dominated the bill.

The same instinct produced the one performance rationale that predates this
document, in [`../dbt/dbt_project.yml`](../dbt/dbt_project.yml): silver models
are materialized as **tables**, not views, because a silver view would re-scan
the bronze GCS CSVs on every downstream query.

## Alternatives, and why they were not adopted

| Option | Why not |
|---|---|
| **PySpark on Dataproc** | Distributed shuffle is overhead, not leverage, at 20–50M rows; a monthly job leaves the cluster idle or pays ephemeral startup on every run; and the transforms are relational, so PySpark would be SQL with extra steps — minus dbt's tests, lineage, and docs. |
| **Databricks** | Same shape as above, plus a second platform and billing surface for a project already resident in GCP with ADC everywhere. |
| **Python/pandas on Cloud Run** | Would violate CLAUDE.md §5 and, more concretely, put the leakage boundary in application code where no build-time test can check it. Also re-scans bronze per run instead of reading materialized tables. |
| **BigQuery + dbt (chosen)** | Zero idle cost, no provisioning, set-based SQL for set-based work, and — decisively — a testable transformation layer that lets the pre-departure boundary be enforced by the build. |

## Where this choice would be wrong

The honest limits. This architecture would be the wrong call if the project
needed:

- **Sub-minute or streaming latency.** BigQuery batch + a monthly Dagster job is
  built for freshness measured in days. Real-time operational delay prediction
  (see `blog_material.md` ch. 25 on the two-regime framing) would need a genuine
  streaming ingestion path.
- **Non-relational or unstructured transforms** — NLP over free text, image
  work, or anything needing arbitrary Python UDFs across the whole dataset.
  SQL stops being the natural expression and Spark starts earning its cost.
- **Training beyond single-box memory.** The in-memory argument above is
  measured at 20.2M rows with aggressive downcasting. An order of magnitude more
  data, or a much wider feature matrix, breaks it and forces distributed
  training or out-of-core batching.
- **Multi-TB shuffles or heavy iterative computation** — graph algorithms,
  large-scale simulation — where a warehouse's query model is a poor fit.

None of these describe a fixed 2022–2024 batch window of US domestic flights
with a monthly refresh, which is why the decision holds for this project.
