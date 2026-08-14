> **This is the MSDS 682 streaming project.** A seeded replay producer streams a
> held-out week of flights into Kafka; a consumer scores every departure at
> scheduled gate time with the frozen 681 delay model and emits a risk topic and
> alerts. The demo is local and needs no cloud access: `docker compose up` plus
> one make target. Start with [docs/PLAN.md](docs/PLAN.md),
> [docs/schemas.md](docs/schemas.md), and
> [docs/HANDOFF_PROMPTS.md](docs/HANDOFF_PROMPTS.md); binding decisions are in
> [CLAUDE.md](CLAUDE.md), the live streaming rulebook. **Everything below this block describes the
> MSDS 681 batch platform this project was built on, kept as reference.** Its
> GCP setup instructions do not apply to reviewers.

## Quickstart (the whole reviewer path, 9 steps)

1. Prerequisites: Docker Desktop running, [uv](https://docs.astral.sh/uv/) installed, `git`.
2. `git clone https://github.com/sebaleks/flight-delay-stream && cd flight-delay-stream`
3. `uv sync --extra kafka --extra ml --extra serve --extra ingestion`
4. Model artifacts (the consumer scores with them): `bash scripts/fetch_artifacts.sh` downloads the frozen run into `ml/artifacts/` from the GitHub release (needs `gh` logged in; phase 1, ~290 MB, suffices).
5. `make demo` — brings up Kafka + Schema Registry (KRaft, Confluent 8.3.1), registers the Avro contracts, replays the committed week (2024-09-02 to 2024-09-08 plus a warm-up day, 151,878 departures and 151,878 outcomes at event time), scores every departure through the consumer, then prints the outcome-join evaluation.
6. Expected output: both producers report `produced 151878 events`; the consumer reports `scored 151878 events ... (alerts 7061 at p>=0.5)`; the evaluation prints the headline `precision 0.548908 recall 0.177616 (5950 alerts, 133627 scored)` above the nine join counters (warm-up day and cancellations excluded into their own counters, nothing silently dropped). Two runs produce byte-identical reports and alert files.
7. `make ui` — the terminal consumer over the risk topic: every scored departure ranked by probability, and a cascade view ranking which delays would propagate furthest through the aircraft's remaining legs that day. Filters pass through, e.g. `make ui ARGS="--origin ORD --min-risk 0.6"`, and `make ui ARGS="--follow"` tails the topic live.
8. `make test` — the streaming test suite (constants source-pinning, rotation, enrichment, leakage, pressure, cascade, cascade, evaluator; 59 tests) and lint.
9. Cleanup: `make down` (equivalent to `docker compose down -v`).

Without running anything: [data/reference_output/](data/reference_output/) holds the committed alert artifact and evaluation report from this exact commit, with a README explaining the fields and the join counters. Because replay is deterministic, `diff`-ing your run against those files is a regression test. Data provenance, ownership, rights and access are in [docs/data_sources.md](docs/data_sources.md); who built what is in [docs/CONTRIBUTIONS.md](docs/CONTRIBUTIONS.md).

---

# Flight-Delay Lakehouse

A GCP lakehouse for US domestic flight-delay analytics and ML. Raw data lands as
immutable **bronze CSV in GCS**; **silver/gold** are native **BigQuery** tables
built by **dbt Core**; **Dagster** orchestrates the end-to-end DAG; two models
predict delays using only pre-departure information.

> Architectural decisions are recorded in [CLAUDE.md](CLAUDE.md). Read it first.

---

## Architecture

```
                 ingestion/ (Python, ADC)
  BTS 2022-2024  ────►  Bronze: raw CSV in GCS        gs://$GCS_BUCKET/bronze/<source>/year=/month=
                          │  (immutable, partitioned by year/month)
                          │  exposed to BigQuery as external tables (bronze dataset)
  NOAA ISD hourly ─────► │  (station-year CSVs, year= partitions + NDJSON access layer
                          │   for the external table — ML weather at scheduled departure)
  NOAA GSOD  ───────────► │  (read in place from bigquery-public-data.noaa_gsod;
                          │   used for the airport→station map)
  airports+tz, holidays ► │  (dbt seeds: small static CSVs in git → bronze dataset)
                          ▼
              dbt Core (BigQuery SQL, ADC)
                          │
                    Silver dataset  (cleaned, typed, conformed)
                          ▼
                    Gold dataset
                     ├── star schema:  fact_flights, dim_airport, dim_carrier, dim_date
                     ├── ML feature mart:  wide, flat, one row per flight (pre-departure only)
                     └── BI marts + dash_* views  (pre-aggregated, <1 MB/query)
                          ▼                         ▼
   ml/ ── time split ──► classifier + regressor    dashboard/ (Streamlit) ──► non-technical consumer

        Dagster (orchestration/) drives:  ingest ──► dbt ──► ML   (added last)
```

**One gold layer, two consumers.** The analytical branch (`dash_*` → Streamlit)
and the ML branch (`ml_flight_features` → the two models) both descend from
`stg_gold__flights` and both read the shared `int_historical_delay_rates` — so
the dashboard and the model can never disagree on a delay rate. Rendered
lineage, the train/test boundary overlay, and the defense of *why gold feeds
ML*: [docs/lakehouse_lineage.md](docs/lakehouse_lineage.md).

**Why BigQuery + dbt and not a cluster.** 33.84 GiB of bronze, a 50.8M-row
hourly-weather decode, a 20.2M-row ML mart, refreshed **monthly** — a range
where distributed shuffle is overhead rather than leverage, and where a standing
cluster would idle ~99.9% of the time. Measured volumes, the alternatives
rejected, and the conditions under which this choice would be wrong:
[docs/compute_choice.md](docs/compute_choice.md).

**Current model (held-out Jul–Dec 2024): ROC 0.7389 / PR-AUC 0.4652** —
three controlled feature generations (daily → hourly-at-departure weather →
cascade/rotation), with rotation features **restricted to
schedule-consistent tail linkages** after the 2026-07 tail-swap leakage
experiment (89% of the cascade uplift survived; swap-shaped links are NULL —
mechanism and three-way comparison in `ml/README.md` and
`dbt/models/gold/shared/int_aircraft_rotation.sql`). Four standing dbt
guards (three pin the ML leakage boundary — schema allowlist, weather
obs-before-departure, rotation schedule-only — plus the airport-timezone
pin); metrics byte-reproducible across full mart rebuilds.

## Repository layout

```
flight-delay-lakehouse/
├── CLAUDE.md            # binding architecture decisions
├── README.md
├── pyproject.toml       # uv-managed; extras: ingestion / transform / orchestration / ml
├── .python-version      # 3.12
├── .env.example         # template for GCP project/bucket/datasets (copy to .env)
├── .gitignore           # excludes secrets, data, virtualenvs
├── ingestion/           # Python: extract sources -> bronze CSV in GCS
├── dbt/                 # dbt Core (BigQuery): bronze sources -> silver -> gold
│   ├── dbt_project.yml
│   ├── profiles.yml     # BigQuery, method: oauth (ADC), env-var driven
│   ├── packages.yml
│   ├── macros/          # generate_schema_name -> datasets named verbatim
│   ├── models/
│   │   ├── bronze/      # sources only (external tables + NOAA public data)
│   │   ├── silver/      # cleaned/conformed
│   │   └── gold/{star,ml}
│   └── seeds/           # small static reference CSVs
├── orchestration/       # Dagster code location (placeholder, added last)
├── ml/                  # Python: feature load, time-split, train/eval two models
└── dashboard/           # Streamlit app: serves the gold dash_* views (end product)
```

## Prerequisites

- [`uv`](https://docs.astral.sh/uv/) (Python is pinned in `.python-version`; uv
  will fetch it).
- [`gcloud` CLI](https://cloud.google.com/sdk/docs/install) authenticated to your
  GCP project.
- A GCP project with billing enabled (see **GCP setup** below).

## GCP setup (one-time)

See the checklist below. In short: create a **project**, a **GCS bucket** for
bronze, three **BigQuery datasets** (bronze/silver/gold), enable the required
**APIs**, and authenticate with **ADC**.

<!-- TODO: fill in exact commands / IaC once the project id is chosen. -->

## Local setup

```bash
# 1. Configuration
cp .env.example .env          # then edit values; .env is git-ignored

# 2. Authenticate (Application Default Credentials)
gcloud auth application-default login
gcloud config set project "$GCP_PROJECT_ID"

# 3. Python env (choose the extras you need)
uv sync --extra ingestion --extra transform --extra ml --extra orchestration

# 4. dbt (uses ./dbt/profiles.yml via DBT_PROFILES_DIR=./dbt)
uv run dbt deps --project-dir dbt
uv run dbt debug --project-dir dbt      # verifies BigQuery + ADC connectivity
```

## Configuration & credentials flow

- **Config** lives only in env vars. `.env` (git-ignored) holds real values;
  `.env.example` is the committed template. Python reads them via
  `python-dotenv`; dbt reads them via `env_var()` in `profiles.yml` /
  `dbt_project.yml`; Dagster resources read the same vars.
- **Credentials** use Application Default Credentials — no key files in the repo.
  Local: `gcloud auth application-default login`. CI: mount a service-account key
  and point `GOOGLE_APPLICATION_CREDENTIALS` at it (env only, never committed).
- **Datasets** bronze/silver/gold are set by `BQ_BRONZE_DATASET` /
  `BQ_SILVER_DATASET` / `BQ_GOLD_DATASET`; dbt maps each model's `+schema` to the
  dataset name verbatim (see `dbt/macros/generate_schema_name.sql`).

## Status / roadmap

- [x] Repo scaffold (uv, dbt, Dagster placeholder, ingestion/ml folders)
- [x] Ingestion: BTS → bronze CSV in GCS + external table; airports/holidays → dbt seeds
- [x] dbt: silver staging models
- [x] dbt: gold star schema (`fact_flights` + dims)
- [x] dbt: gold wide ML feature mart (pre-departure features only)
- [x] dbt: gold BI marts + dashboard views
- [x] ML: time-split, classifier (`ArrDel15`), regressor (`ArrDelayMinutes`)
- [x] Performance benchmark: `fact_flights` partition/cluster pruning (see `docs/benchmarks/`)
- [x] Dashboard: Streamlit app over the gold `dash_*` views (see `dashboard/`),
      live on Cloud Run at
      <https://flight-delay-dashboard-buboj66t4q-uc.a.run.app> (Cloud Build CD on
      push to `main`)
- [x] Dagster: ingest → dbt → ML wired, blocking asset checks, monthly schedule
- [x] CI: `pr-checks` (gitleaks, dbt parse, Dagster definitions validate, ruff)
- [x] Feature gen 2: hourly ISD weather at the scheduled departure hour
- [x] Feature gen 3: cascade/aircraft-rotation (tail-swap-restricted; see above)
- [x] Forecast inference endpoint (FastAPI + NWS/NDFD at the departure hour)
- [x] Hyperparameter tuning (Stage 3: regressor tuned; classifier kept defaults)
- [x] Probability calibration (Stage 4: Platt-calibrated classifier, AUC preserved)
- [ ] Blog writeup

The full pipeline runs end-to-end under Dagster. Open PRs at any time are
listed on GitHub; the model headline above is the tail-swap-restricted number.
