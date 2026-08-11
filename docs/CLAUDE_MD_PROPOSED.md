# CLAUDE.md: Flight-Delay Stream (PROPOSED REPLACEMENT, draft for review)

This is the draft replacement for the repo's live `CLAUDE.md`. It does not take effect until Seb and Aidan review it and copy it over. The live file currently carries a section 0 status header pointing here; sections 1-11 of the live file describe the 681 batch lakehouse and are reference only.

Two blocks below are carried verbatim from the live file's section 9, as required: the leakage rule including the linkage clause, and the MLflow adoption rule. They are quoted exactly, dangling references included; the adaptation notes that follow each block fix the navigation without editing the quoted text.

---

## 1. Purpose

An MSDS 682 realtime streaming inference project. A seeded replay producer streams one held-out week of US domestic flights into Kafka; a consumer scores every departure at scheduled gate time with the frozen 681 delay classifier, maintains per-tail rotation state under the schedule-consistency rule, emits a scored risk topic and an alert artifact, and an evaluator joins late-arriving outcomes to report alert precision and recall. Two findings organize all scope: the leakage boundary enforced in-stream, and the measured cost of forecast-for-observation weather substitution.

## 2. Binding architectural decisions

- **Local-first.** Docker Compose with `confluentinc/cp-kafka:8.3.1` and `confluentinc/cp-schema-registry:8.3.1` (KRaft; arm64-native) is the primary deliverable and the demo. `docker compose up` plus one make target must produce flowing events, a populated risk topic, an alert file, and an evaluation report, deterministically, on a clean machine, with a README of at most 10 steps. Confluent Cloud is a one-time documented deployment for screenshots, never a dependency.
- **Contracts are Avro at the Schema Registry, BACKWARD compatibility, and they are the ONLY contract definitions.** No application-level schema mirror. Contract shapes and the `knowable_at` discipline are specified in `docs/schemas.md`.
- **Topics are keyed by tail number** (sentinel `"NOTAIL"` for null tails, routed to a dedicated partition and scored with swap-shaped NULL rotation semantics). Per-tail ordering is what makes in-stream rotation state possible.
- **Every contract field carries `knowable_at`** in {schedule, pre_departure_stream, post_departure}. No `post_departure` field may reach the scorer; a contract test enforces this against the registry.
- **The shared constants module (`streaming/constants.py`) is the single source** for every leakage constant: duty window [0, 840] minutes, 35-minute turnaround, band edges 35/60/120, position cap 6, swap-class triggers, 3-hour weather staleness, the `knowable_at` field sets, and the forbidden post-departure columns. Batch tests and consumer assertions both import it. Restating a value from it anywhere is a defect.
- **The 2024-06-30 model is the shipped artifact and stays fixed.** It is the baseline every measurement compares against. Model changes are governed by the adoption rule quoted in section 4 and by `docs/leakage_discipline.md` rule 7 (the one-time held-out confirmation for CatBoost is already spent: not adopted).
- **Determinism.** Seeded replay; two identical runs produce byte-identical event sequences, alerts, and evaluation reports. No wall-clock time in any scored field; event time only.
- **Measurement norm.** Every performance or cost number this project reports follows the 681 benchmarks methodology: executed statistics, caches off, median of repeated runs (`docs_legacy/benchmarks/`).
- **No dbt, Dagster, BigQuery, or GCS in the runtime path.** BigQuery appears exactly once more, in the one-time export scripts, and dies with them.

## 3. Leakage rule (carried verbatim from the live CLAUDE.md section 9)

> - **Leakage rule (critical):** predictors may use **only information knowable
>   before departure**. Anything realized at/after departure or arrival
>   (`DepDelay`, `ArrDelay`, `ArrDelayMinutes`, actual gate/wheels times,
>   diverted/cancelled outcomes, `ArrDel15` for the classifier's features, etc.)
>   is a **label or forbidden feature**, never an input. Weather features must use
>   forecast-available / historical data, not same-flight realized conditions.
>   When adding a feature, explicitly justify it is pre-departure-known.
>   The rule extends to LINKAGE, not just values (decided 2026-07, the
>   tail-swap experiment): rotation features chain legs by the post-hoc
>   OPERATED tail, and a swap-restructured linkage is itself a day-of outcome —
>   so rotation features exist **only for schedule-consistent links**
>   (swap-shaped: NULL). The experiment: 89% of the cascade uplift survived
>   the restriction; the mechanism (no_inbound band rate 0.388→0.224 clean)
>   and the full three-way comparison live in `int_aircraft_rotation`'s
>   header and PR #18. Current held-out headline: **ROC 0.7389 /
>   PR-AUC 0.4652** (restricted; details in `ml/README.md`).

Adaptation notes (not part of the quoted rule): "PR #18" resolves against the original lakehouse repo; the recovered experiment write-up lives at `docs_legacy/tail_swap_experiment.md`. In this project the rule's serve-time enforcement is the consumer: swap-shaped links yield NULL cascade features in-stream, proven by the leakage test suite and the full-week rotation parity check. The weather sentence's "forecast-available" requirement is exactly what the TAF study measures; training used the last observation at or before scheduled departure, a recorded deviation this project quantifies.

## 4. Model adoption and MLflow (carried verbatim from the live CLAUDE.md section 9)

> - **Experiment tracking & model comparison.** MLflow (`ml/tracking.py`) tracks
>   every training run — **artifacts to `gs://$GCS_BUCKET/mlflow`, run metadata in
>   a local SQLite backend** (`mlflow.db`, git-ignored; a tracking server is the
>   upgrade path for cloud metadata). Tracking is a **pure side effect**: it never
>   changes fits, `metrics.json`, or determinism, and degrades to a warning if
>   MLflow/GCS is unreachable (a tracking outage must never fail a run).
>   Alternative learners are explored in `ml/experiments.py` on the **identical
>   split/features** — only the learner changes; the leakage boundary above is
>   fixed — and the shipped model changes only when an alternative **wins the
>   validation selection** against it, with the held-out test used as a one-time
>   confirmation report, never the adoption gate (adopting on a test comparison
>   re-selects on test; see `docs/leakage_discipline.md` rule 7).

Adaptation notes (not part of the quoted rule): the artifact store re-points from `gs://$GCS_BUCKET/mlflow` to a local path; the SQLite metadata backend and the pure-side-effect property are unchanged and binding. Evaluation runs (streaming eval, TAF horizon study, drift) are logged as tracked runs. `ml/experiments.py` is deleted with the model search concluded; the adoption rule itself outlives it and governs any future learner or feature change, including any retrain triggered by the TAF study.

## 5. Repo layout

```
streaming/      producer, consumer, rotation state, constants, evaluator, tests
data/           committed replay week, lookups, golden vectors (sanctioned git data)
ml/             kept modules: features, serving core, calibration, parity, tracking
docs/           live project documentation (this rulebook, plan, schemas, sources)
docs_legacy/    inherited 681 material: reference and provenance only, never policy
```

- `docs/` holds live policy; `docs_legacy/` holds inherited 681 material, reference only, never instructions. No third documentation location. `docs/leakage_discipline.md` is live policy (rules 7 and 12 govern current work).
- Committed data is the sanctioned exception to the old "data never lives in git" rule, limited to: the replay week, the outcome sample, the serving lookups, golden vectors, and the seeds. The model artifact ships as a release asset if it exceeds comfortable repo size.

## 6. Tooling and conventions

- Python via `uv`, one `pyproject.toml`, one lock. Extras: `kafka` (streaming stack), `ml` (inference). Never call pip.
- Config through env vars from `.env` (git-ignored; `.env.example` is the template and lists only Kafka and Schema Registry variables).
- Cost ceiling: under $5 incremental, target $0. Nothing a reviewer runs requires cloud credentials or spends money.
- Style for documentation: plain declarative prose, short paragraphs, no em-dashes, technical terms explained once at first use.
