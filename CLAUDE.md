# CLAUDE.md — Flight-Delay Streaming (MSDS 682)

The binding rulebook for this repo. The 681 batch rulebook it replaces is archived unchanged at `docs_legacy/CLAUDE_MD_LEGACY.md`.

Two blocks below are carried verbatim from the 681 rulebook's section 9: the leakage rule including the linkage clause, and the MLflow adoption rule. They are quoted exactly, dangling references included; the adaptation notes that follow each block fix the navigation without editing the quoted text.

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
- **No dbt, Dagster, BigQuery, or GCS in the runtime path.** The plan (not yet executed): BigQuery appears exactly once more, in the one-time export scripts, and dies with them. Those scripts do not exist yet; see `docs/PLAN.md` Step 0.

## 3. Leakage rule (carried verbatim from the 681 rulebook's section 9, `docs_legacy/CLAUDE_MD_LEGACY.md`)

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

## 4. Model adoption and MLflow (carried verbatim from the 681 rulebook's section 9, `docs_legacy/CLAUDE_MD_LEGACY.md`)

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

This is the TARGET layout after the deletion audit in `docs/PLAN.md` executes. Today, `dbt/`, `orchestration/`, `dashboard/`, and most of `ingestion/` still exist pending their audit verdicts, and `streaming/` and `data/` do not exist yet.

```
streaming/      producer, consumer, rotation state, constants, evaluator, tests
data/           committed replay week, lookups, golden vectors (sanctioned git data)
ml/             kept modules: features, serving core, calibration, parity, tracking
docs/           live project documentation (this rulebook, plan, schemas, sources)
docs_legacy/    inherited 681 material: reference and provenance only, never policy
```

- `docs/` holds live policy; `docs_legacy/` holds inherited 681 material, reference only, never instructions. No third documentation location. `docs/leakage_discipline.md` is live policy (rules 7 and 12 govern current work).
- Committed data is the sanctioned exception to the old "data never lives in git" rule, limited to: the replay week, the outcome sample, the serving lookups, golden vectors, the seeds, and the reference output in `data/reference_output/` (the alert artifact, the evaluation report, and the figures rendered from them by `scripts/plot_evaluation.py`). The model artifact ships as a release asset if it exceeds comfortable repo size.
- The reference output pair was added to that list on 2026-08-13, deliberately: the final-package rubric requires a representative output artifact and a validation artifact to be present in the repository, and a reviewer should not have to run Docker to see what the system emits. It is admissible only because replay is deterministic, which makes the committed copies a regression reference rather than a stale snapshot. Live runtime output (`alerts.jsonl`, `evaluation/`) stays git-ignored; regenerating it must leave the reference copies byte-identical, and a diff between them is a real test failure.

## 6. Tooling and conventions

- Python via `uv`, one `pyproject.toml`, one lock. Extras in the runtime path: `kafka` (streaming stack), `ml` (inference), `serve`, `ingestion`; the legacy `transform`/`orchestration`/`dashboard` extras persist only until the deletion audit executes. Never call pip.
- Config through env vars from `.env` (git-ignored; `.env.example` is the template and lists only Kafka and Schema Registry variables).
- Cost ceiling: under $5 incremental, target $0. Nothing a reviewer runs requires cloud credentials or spends money.
- Style for documentation: plain declarative prose, short paragraphs, no em-dashes, technical terms explained once at first use.

## 7. Commands

The Makefile is the entry point; its `UV` variable (`uv run --extra kafka --extra ml --extra serve --extra ingestion`) is the canonical way to run any module.

```bash
uv sync --extra kafka --extra ml --extra serve --extra ingestion  # one-time setup
make demo    # up + recreate topics + register contracts + replay week + evaluation
make eval    # outcome-join evaluation report only
make test    # pytest streaming/ -q, then ruff check streaming/ scripts/
make up      # docker compose up -d --wait (Kafka + Schema Registry, KRaft)
make down    # docker compose down -v (wipes broker and registry state)
make reset   # up + recreate the three topics + register contracts
```

- Single test file: `uv run --extra kafka --extra ml --extra serve --extra ingestion python -m pytest streaming/test_evaluator.py -q` (tests live beside the code in `streaming/` as `test_*.py`; no cloud credentials needed).
- `make demo` recreates topics first, so every run is a clean deterministic replay. Two runs must produce byte-identical outputs; treat any diff as a bug.
- Model artifacts for the consumer: `bash scripts/fetch_artifacts.sh` downloads the frozen 2024-06-30 run into `ml/artifacts/` from the GitHub release. Not needed for `make demo` while the consumer is unbuilt.
- Work is sequenced as handoff prompts in `docs/HANDOFF_PROMPTS.md` (H2/H3 build the scoring consumer); check it and `docs/PLAN.md` for what is and is not built yet before assuming a component exists. Until the consumer lands, `make demo` reports every outcome as `orphan_outcome` by design.
