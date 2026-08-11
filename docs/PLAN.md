# MSDS 682 Streaming Project: Plan

This is the working plan for turning the copied 681 batch lakehouse into a realtime streaming inference project on a two-day build clock. It contains the corrections to the planning brief with evidence, the decisions and their arguments, the two-day schedule with a solo-start variant, the deletion and grep audits, the reconciliation with Aidan's draft, risks, costs, and non-goals.

Companion documents: `docs/HANDOFF_PROMPTS.md` (self-contained prompts for Aidan's fresh Claude Code sessions), `docs/PROPOSAL_DRAFT.md` (submitted and frozen; never edited again), `docs/schemas.md` (event contracts), `docs/data_sources.md` (verified external sources), and the live `CLAUDE.md` (the streaming rulebook, promoted 2026-08-11; the 681 rulebook is archived at `docs_legacy/CLAUDE_MD_LEGACY.md`). The original brief's "open questions" deliverable is superseded: a two-day path cannot leave decisions open, so they are resolved below as kickoff decisions with rationale, to be confirmed or adjusted at the first sync.

## The organizing judgment

Every project in this course has Kafka. Two things make this one good, and every scope decision is ranked against them:

1. The leakage boundary follows the data into the streaming layer. Rotation features exist only for schedule-consistent links, enforced at serve time in the consumer from the same constants the batch tests use. One rule, two enforcement points, no drift.
2. We can say what forecast substitution costs. The 681 model trained on observed weather; at serve time only a forecast exists. Quantifying that gap per horizon is a finding, not a caveat.

Anything serving neither is a cut candidate, however interesting.

## Corrections to the brief (verified against both repos)

The brief asked for verification of every claim. These items came back different from what the brief assumed. Each carries its evidence.

1. The stream repo is not history-free. It has 153 commits: the lakehouse's 152 plus one merge (`249abf9`, PR #31). HEAD trees are byte-identical between the repos. The lakehouse lives at `/Users/sebastiansteen/flight-delay-lakehouse` (home directory, not Desktop). `PR #NN` citations resolve only against `github.com/sebaleks/flight-delay-lakehouse`.
2. The CatBoost confirmation already ran (2026-08-10, commits `9003986` and `1dfc7a7`). Held-out: CatBoost ROC-AUC 0.7362 / PR-AUC 0.4623 versus the shipped 0.7389 / 0.4652, so `beats_shipped: false` and the shipped model stands. CatBoost calibrated better (Platt ECE 0.0147 vs 0.0170). The record is `ml/search_results/CONFIRM.md` and `confirm_catboost.json`. The one-time held-out budget is spent under rule 7 of `docs_legacy/leakage_discipline.md`; the former Phase 6 is now a documentation item, not work.
3. Turnaround slack already exists end to end: `sched_turnaround_slack_min = sched_turnaround_min - 35` (`dbt/models/gold/shared/int_aircraft_rotation.sql:199-200`, registered in `ml/features.py:77`, guarded in `dbt/tests/assert_ml_rotation_schedule_only.sql:171`, served at `ml/serving.py:493`). Introduced in PR #16.
4. The class-weighting versus calibration check is already resolved in-repo: `scale_pos_weight` of about 3.75 inflates raw scores (ECE 0.227), the Platt map fit on the validation slice fixes it (ECE 0.01658), and `ml/calibration.py` hard-fails any run where the served map moves ROC or PR-AUC beyond 1e-6. (0.01658, 0.017, and 0.0170 in various docs are the same number at different precision.)
5. "Four standing dbt leakage guards" is imprecise. The repo counts three (`assert_ml_features_no_leakage`, `assert_ml_weather_obs_before_departure`, `assert_ml_rotation_schedule_only` with 13 arms). The timezone test `assert_flown_airports_have_timezone` exists but is framed as a data-quality guard. Two serving-lookup guards also exist (`assert_serving_lookup_entities_constant`, `assert_serving_typical_rotation_singleton`).
6. Aidan's draft was at `docs/prior_proposal.md`, not `docs/prior_proposal_ap.md`. It is now marked superseded and lives at `docs_legacy/prior_proposal.md`.
7. The docs_legacy migration is already physically done but uncommitted (plain `mv`, unstaged). It swept `leakage_discipline.md` into `docs_legacy/` along with everything else. That file is live policy (rules 7 and 12 govern current work) and more than 15 live files cite it at `docs/leakage_discipline.md`; it must come back to `docs/`.
8. "Roughly 23 unused months" needs precision: 17 months exist beyond the test set (2025-01 through 2026-05); 23 beyond the training cutoff, of which 6 are the 681 test set.
9. No tail count is recorded anywhere in either repo. Only 0.34% tail-unknown legs and the linkage class shares (91.95 / 3.93 / 4.12) are on record. The ~6,000-7,000 claim needs the one-time cardinality query during the export step; do not assert it before then.
10. The brief's Phase 5 rule (absent inbound leg means NULL cascade features, reusing swap semantics) contradicts the repo's own serving design. Post-restriction commit `ef65330` records that all-NULL rotation MEANS a swap-restructured linkage, a day-of outcome, so the API deliberately serves a typical-rotation estimate flagged `rotation_context: "typical_estimate"` for merely-unknown rotation. Kickoff decision 7 resolves this.
11. MLflow "served by tag" is not current behavior: no `register_model`, `log_model`, or `models:/` URI exists anywhere in `ml/`. Serving loads the newest complete run under `ml/artifacts/`. The keep-and-adapt verdict stands for what MLflow actually does here (pure-side-effect logging, local SQLite metadata), plus logging per-horizon calibration curves and the drift comparison as tracked evaluation runs. A registry is stretch work, not adaptation.
12. The model artifact is not in the repo. `ml/artifacts/` is git-ignored and absent from this worktree; artifacts live only in `gs://$GCS_BUCKET/serving/<run>/` and the MLflow GCS store (about 695 MB per run; four-file contract: `xgb_classifier.ubj`, `xgb_regressor.ubj`, `logreg_pipeline.joblib`, `calibrator.joblib`). A one-time export is mandatory before any cloud coupling is deleted.
13. The 681 rulebook's section 9 (archived at `docs_legacy/CLAUDE_MD_LEGACY.md`, carried into the live CLAUDE.md section 3) requires "forecast-available" weather while the implementation joins the last ISD observation at or before scheduled departure (`dbt/models/gold/ml/ml_flight_features.sql:246-265`, 3-hour ceiling). An observation is not a forecast. This is the known deviation the TAF study resolves; the repo itself reconciles it at serve time (`ml/serving.py` header, `ml/README.md` train/serve gap #1).

Status note: the streaming rulebook is now the live `CLAUDE.md` (promoted 2026-08-11 from the draft at `docs/CLAUDE_MD_PROPOSED.md`, at Seb's direction). The 681 rulebook is archived unchanged at `docs_legacy/CLAUDE_MD_LEGACY.md`, section 0 status header included. The live file marks its repo layout and export-script line as target state pending the deletion audit.

### Deck-versus-repo discrepancies, all of them

- dbt models: the deck's 31 is the repo at PR #29; the repo now has 36 (PR #30 added exactly 5 models plus the carriers seed).
- Dagster assets: the deck's 38 is 31 models + 2 seeds + 5 Python assets at PR #29; the repo now shows 44 (36 + 3 + 5).
- Additional discrepancies against the brief's own claims: corrections 2, 3, 5, 8, and 9 above. Nothing else surfaced.

## Kickoff decisions (resolved; confirm or adjust at the first sync)

1. **Replay window: the 2024-H2 held-out week.** The tradeoff, in hours. Option A (2024-H2): export queries over the existing mart, about 1-2 hours, mostly wall-clock, no new weather engineering, and the mart's rotation columns become ground truth for a full-week parity check of the in-stream rotation state machine. Its costs: the "never inspected" claim weakens to "held-out, never trained on, previously scored only in aggregate" (disclose exactly that), and drift needs separate recent data. Option B (May 2026): the BTS download is one minute (verified live), but parsing plus mapping a new observed-weather source onto the 12 ISD-derived features adds 4 to 7 hours to the critical path, with semantic-drift risk and no parity reference. On a two-day clock, A wins. The BTS 2026-05 zip still gets downloaded on day 1 (one minute, background) so drift stays possible.
2. **Event key: bare tail number, state reset on carrier change. Settled on measured grounds (2026-08-11), no longer a judgment call.** The export-day queries measured: 6,682 distinct tails over 2022-2024; **zero** tails flown by more than one carrier within the replay week (49 ever, across three years); and only **104 of the week's 151,878 rows have a null tail, every one a cancelled flight** (F9 50, UA 46, HA 8). A composite (tail, carrier) key therefore buys nothing. Null-tail events ride the `NOTAIL` sentinel key on a dedicated partition and are always scored with the class-c NULL rotation block, which matches training semantics exactly (unknown tail is swap-shaped, `int_aircraft_rotation.sql:128-133`); at 0.068% of the week the sentinel partition is near-empty, not a hot partition. Scope note: the whole-table null-tail count is 70,198, which is 0.34% of 20,656,085, the audit's figure; an earlier sprint report showed that number without its denominator and it briefly read as a within-week share. It is not. H3's class-share report splits class c by trigger so the cancelled-null-tail subset stays visible.
3. **Artifact distribution: GitHub release asset.** Decide the slim-versus-full question when the export reveals actual file sizes; keep the four-file completeness contract if manageable, otherwise ship classifier plus calibrator and document the deliberate break.
4. **Alert threshold: calibrated p >= 0.5** (the flight is more likely delayed than not), with sensitivity reported at 0.3 and 0.7. Justified from the exported calibration and exceedance tables, never tuned on the replay week.
5. **TAF horizon bins 0-3h / 3-12h / 12-30h; retrain trigger at 0.010 PR-AUC short-bin degradation.** The threshold is about 5 times the measured 0.0018 draw-noise floor and the historical rebuild band of about 0.002, and comparable to the 0.0096 the tail-swap restriction cost. Short-bin degradation beyond it means representation mismatch; the first response is harmonization (a shared flight-rules category derived from both ISD and TAF), retraining only if harmonization fails, and never for long-bin degradation.
6. **Split at the schema, one seam adjustment.** Seb: constants module first, then replay export, producer, topics, Avro contracts, and the outcome-join evaluator. Aidan: consumer enrichment, rotation state, scoring, banding, alerts. The evaluator moves to Seb because it pairs with his outcomes producer, touches only the contracts, and rebalances day 1 (Aidan's rotation state machine is the heaviest single item). All GCP exports are Seb's (his ADC access). The tail UI is a terminal consumer; the Streamlit one-pager is cut.
7. **Tier 1 unknown-rotation semantics (only if the live mode is ever reached): follow the existing serving design**, the typical-rotation estimate flagged `typical_estimate`, because it already exists and because the repo records the semantic argument (all-NULL means swap, not unknown). This contradicts the brief's Phase 5 text; correction 10 carries the evidence.

Accepted from review (2026-08-11): Flag 1 below is accepted, so the TAF study outranks drift. These kickoff decisions stand as recommended.

## Priority order (drop from the bottom, never the top)

1. Shared constants module + in-stream rotation state under the schedule-consistency rule. The deep dive; everything else is a Kafka tutorial without it.
2. Working local demo end to end: producer, topics, Avro at the registry, consumer, scoring, alerts, outcome join, evaluator, clean-machine README.
3. PROPOSAL_DRAFT.md (submitted and frozen; the day 2 audit records deviations instead of editing it).
4. TAF forecast-substitution study (accepted swap with drift: it serves organizing judgment 2 directly, and the 2024-H2 window makes it lower-risk since the IEM archive reaches 1996).
5. Drift measurement (batch, cheap; needs the 2026 ingest and the weather-NULL design below).
6. Confluent Cloud deploy + screenshot.
7. Tier 1 live mode (cut by default).

Flag on 3, standing: the submitted proposal may claim things that later get timeboxed out. The day 2 17:00 pass audits what actually shipped against the submitted claims and writes the delta into the deviations section at the end of this plan. That section is what we speak from if anyone asks about a claim we did not meet.

Drift design under the 2024-H2 replay window: score both the 2024-H2 held-out set and the 2026 window with the same frozen model and the 12 weather features NULLed in both (the `has_origin_weather=false` path is in-distribution and trained), so the comparison is apples-to-apples on the schedule + rotation + hist regime. Disclose the regime. Prediction registered in advance: calibration degrades faster than ranking.

## Wall-clock-bound work, fired first (Step 0)

Everything here launches in background before any design work, and none of it is waited on serially:

- Docker pulls: `confluentinc/cp-kafka:8.3.1`, `confluentinc/cp-schema-registry:8.3.1` (arm64-native, KRaft).
- GCS download of the model artifacts from `gs://$GCS_BUCKET/serving/<run>/` (about 695 MB).
- BigQuery exports, landing at the exact paths the handoff prompts cite: `data/replay/departures_week.parquet`, `data/replay/outcomes_week.parquet` (week chosen via `ml/day_typicality.py`), `data/weather/isd_week.parquet`, `data/lookups/{entity_profile,density_profile,typical_rotation,airports}.parquet` plus route distances, `data/golden/rotation_reference_week.parquet` (the mart's rotation feature columns for the replay week, keyed by flight identity; the H3 parity check blocks on it), `exceedance.json`, and the tail-cardinality query.
- Golden vectors via `ml/parity.py` into `data/golden/golden_vectors.parquet`, captured while the BigQuery path is still live. This is the only proof that streaming scoring matches batch scoring and is unrecoverable if skipped; verify non-empty before moving on.
- BTS 2026-05 PREZIP (one minute, verified live).
- IEM TAF fetch for the replay week plus a 30-hour lookback into `data/weather/taf_week.csv` (about 15-25 MB at week scale).
- `uv sync` after the `kafka` extra lands in `pyproject.toml`.

## The two-day schedule

### Solo-start variant (if day 1 begins with one person; accepted ordering)

Step 0 above, all in background. Step 1: `streaming/constants.py` plus unit tests, committed and pushed by hour 1.5; every value cited in a comment to its source file and line, tests assert the values match those sources, including `hist_smoothing_prior_strength: 50` from `dbt/dbt_project.yml:54`, its sole definition. Step 2: handoff prompts H1 through H4 finalized by hour 3 (done; verify paths against the landed exports and fix any that differ). Step 3: the four Avro contracts registered against the local registry with BACKWARD compatibility by hour 4. Step 4: the replay producer hitting its gate by hour 5.5: committed Parquet, scheduled-departure order, seeded, resumable, fixed speed multiplier, one-day warm-up emission, null-tail sentinel routing; gate is validated events on `flight.departures.v1` and a second same-seed run producing a byte-identical event sequence. Checkpoints at the two-hour and four-hour marks; scope cuts happen inside the current step, never to the producer.

### Two-person grid (sync points bold; times are working blocks, 09:00-21:00)

**Day 1**
- **09:00-09:45 Sync 0.** Confirm kickoff decisions. Step 0 background tasks fire during the meeting.
- 09:45-11:00. Seb: constants module, tests, push. Aidan: H1 (compose stack up, healthchecked, smoke test).
- **11:00 Sync 1.** Constants pushed; exports verified at their paths; Avro schemas agreed; artifact-size decision (timebox; fallback release asset, slim if oversized).
- 11:00-14:00. Seb: register schemas, replay producer. Aidan: H2 (consumer enrichment and scoring, rotation stubbed swap-NULL).
- **14:00 Sync 2.** First event flows end to end. Fix together if red.
- 14:00-17:00. Seb: outcomes producer plus outcome-join evaluator (TTL-bounded state, unmatched counted, alert precision/recall plus PR-AUC/ECE, deterministic output). Aidan: H3 (rotation state machine, priority 1).
- 17:00-19:00. Seb: plan upkeep, evaluator hardening. Aidan: H4 (alerts plus the leakage test suite; full-week rotation parity, timebox to 21:00, else reconcile at 09:30 with divergence counts reported).
- **19:00 Sync 3, day 1 gate.** Full local path: producer to topics to consumer to risk topic to alerts.jsonl to evaluator report. Priority 1 tests green or one fix away.
- 19:00-21:00. Buffer, integration debt only.

**Day 2**
- **09:00-09:30 Sync 4.** Demo gate check. If red, both swarm the demo; drop from the bottom of the priority list.
- 09:30-11:30. Seb: Makefile (`make demo` / `make eval` / `make test`; no Makefile exists today), README of at most 10 steps, clean-machine rehearsal. Aidan: determinism run (two replays, byte-identical evaluator reports), golden-vector parity green, TTL and unmatched counters verified.
- **10:00 checkpoint.** TAF parsing producing decoded rows?
- **11:30 Sync 5.** Confirm the science lane order: TAF study first (accepted), drift second if time remains.
- 11:30-15:30. Aidan: H5, the TAF study; hard go/no-go at 13:00 (no usable feature rows means drop it, one-line future-work note). Seb: drift under the weather-NULL design, abandon at 14:30; then Confluent Cloud deploy and screenshots (org created now so the 30-day credit is fresh), abandon at 15:30.
- 15:30-17:30. Both: finish docs (this plan's tables kept current, schemas.md, data_sources.md; the streaming rulebook is already live).
- **17:00-17:30.** Shipped-versus-claimed audit: compare what actually shipped against the submitted proposal's claims and write the delta into the deviations section of this plan. The proposal itself is frozen and is not edited.
- 17:30-19:00. Full dress rehearsal on a clean machine; capture demo output and the evaluation report.
- 19:00-21:00. Buffer, held not planned.

### Timebox register

| Item | Abandon at | Fallback |
|---|---|---|
| Artifact size and distribution decision | Day 1, 11:00 | Release asset; slim to classifier + calibrator if oversized |
| Rotation parity versus mart | Day 1, 21:00, then Day 2, 09:30 | Ship with divergence counts reported explicitly, never silently |
| TAF parse to feature rows | Day 2, 13:00 | Drop the study; future work; the representation-mismatch analysis stands |
| Drift measurement | Day 2, 14:30 | Future-work line with the elapsed-gap statement |
| Confluent Cloud screenshot | Day 2, 15:30 | Local-only; proposal reworded to "documented deployment path" |
| Tier 1 live mode | not scheduled | Cut unless everything is green by Day 2, 15:00 |
| Streamlit tail UI | cut now | Terminal consumer output |

## Deletion-first audit (recommendations; executed so far: the docs_legacy migration, the rulebook promotion, and the reviewer-facing README/.env.example rewrites, all committed)

Default verdict is delete. Everything kept carries a one-line justification tied to the streaming path. Verdicts: keep / keep-and-adapt / export-then-delete / move-to-docs_legacy / delete.

| Path | Verdict | Rationale | What breaks if deleted |
|---|---|---|---|
| `CLAUDE.md` | keep-and-adapt (done) | Streaming rulebook promoted live; 681 rulebook archived at `docs_legacy/CLAUDE_MD_LEGACY.md` | Agent sessions revert to batch-lakehouse behavior |
| `README.md` | keep-and-adapt | Front door must describe the Kafka architecture | Reviewers land on a different project |
| `pyproject.toml`, `uv.lock` | keep-and-adapt | Add `kafka` extra; drop transform/orchestration/dashboard extras; regenerate lock | The environment |
| `.env.example` | keep-and-adapt | Zero current vars survive; Kafka and Schema Registry vars replace the all-GCP surface | New-clone setup |
| `LICENSE`, `.python-version`, `.pre-commit-config.yaml`, `.dockerignore` | keep / keep-and-adapt | Still used; pre-commit loses the sqlfluff/dbt hooks | Hygiene |
| `.github/workflows/pr-checks.yml` | keep-and-adapt | gitleaks and ruff stay; dbt/dagster/streamlit steps die; add pytest | The PR gate |
| `Dockerfile`, `Dockerfile.predictor`, `cloudbuild.yaml`, `cloudbuild.predictor.yaml`, `.gcloudignore`, `.streamlit/` | delete | Cloud Run and Streamlit deployment for services that no longer run; `cloudbuild.yaml:42` hardcodes a real project and service account (flagged leak) | Nothing; also stops accidental deploys |
| `blog_material_legacy.md` | move-to-docs_legacy | 681 writing archive, already renamed at root | Root clutter only |
| `dashboard/` (entire tree) | delete | Streamlit BI app on BigQuery views and the Cloud Run predictor; out of the streaming path | Nothing in this project |
| `orchestration/` (entire tree) | delete | Thin Dagster wrappers over entry points being deleted; a Compose replay needs no asset orchestrator | Nothing; drop `[tool.dagster]` too |
| `dbt/models/silver/`, `gold/staging/`, `gold/star/` | export-then-delete | Mart upstream only; `dim_airport` feeds serving startup and must be exported (or replaced by the airports seed) first | Warehouse rebuild ability (acceptable after export) |
| `dbt/models/gold/marts/`, `gold/dashboard/` | delete | Pre-aggregated BI views for the deleted dashboard | Nothing |
| `dbt/models/gold/shared/int_aircraft_rotation.sql` | export-then-delete | Sole SQL definition of the rotation chain, the tail-swap restriction, and the linkage constants; header preserved to `docs_legacy/tail_swap_experiment.md` (recommended standalone doc, substance recovered from history) | The pinned reference for the serving mirror and the recorded rationale |
| `dbt/models/gold/shared/int_historical_delay_rates.sql` + `dbt_project.yml:54` | export-then-delete | Sole definition of the hist smoothing formula with m=50; serving reads values, never the formula; record both before deletion | Ability to recompute hist values |
| `dbt/models/gold/ml/ml_flight_features.sql` | export-then-delete | The leakage boundary as a build artifact and the source of the replay sample | The one-time export; the authoritative weather-join semantics |
| `dbt/models/gold/ml/serving_*.sql` (3 models) | export-then-delete | Sole definitions of the serve-time lookups; their rule-12 `where is_training_row` predicates survive as documented properties of the exported data | Consumer enrichment has nothing to load |
| `dbt/tests/` (3 leakage guards) | export-then-delete | Re-expressed as pytest over the exported sample and the consumer's state machine | The machine-checked boundary |
| `dbt/tests/` (2 serving guards) | export-then-delete | Re-checked once at export time | Silent lookup nondeterminism on re-export |
| `dbt/tests/assert_flown_airports_have_timezone.sql` | export-then-delete | Data-quality check on the airports seed; one pytest over the seed replaces it | Timezone coverage assurance |
| `dbt/seeds/airports.csv`, `holidays.csv`, `carriers.csv` | keep | In-repo airport lat/lon/tz (replaces `dim_airport`), the exact training holiday calendar, readable carrier names | Timezone resolution; holiday parity; alert readability |
| `dbt/` executor scaffolding (dbt_project.yml, profiles.yml, macros, packages) | delete | No dbt runtime; record m=50 first | Nothing after exports |
| `ingestion/config.py` | keep-and-adapt | `require_env` and dotenv loading are imported by everything that survives | Env access for surviving modules |
| `ingestion/util.py` | keep | Retry download plumbing for the narrow BTS fetch | Re-implementing plumbing |
| `ingestion/bts.py` | keep-and-adapt (trim) | Keep the verified PREZIP URL template, `REQUIRED_COLUMNS`, and the month-identity check; delete the GCS half; land to a local file | The verified URL pattern and wrong-payload defense |
| `ingestion/` remainder (isd, external tables, airports, holidays_cal, README) | delete | Bulk bronze ingestion for a warehouse that no longer runs | Nothing (recoverable from history) |
| `ml/features.py` | keep | The canonical 51-feature registry every contract and assertion checks against | The event contract's anchor |
| `ml/serving.py` | keep-and-adapt | The scoring engine becomes the consumer core; swap four startup BigQuery reads for local Parquet | The consumer's scorer |
| `ml/api.py`, `ml/forecast.py` | keep-and-adapt (dormant) | The Tier 1 live-mode entry point and the only forecast-substitution implementation | Only the live mode |
| `ml/calibration.py` | keep | Defines the served Platt map and its AUC-preservation gate | Interpreting `calibrator.joblib` |
| `ml/train.py` | keep-and-adapt | `ml/serving.py` imports constants from it; provenance of the artifact | Serving import error; retraining path |
| `ml/audit.py` | keep-and-adapt | The local feature/forbidden-column audit re-pointed at the frozen sample | The pre-scoring leakage gate |
| `ml/tracking.py` | keep-and-adapt | Pure-side-effect MLflow logging, local SQLite; artifact store re-pointed local | Tracked evaluation runs |
| `ml/parity.py` | keep-and-adapt | The golden-vector harness guards the BigQuery-to-local migration | Proof of scoring parity |
| `ml/replay.py` | keep-and-adapt | The honest-framing seed of the replay evaluation | The prediction-versus-truth report logic |
| `ml/data.py`, `ml/exceedance.py`, `ml/day_typicality.py` | export-then-delete | Each runs once more against BigQuery (loader, outcome-mix bands, non-cherry-picked week choice), then has no data source | The one-time exports |
| `ml/experiments.py`, `model_search.py`, `confirm_catboost.py`, `tuning.py`, `publish.py` | delete | Model selection concluded (correction 2); GCS publisher fed Cloud Run only | Nothing; the record survives in `ml/search_results/` |
| `ml/search_results/` reports | keep | Small in-repo provenance that the shipped model won and the test was spent once | The audit trail |
| `ml/search_results/*.log` | delete | Leak a real bucket name and local username | Nothing |
| `ml/test_serving.py`, `test_day_typicality.py` | keep | Pure unit tests pinning surviving logic | Regression coverage |
| `ml/test_api.py`, `test_replay.py` | keep-and-adapt | Contract and refusal guards with streaming equivalents | Contract coverage |
| `ml/README.md` | keep-and-adapt | Holds the held-out baseline the streaming evaluation compares against | The baseline record |
| `docs_legacy/prior_proposal.md` | move-to-docs_legacy (done) | Aidan's superseded draft, marked as such; reconciliation input | The reconciliation record |
| `docs/leakage_discipline.md` | moved back to `docs/` (done) | Live policy (rules 7, 12); had been swept into legacy by the bulk move; 15+ live citations | The rulebook governing the scorer and any model swap |
| `docs_legacy/` remainder (compute_choice, benchmarks, lineage, plan, dashboard spec) | move-to-docs_legacy (already done) | compute_choice is the right-for-batch, wrong-for-events contrast; the benchmarks' methodology (executed statistics, cache off, median of runs) is the template for every number this project reports | Nothing live; keep a pointer to the preload benchmark |

## Grep audit (path, line, action; grouped within files, never across files)

Recommended-action vocabulary: delete-with-file, rewrite, move-to-docs_legacy, keep-as-is, extract-to-doc.

### A. Dangling repo references

| Path | Line(s) | Match | Action |
|---|---|---|---|
| `CLAUDE.md` | 132 | "…live in `int_aircraft_rotation`'s header and PR #18" (the only live PR citation) | extract-to-doc: the substance is recovered from history (see `docs_legacy/tail_swap_experiment.md`, recommended); the citation dies in the rewrite |
| `blog_material_legacy.md` | 34-662 | dense PR #1-#29 citation matrix | move-to-docs_legacy, do not rewrite |
| `docs_legacy/plan.md` | 16-212 | PR #26-#29 refs, one live `github.com/sebaleks/flight-delay-lakehouse` URL | keep-as-is (quarantined history) |
| `docs_legacy/leakage_discipline.md` | 202, 225-226 | PR #23/#24 refs | keep-as-is; but the file returns to `docs/` |
| `ml/experiments.py` | 216 | "pr 0.4652" is PR-AUC, a false positive | delete-with-file |

Link-level danglers: `README.md:44` and `ml/README.md:9` point at `docs/lakehouse_lineage.md`; `CLAUDE.md` sections 5 and 9 point at `docs/compute_choice.md` and `docs/leakage_discipline.md`. All four targets moved; fix in the rewrites (the leakage doc by moving it back).

### B. Course identity (681, lakehouse)

| Path | Line(s) | Match | Action |
|---|---|---|---|
| `CLAUDE.md` | 1, 11 | "Flight-Delay Lakehouse", "A GCP lakehouse…" | rewrite (done: streaming rulebook promoted live; legacy archived) |
| `README.md` | 1-67 | lakehouse identity throughout | rewrite |
| `pyproject.toml` | 2, 4 | `name = "flight-delay-lakehouse"` | rewrite |
| `dashboard/app.py`, `views/overview.py`, `README.md` | 15, 89; 3, 17; 1 | page titles | delete-with-file |
| `ml/forecast.py` | 81-82 | User-Agent "flight-delay-lakehouse" | keep-and-adapt with file (rename UA when live mode lands) |
| `ml/README.md` | 9 | dangling lineage link | rewrite with file |
| `ml/search_results/*.log` | 10-67 | absolute paths leaking username and bucket | delete-with-file |
| `orchestration/__init__.py` | 1 | docstring | delete-with-file |
| `docs/prior_proposal.md` | 11, 14, 35 | deliberate 681 provenance | keep-as-is |
| `docs_legacy/*` | various | legacy prose | keep-as-is |

False positives excluded: airport coordinates in `dbt/seeds/airports.csv`, metric decimals in `blog_material_legacy.md` (bare "681": 30 raw hits, 16 noise).

### C. Cloud coupling (~90 files; grouped by deletion unit)

| Path group | Representative lines | Match | Action |
|---|---|---|---|
| `CLAUDE.md`, `README.md` | 20 and 28 hits | GCS/BigQuery/Dagster/Cloud Run architecture | rewrite |
| `pyproject.toml` | 13-52, 76, 97-98 | `google-cloud-bigquery`, `dbt-bigquery`, `dagster*`, `streamlit` deps, `[tool.dagster]` | rewrite |
| `.env.example` | all 8 active vars | all-GCP surface (`GCP_PROJECT_ID`, `GCS_BUCKET`, `BQ_*`, `DBT_PROFILES_DIR`) | rewrite: zero survive; add Kafka bootstrap, Schema Registry URL |
| `.gitignore` | 13, 43-53 | GCS/Dagster/mlflow notes | rewrite |
| `cloudbuild.yaml` | 42 | hardcoded real project and service account | delete-with-file, leak flagged |
| `cloudbuild.predictor.yaml`, `Dockerfile`, `Dockerfile.predictor`, `.gcloudignore`, `.streamlit/` | throughout | Cloud Run deployment | delete-with-file |
| `.github/workflows/pr-checks.yml` | 27-28, 58-84 | CI placeholders, dbt parse, dagster validate, streamlit check | rewrite (keep gitleaks, ruff; add pytest) |
| `.github/PULL_REQUEST_TEMPLATE.md` | 11 | "data lives in GCS/BigQuery, never git" (now wrong: sample data is committed) | rewrite |
| `ingestion/*` | throughout | GCS upload, external tables | delete-with-file except `bts.py` trim, `config.py`, `util.py` |
| `dbt/**` | headers, env_var refs | BigQuery everywhere; `dbt/profiles.yml:9-14` is the reviewer-facing config surface | delete-with-file after exports; extract the two shared-model headers first |
| `orchestration/*`, `dashboard/*` | 43 and ~90 hits | Dagster, Streamlit, Cloud Run auth | delete-with-file |
| `ml/*` (data, publish, tracking, api, serving, parity, replay, model_search) | as audited | BigQuery mart loads, `gs://` stores | keep-and-adapt per the audit table; the BigQuery touchpoints are exactly the export-then-swap surface |

### D. Hardcoded window and cutoff

| Path | Line(s) | Match | Action |
|---|---|---|---|
| `dbt/dbt_project.yml` | 40-48 | `train_test_cutoff_date: "2024-07-01"` | extract-to-doc (the replay sample's provenance statement), then delete-with-file |
| `ml/replay.py` | 48 | `HOLDOUT_FLOOR = 2024-07-01` | extract-to-doc: the same invariant guards the replay export |
| `ml/exceedance.py` | 110 | training-row assert at the cutoff | extract-to-doc, then export-then-delete |
| `ml/tuning.py`, `ml/train.py`, `ml/README.md`, `ml/test_api.py`, `ml/search_results/*` | various | validation-slice dates, cutoff mirrors | per their file verdicts |
| `ingestion/bts.py`, `isd.py`, `holidays_cal.py` | 21-291; 6-58; 32-65 | 2022-2024 window defaults | trimmed or deleted with files |
| `orchestration/*` | 17, 55, 24 | fixed window | delete-with-file |
| `CLAUDE.md`, `README.md` | 98-99; 16 | "2022-2024" | rewrite |
| dbt model headers, `dashboard/*` | various | window prose, gsod year shards | delete-with-file |
| `dbt/seeds/holidays.csv` | 913-1097 | calendar rows, not config | noise, keep seed |

Raw "2022" hits: 425, of which 365 are holiday seed rows and most of the rest legacy prose; the table above is the meaningful set.

### E. Config surfaces a reviewer would be prompted to fill

| Path | What it asks for | Action |
|---|---|---|
| `.env.example` | 8 GCP vars, all obsolete for reviewers | rewrite to Kafka + Schema Registry vars only |
| `dbt/profiles.yml` | BigQuery oauth, project, dataset, location | delete-with-file |
| `cloudbuild.predictor.yaml` | `_RUN`, `_BUCKET`, `_REGION`, `_NWS_CONTACT` substitutions | delete-with-file |
| `Dockerfile`, `Dockerfile.predictor` | dashboard/predictor extras, baked artifacts | delete-with-file |
| Makefile | none exists; the "one make target" is new work | create |

## Reconciliation with Aidan's draft (`docs_legacy/prior_proposal.md`, superseded)

| Element | His design | This plan | Verdict | Reason |
|---|---|---|---|---|
| Contracts | Avro + Schema Registry + pydantic validation | Avro + Schema Registry only | adopt, minus pydantic | Registry enforces compatibility; consumer assertions come from the constants module; a pydantic mirror is a second contract definition to drift. Pydantic returns only if it earns a distinct in-process role (producer-side input validation) without duplicating the Avro contract |
| Alert artifact | `alerts.jsonl` | same | adopt | Right shape, cheap, demoable |
| Headline metric | streaming PR-AUC + alert precision/recall | alert precision/recall at a band threshold, PR-AUC/ECE beneath | adopt (his, sharpened) | A decision metric is what calibration buys |
| Product framing | ops/notification audience | same | adopt | His framing, verbatim |
| Topic naming | `.v1` suffixes | same | adopt | Cheap versioning hygiene |
| Fallback | smoothed route/carrier rate + flag | same | adopt | Identical to the brief's decision 11 |
| Event key | `flight_id` | tail number (sentinel for null tails) | override | In-stream rotation state needs per-tail ordering, which Kafka guarantees only within a partition key; flight_id reduces the consumer to a lookup plus a model call. The outcome join still uses flight identity as a field, so nothing is lost there. Balance is checked by the day 1 cardinality query |
| Review path | Confluent Cloud primary, full reviewer access | local Compose primary, cloud screenshotted once | override | The 30-day credit expiry is verified; a grade must not depend on trial timing. Local-first also makes the clean-machine determinism gate possible |
| Replay source | "~1-week sample" from the 681 test period | 2024-H2 held-out week (same period), day-typicality-chosen | adopt with provenance discipline | Kickoff decision 1; his instinct was right for a two-day clock, and the week is chosen by a documented method rather than hand-picked |
| Format | Individual | team of two, 50-50 split at the schema | override | Two-person submission requirement |
| Milestones | 5 build milestones | sync-gated two-day schedule + instructor sign-off | adopt, reshaped | Same skeleton, deadline-shaped, plus the required sign-off |

## Risk register

| Risk | Mitigation | Fallback |
|---|---|---|
| Artifact too large for git | Release asset; size known at export | Slim to classifier + calibrator, break documented |
| In-stream features drift from training semantics | Golden vectors + full-week rotation parity + guard re-expression + warm-up day | Divergence counts reported explicitly |
| Late/out-of-order outcomes blow join state | TTL-bounded state, unmatched counters | Shrink TTL, report unmatched share |
| TAF parse quality (visibility 6.01, amendments, missing temp/dewpoint) | Known quirks documented in H5; only 8 of 12 features mappable by design | 13:00 go/no-go, future-work note |
| Confluent Cloud credit expiry | Org created at demo time | Local-only, wording adjusted |
| Partition imbalance | RESOLVED by measurement: 6,682 tails, 5,314 in the week, sentinel partition holds 104 events | none needed |
| Two-person slippage | Both own the constants module and demo; handoff prompts are self-contained | The solo-start variant is the schedule's floor |

## Cost table

| Item | Cost |
|---|---|
| BigQuery one-time exports (reads over a ~9 GB mart and small lookups) | cents at $6.25/TB |
| GCS egress for artifacts (~700 MB) | cents |
| BTS, IEM TAF, api.weather.gov, aviationweather.gov | $0, keyless |
| Local Compose stack | $0 |
| Confluent Cloud screenshot deployment | $0 inside the 30-day credit; Basic bills $0 idle |
| Total | well under the $5 ceiling; target $0 holds |

## Deviations from the submitted proposal

The proposal (`docs/PROPOSAL_DRAFT.md`) is submitted and frozen; it is never edited to match reality. This section is the ledger of every claim in it that shipped differently, written at the day 2 17:00 audit and amended afterward if anything changes. It is what we speak from if anyone asks about a claim we did not meet.

| Proposal claim | What shipped | Status |
|---|---|---|
| Milestone 5: Confluent Cloud screenshot | pending the day 2 timebox (abandon 15:30) | open |
| Milestone 5: drift check | pending the day 2 timebox (abandon 14:30) | open |
| Milestone 4: TAF skill-by-horizon study | pending the day 2 go/no-go (13:00) | open |

## Non-goals

Everything the former Phase 6 cut, now with its confirmation recorded: no CatBoost re-run (the one-time held-out confirmation is spent: 0.7362/0.4623 vs 0.7389/0.4652, not adopted), no extended-split retraining, no pre-COVID or COVID data, no LightGBM, no monotonic constraints, no P(swap | cancellation) analysis (the in-stream cancellation-pressure feature captures the mechanism). Also: no third-party schedule API, no dbt/Dagster/BigQuery in the runtime path, no medallion rebuild, no Tier 2 or Tier 3 live modes, no Streamlit UI.
