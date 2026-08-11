# Prediction apps — working plan and status

Handoff document. Written 2026-08-10. Read this first if you are picking the
work up on another device.

Full original plan (the design reasoning, alternatives weighed, things
deliberately not built): `~/.claude/plans/can-you-make-a-lazy-bubble.md`. This
file is the *live* state — what shipped, what changed, what is next.

---

## 1. Status at a glance

| | State |
|---|---|
| **Phase 0** — serving preload + 2nd benchmark | **merged to `main`** (PR #27) |
| **Phase 1** — predictor service, evidence endpoints | **merged to `main`** (PR #28), DEPLOYED |
| **Phase 2** — consumer page | **merged to `main`** (PR #28) — verify it went live |
| **Phase 4a** — ops capacity page (replay mode) | **built**, PR #29 open (all four steps) |
| Phase 3 (plan mode), 4b (CSV upload) | deferred by decision |

**#28 was merged 2026-08-10** (no Codex re-review had appeared; CI green,
mergeable clean). CD rebuilds the dashboard from `main` on that merge — the
consumer page should be live; **verify it renders and a prediction
round-trips** (could not be checked from the remote session that merged it:
its network policy blocks `run.app`).

**Open PR: [#29](https://github.com/sebaleks/flight-delay-lakehouse/pull/29)**
(`claude/live-ide-connection-wrm1t7` → `main`), draft. Contains all of Phase
4a: the `/replay/airport-day` endpoint, `mart_delays_by_airport_hour` +
`dash_airport_hour_baseline`, the ops capacity page, and the day-typicality
harness. One commit per step.

**What 4a still needs before it is DEMOABLE (all need GCP, in order):**
1. Merge #29, `dbt run` (or `--select mart_delays_by_airport_hour+`) so the
   new mart + skin exist.
2. **Publish + deploy a new predictor image** — the deployed one predates
   `/replay/airport-day`. Same run id is fine: same artifacts, new code.
   (`gcloud builds submit --config cloudbuild.predictor.yaml ...`, §6.)
3. Run the typicality check before naming a demo date:
   `uv run --extra ml --extra ingestion python -m ml.day_typicality --origin ORD --date 2024-09-13`
   — the plan's example date is a CANDIDATE until this says TYPICAL.
4. The dashboard redeploys itself from `main`; the ops page then works
   end-to-end. Verify the headline beat renders.

### Live services

| Service | URL | Auth |
|---|---|---|
| Dashboard (BI) | https://flight-delay-dashboard-buboj66t4q-uc.a.run.app | public |
| Predictor (API) | https://flight-delay-predictor-buboj66t4q-uc.a.run.app | **private** |

The predictor refuses anonymous callers (403). To hit it by hand:

```bash
TOK=$(gcloud auth print-identity-token)
curl -s -H "Authorization: Bearer $TOK" \
  https://flight-delay-predictor-buboj66t4q-uc.a.run.app/health
# {"status":"ok","artifacts":"20260730_145241"}
```

The dashboard's service account (`dashboard-run@…`) holds `roles/run.invoker`,
and the dashboard service already has `PREDICTOR_URL` set. **The consumer page
will not appear on the live dashboard until #28 merges** — CD rebuilds the
dashboard image from `main` on push, and the page code is not there yet.

---

## 2. What is done

### Phase 0 — serving preload (merged, PR #27)

Materialized the serving lookups as three tiny dbt models
(`serving_entity_profile` 8,316 rows, `serving_density_profile` 34,979,
`serving_typical_rotation` 1) and read them once at startup.

| | Before | After |
|---|---|---|
| BigQuery queries per prediction | 6 | **0** |
| Bytes per prediction | 2.31 GB | **0** |
| Cost per 100k predictions | $1,315 | **$0** |
| Latency, 1 flight | 5,078 ms | **13 ms** (390×) |

Documented in `docs/benchmarks/serving_preload_benchmark.md`; blog material in
`blog_material.md` chapter 26.

### Phase 1 — predictor service (deployed, in #28)

- `prediction_basis` on every response: `flight_in_past` plus `weather_horizon`
  ∈ `forecast` / `beyond_horizon` / `past` / `unavailable`.
- `GET /calibration` — held-out reliability table, 10 bands, 3,561,782 rows.
- `GET /outcome-mix` — held-out P(arrival delay ≥ t), t ∈ {15,30,60,90,120},
  built by `ml/exceedance.py`.
- `ml/publish.py` → `gs://$GCS_BUCKET/serving/<run>/`, immutable once complete.
- `Dockerfile.predictor` + `cloudbuild.predictor.yaml`, **manual** deploy.
- NWS User-Agent now carries a real contact (`NWS_CONTACT_EMAIL`).
- `/demo/ord-departures` → `/demo/airport-departures?origin=XXX`.

### Phase 2 — consumer page (built, in #28, not live)

- `dashboard/uncertainty.py` — the wording rules, pure and unit-tested.
- `dashboard/predict_client.py` — HTTP + ADC ID token; `dashboard/` never
  imports `ml/`.
- `dashboard/views/predict_flight.py` — registered as "Will my flight be late?".

38 tests green; ruff, `dbt parse`, `dagster definitions validate` all clean.

---

## 3. Deviations from the original plan — read this section

Everything here differs from what the approved plan said. Most were forced by
something discovered while building.

**Scope was cut to 0 + 1 + 2 + 4a.** Phase 3 (plan mode: compare alternatives,
best time to fly, connection risk, insurance framing) and Phase 4b (CSV upload
for real airline schedules) are deferred. Recorded in the plan file; connection
risk is the one worth adding back first if time appears.

**`ml/exceedance.py` moved from Phase 3 to Phase 1.** Phase 2 depends on it —
it is what lets the consumer page show an outcome mix instead of the
regressor's point estimate. Deferring Phase 3 without moving it would have
quietly reintroduced `"expected delay: +23 min"`, the single most misleading
number the system can show a person (held-out MAE 18.99 / RMSE 49.26).

**The medians changed on purpose — this is a behaviour change, not a refactor.**
The typical rotation profile was built with `approx_quantiles` **at process
startup**, which is approximate and shard-dependent. The same median was
observed returning **four different values** for `inbound_distance` (666 / 674 /
663 / 651; exact is 667). Two processes could score the same context-less
request differently — largest measured divergence **0.4300 vs 0.4968**. Now
exact `percentile_disc`, verified to rebuild byte-identically. Relatedly
`min(distance)` replaced `any_value(distance)` (85 of 7,539 routes carry two
distances from a 1-mile rounding split).

**Because of that, parity is reported in two halves, not as one "bit-identical"
claim.** 28/28 requests that supplied both rotation context and density are
bit-identical; of the 156 that depend on the changed medians, 151 were unchanged
and only the four median-fed features moved anywhere.

**PR #26 was auto-closed by my mistake.** I merged #25 with `--delete-branch`,
which deleted #26's base branch and closed it; a closed PR cannot be retargeted.
Reopened as #27 from the same untouched branch. No commits lost. **Lesson for
next time: retarget dependent PRs to `main` BEFORE deleting a base branch.**
A side benefit — #26 had never run CI (`pr-checks.yml` only triggers on PRs
targeting `main`), and #27 did.

**Publishing artifacts needed a redesign after the first attempt failed.** The
729 MB upload timed out on the 438 MB regressor (the storage client's default
120 s deadline), leaving a partial prefix that the immutability guard then
refused to retry — stranding the run. Fixed with 8 MiB resumable chunks, a 900 s
timeout, and a `_PUBLISHED.json` **completion marker written last**: a prefix
*with* the marker is immutable and refused; one *without* is recognised as
failed-upload wreckage and safely overwritten. `cloudbuild.predictor.yaml` also
refuses to bake a run whose marker is missing.

**The demo endpoint's proxy day is now looked up, not computed.** It subtracted
a literal 104 weeks, which stops landing inside the 2022-2024 mart once target
dates pass ~2026-12-30 — every call would 404. It now selects the most recent
in-mart date with the same weekday at that origin. Self-healing, and verified
working for 2027-06-15.

**The deploy is manual, not push-to-main.** The dashboard keeps its automatic
trigger; the predictor does not. Which model is served is a decision, not a
consequence of merging.

---

## 4. Codex review workflow — now part of the routine

Codex posts **inline PR review comments**, which live at
`repos/OWNER/REPO/pulls/N/comments`. They are *not* issue comments, which is why
`gh pr view --comments` shows nothing for these PRs — that is why they were
being missed.

```bash
gh api repos/sebaleks/flight-delay-lakehouse/pulls/28/comments \
  --jq '.[] | select(.user.login|test("codex"))
        | "\(.path):\(.line // .original_line)\n\(.body)\n"'
```

**The step: after pushing to a PR, wait a few minutes, pull the review, and
address the findings before calling the PR done.**

### Findings so far, and what they were

| PR | Finding | Severity | Status |
|---|---|---|---|
| #27 | Parity harness sampled with `any_value()` per column — could assemble rotation contexts no real flight has, and report **false regressions** | P2 | fixed (`row_number()` over a total order) |
| #27 | `compare()` only asserted the independent subset — a broken density table would print differences and still **exit 0** | P2 | fixed (fails by default; `--expect-medians-change` still fails on unreachable features) |
| #27 | Bulk demo endpoint still built the 51-key features block and popped it | P2 | already fixed in `c71d08e` before the review landed |
| #28 | `substitutionOption` at the build root — Cloud Build **rejects the whole config**; the deploy could never have started | **P1** | fixed (moved under `options:`) |
| #28 | `_departure_utc` truncated minutes before the past/future check — at 17:05 a 17:30 flight read as `past`, so a UI hard-gating on that value would hide valid predictions for up to 59 min before **every** departure | **P1** | fixed (`hour_only` flag; weather keeps the hour bucket, the past check uses the full instant) + 2 regression tests |
| #28 | Interrupted publish stranded the run | P2 | fixed (completion marker + resumable uploads) |

The first #27 finding is worth remembering: **the parity harness reproduced the
exact class of defect it existed to detect.**

**A re-review of #28's fix commits (`8ac4b1f`, `bbaf655`) had not appeared as of
this writing — check for it before merging.**

---

## 5. Next actions

1. ~~Check for the Codex re-review of #28~~ — done 2026-08-10, none appeared.
2. ~~Merge #28~~ — **merged 2026-08-10.** Still to do: verify the consumer
   page went live and a prediction round-trips (see §1).
3. **Check Codex's review of #29, address findings, merge, then run the
   demoable checklist in §1** (dbt build → new predictor image → typicality
   check → verify the ops page live).
4. ~~Phase 4a — build~~ — **built 2026-08-10**, in PR #29. What was built,
   per the original spec below: the replay endpoint guards with the run's own
   split boundary and returns Poisson-binomial summary + per-flight labels;
   the ops page adds a history-implied climatology line (SUM/SUM baseline ×
   the day's schedule) so the model visibly has to beat history; downstream
   exposure counts swap-shaped linkage as ZERO (honest undercount, stated);
   the typicality verdict is two-tailed — a suspiciously nailed day is as
   cherry-picked as a missed one. Original spec, for reference:
   > *"On 2024-09-13 the model expected 41 ± 9 delayed departures in ORD's
   > 18:00 bank. There were 44."*
   - `GET /replay/airport-day?origin=ORD&date=YYYY-MM-DD` on the predictor:
     read held-out rows straight from `ml_flight_features` (reuse
     `ml/replay.py`'s loader — features are already in the mart, so **no
     forecast call and no feature assembly**), score, return predictions **with
     the labels alongside**. Guard: assert every row is held-out and 404 on a
     training-window date.
   - New dbt model `mart_delays_by_airport_hour` — grain
     `(origin_airport_key, day_of_week, crs_dep_hour)`, ≤62,832 rows, **additive
     counts and sums only** — plus a `dash_airport_hour_baseline` skin.
   - `dashboard/views/ops_capacity.py`: expected delayed departures per hour
     (`Σ p`) with a Poisson-binomial band (`Σ p(1−p)`), **actual overlaid**,
     downstream network exposure via schedule linkage only, fragile-bank
     screening, proactive-comms ranking.
   - Before trusting a demo date, run the calibration-implies-counts check
     across a spread of held-out days and say whether the chosen day is typical.
     Picking the first day that looks good is the same cherry-pick trap as the
     top-8 examples.

### Still open on the course itself

All six assignment requirements and the Option 3 deep-dive bullets are already
satisfied. The only graded items left are the **blog post (45 pts)** and the
**final presentation/demo (35 pts)**. None of this app work is compliance work —
it is demo and blog material. If time gets tight, that is the trade to make.

---

## 6. Operational reference

```bash
# env
set -a && . ./.env && set +a          # GCP_PROJECT_ID, BQ_GOLD_DATASET, GCS_BUCKET

# local predictor
NWS_CONTACT_EMAIL=you@example.com \
  uv run --extra ml --extra serve --extra ingestion uvicorn ml.api:app --port 8000

# local dashboard (prediction page needs PREDICTOR_URL)
PREDICTOR_URL=http://127.0.0.1:8000 \
  uv run --extra dashboard streamlit run dashboard/app.py

# the gate CI runs
.venv/bin/ruff check ingestion ml orchestration dashboard
.venv/bin/ruff format --check ingestion ml orchestration dashboard
uv run --extra transform dbt parse --project-dir dbt --profiles-dir dbt
uv run --all-extras dagster definitions validate -m orchestration.definitions
uv run --extra dashboard --extra ml --group dev pytest dashboard ml -q   # 38 tests

# parity, before AND after any serving-lookup change
uv run --extra ml --extra serve --extra ingestion python -m ml.parity capture before.json
uv run --extra ml python -m ml.parity compare before.json after.json
#   add --expect-medians-change ONLY when the change is supposed to move them

# held-out replay + the outcome-mix table
uv run --extra ml --extra ingestion python -m ml.replay --sample 200000
uv run --extra ml --extra ingestion python -m ml.exceedance

# publish a run + deploy it (manual, pinned)
uv run --extra ml --extra ingestion python -m ml.publish        # prints _RUN
gcloud builds submit --config cloudbuild.predictor.yaml \
  --substitutions=_RUN=<run>,_BUCKET=$GCS_BUCKET,_NWS_CONTACT=you@example.com
```

### Gotchas worth not rediscovering

- **`dashboard/` must never `import ml`.** The BI image carries only the
  `dashboard` extra; adding xgboost + ~695 MB of artifacts would put a model
  load on every cold start for every visitor who wanted the delay map.
- **Rebuild the `serving_*` lookups before deploying a predictor image** against
  a dataset. Startup fails loudly if they are absent (deliberately).
- **`ml/artifacts/` is ~15 GB locally.** The ignore files re-exclude it while
  including `ml/`. Check with `gcloud meta list-files-for-upload` — a correct
  context is ~50 files / <1 MB.
- **Publishing is immutable per run id.** To ship a new model, train a new run;
  do not try to overwrite one a deployed image may be pinned to.
- Cold start is **~20–40 s** at `min-instances=0`. That is the deliberate trade
  for zero idle cost; the UI says so rather than showing a bare spinner.
