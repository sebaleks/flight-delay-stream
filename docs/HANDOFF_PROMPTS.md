# Handoff Prompts (H1 to H5)

These are self-contained prompts for fresh Claude Code sessions. Each one assumes zero prior context. Paste one prompt per session. Run them in order: H1, then H2, then H3, then H4. H5 runs on Day 2 only if the Day 2 11:30 sync picks the TAF lane (TAF means Terminal Aerodrome Forecast, the airport point forecast pilots and ops teams use).

Before running H2 or later, confirm Seb has pushed the constants module and the data exports. Every prompt starts with a preflight check that verifies this and stops if anything is missing.

The schedule these prompts serve is in `docs/PLAN.md`. The event contracts are in `docs/schemas.md`.

---

## H1: Environment and Kafka stack (Day 1, 09:45)

```
You are working in the repo at ~/flight-delay-stream (branch main). This is a
two-person, two-day MSDS 682 streaming project: a replay producer streams one
week of historical flight departures into Kafka, a consumer scores each
departure for delay risk with a frozen XGBoost model, and an evaluator joins
scored events to late-arriving outcomes. Today you are setting up the local
stack.

TASK
Create docker-compose.yml at the repo root with two services:
1. confluentinc/cp-kafka:8.3.1, single broker in KRaft mode (KRaft is Kafka's
   built-in metadata mode; no ZooKeeper exists in 8.x). Combined broker and
   controller roles, a fixed CLUSTER_ID, port 9092 exposed, and a healthcheck.
2. confluentinc/cp-schema-registry:8.3.1, depending on the broker being
   healthy, port 8081 exposed, and a healthcheck.
Both images publish arm64 and run natively on Apple Silicon.

Then add a `kafka` optional-dependency extra to pyproject.toml containing
confluent-kafka (with Avro support), fastavro, and requests. Run `uv sync
--extra kafka --extra ml`. Do not remove existing extras.

Write a smoke test script (scripts/smoke_kafka.py or similar) that creates a
throwaway topic, produces one message, consumes it back, registers a trivial
Avro schema in the Schema Registry, fetches it back, and prints PASS for each
step.

GATE
`docker compose up -d` reaches healthy on both services, and the smoke test
prints all PASS lines.

VERIFY
- docker compose ps shows both services healthy.
- curl -s http://localhost:8081/subjects returns a JSON array.
- uv run --extra kafka --extra ml python scripts/smoke_kafka.py prints PASS
  for produce, consume, register, fetch.

BINDING CONSTRAINTS
- Confluent images pinned to 8.3.1. Local Docker Compose is the primary
  deliverable; nothing may depend on any cloud service.
- Use uv for everything Python. Never call pip.

DO NOT TOUCH
- CLAUDE.md (read-only; the live streaming rulebook), docs_legacy/, dbt/, dashboard/, orchestration/, ingestion/,
  any file under ml/, and anything that talks to GCP.

STYLE
Plain declarative code and comments. Comments only where a constraint is not
visible in the code itself.

REPORT BACK
List files created, the exact commands you verified with, their output, and
any deviation from this prompt with one line of reasoning.
```

---

## H2: Consumer enrichment and scoring (Day 1, 11:00)

```
You are working in the repo at ~/flight-delay-stream (branch main). Context: a
replay producer (built by the other teammate) streams one week of flight
departure events into the Kafka topic flight.departures.v1, Avro-encoded
against the local Schema Registry (localhost:8081). Your job is the scoring
consumer. A frozen, Platt-calibrated XGBoost classifier predicts P(arrival
delay >= 15 min) from 51 features that are all knowable before departure
(Platt calibration is a monotonic remap that turns raw scores into honest
frequencies).

PREFLIGHT (stop and report if any item is missing)
- streaming/constants.py exists (shared leakage constants; pushed by Seb).
- data/lookups/entity_profile.parquet, density_profile.parquet,
  typical_rotation.parquet, airports.parquet exist.
- data/replay/departures_week.parquet exists.
- ml/artifacts/ contains one run directory with xgb_classifier.ubj and
  calibrator.joblib.
- data/golden/golden_vectors.parquet exists.
- docker compose up -d gives healthy Kafka + Schema Registry (H1 done).

WHAT THE PIECES ARE
- streaming/constants.py is the single source of truth for every leakage
  constant (turnaround bands, duty window, forbidden fields, knowable_at
  field sets). Import it. Never restate a value from it.
- data/lookups/* are the serving lookup tables exported from the 681
  warehouse: entity_profile holds the training-window historical rates
  (hist_*) for route/carrier/origin/dest/turnaround-band/rotation-position
  grains plus route distances; density_profile holds the (origin, hour,
  weekday) departure-density medians; airports holds iata -> lat/lon/tz.
  The reference port is ml/serving.py build_context() and
  _load_serving_lookups() (lines ~220-330): it loads exactly these tables
  into plain dicts so the scoring path does zero queries. Reuse ml/serving.py
  functions where they import cleanly; port minimally where they reach for
  BigQuery.
- ml/features.py FEATURES is the canonical 51-column registry. The assembled
  frame must be asserted equal to it before any prediction (ml/serving.py
  lines ~202-208 shows the assertion pattern).

TASK
Build streaming/consumer.py:
1. Consume flight.departures.v1 with Avro deserialization against the
   registry.
2. Enrich each event to the 51-feature frame: hist_* by name from the
   lookups (new entities stay NaN), origin_dep_density_hour from the density
   lookup, holiday flags with the same `holidays` library ml/serving.py uses,
   weather features joined from data/weather/isd_week.parquet as the last
   observation at or before scheduled departure within 3 hours (the staleness
   ceiling is in streaming/constants.py; missing weather means NaN plus
   has_origin_weather=false).
3. Rotation block: STUB for now. Emit the swap-shaped state: every rotation
   feature NULL, rotation basis field set to "swap_null". This is
   in-distribution by construction (4.12% of training rows have this shape).
   H3 replaces the stub with the real state machine.
4. Assert the frame schema against ml/features.FEATURES, score with the
   XGBoost classifier, apply the Platt calibrator, attach the risk band, and
   produce an Avro event to flight.delay_risk.v1 following docs/schemas.md
   (fields include delay_probability, risk_band, rotation_state_basis,
   weather_basis, model artifact run id).

GATE
With the producer running (or a hand-produced test event), a calibrated
scored event appears on flight.delay_risk.v1, and a golden-vector spot check
passes: for 20 rows of data/golden/golden_vectors.parquet, the hist_* values
your enrichment produces match the golden values exactly.

VERIFY
- A consumer group offset advances on flight.departures.v1.
- kcat or a python consumer shows valid delay_risk events with p in [0,1].
- pytest streaming/test_enrichment.py (write it) passes the golden check.

BINDING CONSTRAINTS
- Import streaming/constants.py for every threshold; never inline a value.
- The Avro schemas at the registry are the ONLY contract. No pydantic
  contract mirror.
- No post-departure field may enter the feature frame. The knowable_at sets
  in streaming/constants.py define what is forbidden; the schema assertion
  enforces it.
- Determinism: no wall-clock time in any scored field derivation; event time
  comes from the event.
- Never call BigQuery or GCS. Everything reads from local files.

DO NOT TOUCH
- CLAUDE.md (read-only; the live streaming rulebook), docs_legacy/, dbt/, dashboard/, orchestration/, ingestion/,
  ml/ modules (read them, import them, do not edit them), the exported
  parquet files (read-only inputs).

STYLE
Plain code, short functions, comments only for invisible constraints.

REPORT BACK
Files created, gate result with command output, the golden-check pass count,
any port-vs-import decision you made for ml/serving.py functions and why,
open issues.
```

---

## H3: In-stream rotation state machine (Day 1, 14:00; priority 1)

```
You are working in the repo at ~/flight-delay-stream (branch main). Context:
streaming/consumer.py scores flight departure events from Kafka but currently
stubs the aircraft-rotation block as all-NULL. Rotation features describe an
aircraft's day: which leg of the day this is, how tight the turnaround from
the inbound leg is. They are derived ONLY from schedule data, and only when
the linkage between legs is schedule-consistent. This restriction is the
project's core leakage rule: BTS records the tail number that OPERATED the
flight after the fact, so a linkage restructured by a same-day aircraft swap
is itself an outcome, not something knowable before departure. Your task is
the in-stream version of that rule.

PREFLIGHT (stop and report if any item is missing)
- streaming/constants.py, streaming/consumer.py exist and H2's gate passed
  (git log and the H2 report say so).
- data/replay/departures_week.parquet exists.
- data/golden/rotation_reference_week.parquet exists (the 681 warehouse's
  rotation feature columns for every leg of the replay week; this is your
  parity target).

THE RULE (from streaming/constants.py; the authoritative prose is in
docs_legacy/tail_swap_experiment.md and dbt/models/gold/shared/
int_aircraft_rotation.sql lines 110-140)
Events arrive keyed by tail number in scheduled-departure order. For each
tail, maintain the running day state and classify every leg's linkage:
- CONSISTENT INBOUND: the previous leg of this tail has known scheduled
  arrival, arrives at this leg's origin (station continuity), the gap from
  scheduled arrival to this scheduled departure is within the duty window
  [0, 840] minutes, and the previous leg does not overlap this one in
  schedule. Emit the full rotation block: rotation_position, legs_today,
  has_inbound_leg=true, sched_turnaround_min, sched_turnaround_slack_min
  (turnaround minus 35), is_tight_turnaround (< 35), inbound_distance,
  inbound_crs_elapsed_min, and the turnaround band key for the hist lookups
  (bands no_inbound / lt_35 / 35_60 / 60_120 / ge_120; position capped at 6
  for the hist grain).
- CLEAN FIRST LEG: no prior leg in state, or an overnight break longer than
  840 minutes parked at this origin. has_inbound_leg=false, position/legs
  tracked, inbound fields NULL, band no_inbound.
- SWAP-SHAPED: anything else (negative gap, continuity violation, schedule
  overlap, unknown tail, prior leg with unknown scheduled arrival). EVERY
  rotation feature NULL including the band key. rotation_state_basis =
  "swap_null".
Additional policies: null-tail events arrive on a sentinel key and are
always swap-shaped (matches training: unknown tail is class c). A carrier
change on the same tail resets the state. Events inside the producer's
warm-up day are scored but flagged rotation_state_basis="warmup" and are
excluded from evaluation.
All thresholds come from streaming/constants.py. If a needed constant is
missing there, STOP and report; do not define it locally.

TASK
1. Implement the state machine in streaming/rotation.py, wired into
   consumer.py, replacing the H2 stub. Set rotation_state_basis to
   consistent / clean_first / swap_null / warmup.
2. Write pytest cases for each class and each swap trigger, plus
   carrier-change reset and warm-up.
3. Parity run: stream the full replay week through the consumer and compare
   your emitted rotation features against
   data/golden/rotation_reference_week.parquet on the flight key
   (flight_date, carrier, flight_number, origin, dest, crs_dep_time).
   Report class shares (training reference: 91.95% consistent, 3.93% clean
   first, 4.12% swap-shaped) and the mismatch count per column.

GATE
Zero mismatches, or every mismatch class counted and explained in one line
each. Never silently drop a divergence. If mismatches remain unresolved at
21:00, ship with the counts reported and flag for the Day 2 09:00 sync.

VERIFY
- pytest streaming/test_rotation.py green.
- The parity script prints class shares and a per-column mismatch table.

BINDING CONSTRAINTS
- Constants imported from streaming/constants.py, never restated.
- Tail is the partition key; your state may assume per-tail ordering within
  a partition and nothing across tails.
- Determinism: same input stream, same output, byte for byte.
- The prior leg's ACTUAL times, delays, or cancellation status must never
  enter state or features. Schedule columns only.

DO NOT TOUCH
- CLAUDE.md (read-only; the live streaming rulebook), docs_legacy/, dbt/, dashboard/, orchestration/, ingestion/,
  ml/ modules, exported parquet files.

STYLE
Plain code, short functions, comments only for invisible constraints.

REPORT BACK
Class-share table, mismatch table, gate result, files changed, open issues.
```

---

## H4: Alerts and the leakage test suite (Day 1, 17:00)

```
You are working in the repo at ~/flight-delay-stream (branch main). Context:
streaming/consumer.py scores flight departure events (H2) with real in-stream
rotation state (H3) and produces to flight.delay_risk.v1. Two things remain
on your side: the alert artifact and the tests that make the leakage
enforcement demonstrable rather than claimed.

PREFLIGHT (stop and report if any item is missing)
- H2 and H3 gates green per their reports.
- streaming/constants.py present; docs/schemas.md present.

TASK
1. Alert writer: from the consumer, append one JSON line per alert to
   alerts.jsonl for every scored event with calibrated p >= the alert
   threshold in streaming/constants.py (0.5 primary). Fields per
   docs/schemas.md: flight identity, p, risk_band, threshold, issued_at
   (event-time derived, not wall clock).
2. Leakage test suite (streaming/test_leakage.py):
   a. Swap-parity test: a swap-shaped link produces NULL cascade features at
      serve time exactly as in training. Construct one synthetic tail with a
      continuity violation and assert every rotation feature and the band
      key are NULL, and hist_turnaround_band_* / hist_rotation_position_*
      resolve to NaN.
   b. Post-departure exclusion: assert no field tagged post_departure in
      streaming/constants.py knowable_at sets can reach the scorer. Inject
      a forbidden column (for example arr_delay) into an assembled frame and
      assert the schema gate raises.
   c. Guard-fails-when-violated: temporarily monkeypatch a rule (for
      example, widen the duty window) inside the test and assert the parity
      or classification test FAILS. This proves the tests detect violations
      rather than passing vacuously.
   d. Contract test: the registered flight.departures.v1 schema contains no
      field tagged post_departure.

GATE
pytest green, and the deliberate-violation test demonstrably fails when the
violation is active (capture the failing output inside the test with
pytest.raises or a subprocess assert).

VERIFY
- uv run pytest streaming/ -q green.
- alerts.jsonl populated after a replay run; line count matches the number
  of scored events with p >= threshold (report both numbers).

BINDING CONSTRAINTS
- Constants imported, never restated. Avro at the registry is the only
  contract. Determinism: two identical replay runs produce byte-identical
  alerts.jsonl.

DO NOT TOUCH
- CLAUDE.md (read-only; the live streaming rulebook), docs_legacy/, dbt/, dashboard/, orchestration/, ingestion/,
  ml/ modules, exported parquet files.

STYLE
Plain code, short functions, comments only for invisible constraints.

REPORT BACK
Test list with one-line purpose each, gate result, alert count vs scored
count, files changed, open issues.
```

---

## H5: TAF forecast-substitution study (Day 2, 11:30; go/no-go 13:00)

```
You are working in the repo at ~/flight-delay-stream (branch main). Context:
the delay model was trained on OBSERVED weather (the last hourly ISD surface
observation at or before scheduled departure). At serve time in a real
deployment only a FORECAST exists. This study measures what that
substitution costs, per forecast horizon. It is the project's second core
finding. The replay week is from 2024-H2; the Iowa Environmental Mesonet
(IEM) archives TAFs back to 1996, and Seb fetched the week (plus a 30-hour
lookback) on Day 1.

PREFLIGHT (stop and report if any item is missing)
- data/weather/taf_week.csv exists (IEM decoded TAF export, one row per
  forecast group; columns include station, valid [issuance], fx_valid,
  fx_valid_end, sknt, drct, gust, visibility, presentwx, skyc, skyl,
  is_amendment).
- data/replay/departures_week.parquet, data/weather/isd_week.parquet,
  streaming/constants.py, and the H2/H3 consumer all present and green.
- An evaluation baseline exists: the replay week scored with observed
  weather (the normal consumer output).

KNOWN QUIRKS OF THE TAF EXPORT
- visibility greater than 6 miles is encoded as 6.01.
- Amendments carry is_amendment; they supersede the scheduled issuance.
- TAF carries NO temperature, dewpoint, or precipitation amount. Only 8 of
  the 12 weather features are mappable (wind speed, gust and gust_reported,
  visibility, and the fog/rain/snow/thunder flags from presentwx). The other
  4 (temp, dewpoint, precip_1h) go NaN. That is part of the measured cost;
  say so in the report, do not impute.

TASK
1. Parse the TAF export into a forecast table keyed by (station, issued_at,
   valid_at). GO/NO-GO: if this table is not producing usable feature rows
   by 13:00, STOP, write the one-line future-work note, and report.
2. For each departure event, select the latest forecast group with
   issued_at <= scheduled departure (a standing guard; assert it) whose
   validity covers the scheduled departure hour. Horizon = scheduled
   departure minus issued_at, binned 0-3h / 3-12h / 12-30h.
3. Rescore the replay week with TAF-substituted weather (8 mapped features,
   4 NaN), everything else identical.
4. Report per bin: ROC-AUC, PR-AUC, ECE against joined outcomes, next to
   the observed-weather baseline. State coverage: how many departures had a
   usable TAF (majors all issue TAFs; the tail of the 374 origins will not;
   those rows take the NULL weather path and are counted separately).
5. Evaluate the pre-registered trigger from docs/PLAN.md: short-bin (0-3h)
   PR-AUC degradation > 0.010 means representation mismatch, and the first
   response is harmonization (a shared flight-rules category from both
   sources), not retraining. Do not implement harmonization; evaluate and
   report the trigger.

GATE
The per-horizon table exists with the trigger evaluated, or the 13:00
no-go note is written. Either outcome is a valid result.

VERIFY
- The report script is deterministic and rerunnable.
- forecast.issued_at <= prediction_time asserted for every substituted row.

BINDING CONSTRAINTS
- Constants imported, never restated. No external fetches during scoring;
  the TAF file is the only forecast source. Determinism throughout.

DO NOT TOUCH
- CLAUDE.md (read-only; the live streaming rulebook), docs_legacy/, dbt/, dashboard/, orchestration/, ingestion/,
  ml/ modules, exported parquet files.

STYLE
Plain code, short functions, comments only for invisible constraints.

REPORT BACK
Coverage numbers, the per-horizon table, trigger verdict, files changed,
open issues. If no-go: the one-line future-work note and where you stopped.
```

---

## What Seb must have in place before each prompt

| Prompt | Blocks on |
|---|---|
| H1 | nothing (can start immediately) |
| H2 | `streaming/constants.py` pushed; `data/lookups/*`, `data/replay/departures_week.parquet`, `data/weather/isd_week.parquet`, `data/golden/golden_vectors.parquet`, `ml/artifacts/<run>/` present |
| H3 | H2 gate; `data/golden/rotation_reference_week.parquet` present |
| H4 | H3 gate; `docs/schemas.md` present |
| H5 | Day 2 sync decision; `data/weather/taf_week.csv` present; observed-weather baseline scored |
