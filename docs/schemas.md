# Event Contracts

Four contracts: three Kafka topics and one file artifact. Avro schemas registered in the Schema Registry are the only contract definitions; no application-level mirror (no pydantic models of these shapes). Compatibility is enforced at the registry, set to BACKWARD per subject (new consumers must read old events; concretely, fields are only ever added with defaults, never removed or retyped without a new topic version).

Every field carries a custom `knowable_at` property with one of three values:

- `schedule`: knowable at booking time from the published schedule. Safe as a model input.
- `pre_departure_stream`: derived in the stream strictly before the scheduled departure time T (scores, bands, trailing-window pressure counts). Safe to emit at T; never an input to anything scored at an earlier T.
- `post_departure`: realized outcomes. Never a model input; exists only on the outcomes topic and in the evaluator.

The leakage rule this encodes is CLAUDE.md section 3 (the rule carried verbatim from the 681 rulebook's section 9), including the linkage clause. The machine-readable field sets live in `streaming/constants.py`; a contract test asserts no `post_departure` field appears in the departures or delay_risk schemas.

## Keying and partitioning

Key: tail number (Avro string key), because in-stream rotation state requires per-tail ordering and Kafka guarantees order only within a partition key. Null-tail flights (some cancelled legs, some regional feeds) use the sentinel key `"NOTAIL"` routed to a dedicated partition and are always scored with the swap-shaped NULL rotation block, which is exactly the training semantics for unknown tails. A carrier change on the same tail resets consumer state; the key stays the bare tail. Flight identity for joins is the six-field grain in every payload: (flight_date, carrier, flight_number, origin, dest, crs_dep_time), the mart's tested-unique grain.

## 1. `flight.departures.v1` (topic; every field `knowable_at: schedule`)

The event is deliberately lean: identity plus schedule primitives. Calendar features (day of week, month, holiday flags), route, and departure hour are derived deterministically by the consumer; weather is consumer-side enrichment (observed table in replay, forecast in live mode), so no weather field rides the event.

```json
{
  "type": "record",
  "name": "Departure",
  "namespace": "flightdelay.v1",
  "fields": [
    {"name": "flight_date",        "type": {"type": "int", "logicalType": "date"},            "knowable_at": "schedule", "doc": "BTS service date"},
    {"name": "carrier",            "type": "string",                                           "knowable_at": "schedule", "doc": "reporting carrier code"},
    {"name": "flight_number",      "type": "string",                                           "knowable_at": "schedule"},
    {"name": "origin",             "type": "string",                                           "knowable_at": "schedule", "doc": "IATA"},
    {"name": "dest",               "type": "string",                                           "knowable_at": "schedule", "doc": "IATA"},
    {"name": "crs_dep_time",       "type": "string",                                           "knowable_at": "schedule", "doc": "scheduled local HHMM; part of the unique grain"},
    {"name": "crs_dep_ts_utc",     "type": {"type": "long", "logicalType": "timestamp-millis"},"knowable_at": "schedule", "doc": "scheduled departure, UTC; precomputed at export from the seed timezone"},
    {"name": "crs_arr_time",       "type": ["null", "string"], "default": null,                "knowable_at": "schedule", "doc": "scheduled local arrival HHMM"},
    {"name": "crs_elapsed_min",    "type": ["null", "double"], "default": null,                "knowable_at": "schedule"},
    {"name": "distance_mi",        "type": ["null", "double"], "default": null,                "knowable_at": "schedule"},
    {"name": "tail_number",        "type": ["null", "string"], "default": null,                "knowable_at": "schedule", "doc": "null means sentinel-keyed, swap-shaped rotation semantics; see note below"},
    {"name": "mode",               "type": {"type": "enum", "name": "Mode", "symbols": ["replay", "live"]}, "knowable_at": "schedule"},
    {"name": "is_warmup",          "type": "boolean", "default": false,                        "knowable_at": "schedule", "doc": "producer-marked warm-up day; scored but excluded from evaluation"}
  ]
}
```

Honesty note on `tail_number`: BTS records the tail that OPERATED the flight, post hoc. The event carries it as the stream key because rotation state needs it, but the consumer trusts only schedule-consistent linkages built from it; swap-shaped linkages null the whole rotation block. The linkage discipline, not the field itself, is what keeps this pre-departure-safe (CLAUDE.md section 3, linkage clause).

## 2. `flight.outcomes.v1` (topic; outcome fields `knowable_at: post_departure`)

Emitted late and out of order by design (truth arrives when the flight lands or cancels). Keyed like departures for partition affinity. Identity fields are `schedule`; everything else is `post_departure` and exists only for the evaluator, never for the scorer.

```json
{
  "type": "record",
  "name": "Outcome",
  "namespace": "flightdelay.v1",
  "fields": [
    {"name": "flight_date",       "type": {"type": "int", "logicalType": "date"},             "knowable_at": "schedule"},
    {"name": "carrier",           "type": "string",                                            "knowable_at": "schedule"},
    {"name": "flight_number",     "type": "string",                                            "knowable_at": "schedule"},
    {"name": "origin",            "type": "string",                                            "knowable_at": "schedule"},
    {"name": "dest",              "type": "string",                                            "knowable_at": "schedule"},
    {"name": "crs_dep_time",      "type": "string",                                            "knowable_at": "schedule"},
    {"name": "tail_number",       "type": ["null", "string"], "default": null,                 "knowable_at": "schedule"},
    {"name": "arr_del15",         "type": ["null", "boolean"], "default": null,                "knowable_at": "post_departure", "doc": "label; null when cancelled or diverted without arrival data"},
    {"name": "arr_delay_minutes", "type": ["null", "double"], "default": null,                 "knowable_at": "post_departure"},
    {"name": "cancelled",         "type": "boolean",                                           "knowable_at": "post_departure"},
    {"name": "diverted",          "type": "boolean",                                           "knowable_at": "post_departure"},
    {"name": "truth_ts_utc",      "type": {"type": "long", "logicalType": "timestamp-millis"}, "knowable_at": "post_departure", "doc": "when the outcome became known: actual arrival, or scheduled arrival for cancellations (approximation, disclosed); drives replay emission order and join lateness"}
  ]
}
```

## 3. `flight.delay_risk.v1` (topic; outputs `knowable_at: pre_departure_stream`)

One event per scored departure, defined at T = scheduled departure. `scored_at_ts_utc` equals T (event time, never wall clock) so replays are deterministic.

```json
{
  "type": "record",
  "name": "DelayRisk",
  "namespace": "flightdelay.v1",
  "fields": [
    {"name": "flight_date",          "type": {"type": "int", "logicalType": "date"},             "knowable_at": "schedule"},
    {"name": "carrier",              "type": "string",                                            "knowable_at": "schedule"},
    {"name": "flight_number",        "type": "string",                                            "knowable_at": "schedule"},
    {"name": "origin",               "type": "string",                                            "knowable_at": "schedule"},
    {"name": "dest",                 "type": "string",                                            "knowable_at": "schedule"},
    {"name": "crs_dep_time",         "type": "string",                                            "knowable_at": "schedule"},
    {"name": "tail_number",          "type": ["null", "string"], "default": null,                 "knowable_at": "schedule"},
    {"name": "scored_at_ts_utc",     "type": {"type": "long", "logicalType": "timestamp-millis"}, "knowable_at": "pre_departure_stream", "doc": "equals scheduled departure T"},
    {"name": "delay_probability",    "type": "double",                                            "knowable_at": "pre_departure_stream", "doc": "Platt-calibrated P(arrival delay >= 15 min)"},
    {"name": "risk_band",            "type": "string",                                            "knowable_at": "pre_departure_stream", "doc": "band edges from streaming/constants.py"},
    {"name": "alert",                "type": "boolean",                                           "knowable_at": "pre_departure_stream", "doc": "delay_probability >= threshold (0.5 primary)"},
    {"name": "model_run_id",         "type": "string",                                            "knowable_at": "pre_departure_stream", "doc": "artifact run under ml/artifacts/"},
    {"name": "calibration",          "type": "string", "default": "platt",                        "knowable_at": "pre_departure_stream"},
    {"name": "rotation_state_basis", "type": {"type": "enum", "name": "RotationBasis", "symbols": ["consistent", "clean_first", "swap_null", "warmup"]}, "knowable_at": "pre_departure_stream", "doc": "which linkage class the in-stream state machine assigned"},
    {"name": "weather_basis",        "type": {"type": "enum", "name": "WeatherBasis", "symbols": ["observed", "taf_forecast", "null_path"]}, "knowable_at": "pre_departure_stream"},
    {"name": "taf_horizon_bin",      "type": ["null", "string"], "default": null,                 "knowable_at": "pre_departure_stream", "doc": "0-3h / 3-12h / 12-30h when weather_basis = taf_forecast"},
    {"name": "pressure_late_arrivals",  "type": ["null", "int"], "default": null,                 "knowable_at": "pre_departure_stream", "doc": "trailing-window count of late arrivals at origin before T; ops context, see note"},
    {"name": "pressure_cancellations",  "type": ["null", "int"], "default": null,                 "knowable_at": "pre_departure_stream", "doc": "trailing-window cancellations at origin before T; ops context, see note"}
  ]
}
```

Note on the pressure fields: they are computed in-stream from events strictly before T (knowable at T, no tail linkage required) and carried as ops context beside the score. Implemented semantics (streaming/consumer.PressureIndex, window from `streaming/constants.PRESSURE_WINDOW_HOURS` = 3): late arrivals are outcomes with `arr_del15` true whose DEST is this origin; cancellations are cancelled outcomes whose ORIGIN is this origin; an outcome counts when its `truth_ts_utc` lies in `[T - window, T)` (left-inclusive, right-exclusive: truth at exactly T is simultaneous, not before). Cancellation truth time is the scheduled arrival, the outcomes producer's disclosed approximation. They are NOT inputs to the frozen 2024-06-30 model, whose 51-feature schema is hard-asserted before every prediction; they become model inputs only under a future retrain governed by the adoption rule (validation selects, test confirms once).

## 4. Alert artifact (`alerts.jsonl`; one JSON object per line)

Not a topic; the demoable ops artifact. Fields and their `knowable_at`:

| Field | Type | knowable_at | Note |
|---|---|---|---|
| flight_date, carrier, flight_number, origin, dest, crs_dep_time | as above | schedule | flight identity |
| delay_probability | number | pre_departure_stream | calibrated |
| risk_band | string | pre_departure_stream | |
| threshold | number | pre_departure_stream | the alerting threshold in force (0.5 primary) |
| issued_at | ISO-8601 UTC string | pre_departure_stream | equals T, event time |
| mode | string | schedule | replay or live |

## Registry conventions

- Subject naming: TopicNameStrategy (`flight.departures.v1-value`, etc.); the key is a plain string subject.
- Compatibility: BACKWARD on all subjects, set at registration, enforced by the registry, not by application code.
- Evolution: add fields with defaults only. A breaking change is a new topic (`.v2`), never an in-place mutation.
- The `knowable_at` property is carried in the schema JSON so the contract test can read it from the registry, not from a copy.
