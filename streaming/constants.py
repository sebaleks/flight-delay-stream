"""Single source of truth for every leakage constant (CLAUDE.md section 2).

The batch-side tests and the streaming consumer assertions BOTH import these
values; restating one anywhere else is a defect. One rule, two enforcement
points, no drift.

Each value cites the 681 source it was lifted from (file:line as of
2026-08-11). streaming/test_constants.py parses those sources and fails if
this module drifts from them. When the deletion audit removes the dbt files,
this module becomes the sole definition and those tests retire with the
sources they parse.
"""

from __future__ import annotations

# The forbidden-column registry stays canonical in ml/features.py (a kept
# module); this module re-exposes it so consumers import ONE place.
from ml.features import FORBIDDEN_FEATURES as _ML_FORBIDDEN
from ml.features import LABELS as _ML_LABELS

# --- Rotation linkage (the tail-swap restriction) ---------------------------

# A schedule-consistent inbound link requires the gap from the prior leg's
# scheduled arrival to this leg's scheduled departure to sit inside the duty
# window. Source: dbt/models/gold/shared/int_aircraft_rotation.sql:169
# ("raw_gap_min between 0 and 840"; >840 at :172 is an overnight break, a
# clean first leg). Serve-time mirrors: ml/serving.py:463 ("0 <= t <= 840"),
# ml/api.py:66 (Field ge=0, le=840).
DUTY_WINDOW_MIN_MINUTES = 0
DUTY_WINDOW_MAX_MINUTES = 840

# Typical narrow-body minimum turnaround; the slack subtrahend and the tight
# threshold. Source: int_aircraft_rotation.sql:200
# ("sched_turnaround_min - 35 as sched_turnaround_slack_min") and :203
# ("sched_turnaround_min < 35"); serve-time mirror ml/serving.py:493-494.
MIN_TURNAROUND_MINUTES = 35

# Turnaround band edges and labels for the shared historical rates.
# Source: int_aircraft_rotation.sql:210-217 (< 35 / < 60 / < 120 / else);
# mirrors: ml/serving.py:335-344 (_turnaround_band),
# dbt/models/gold/ml/serving_entity_profile.sql:108-111.
TURNAROUND_BAND_EDGES_MINUTES = (35, 60, 120)
TURNAROUND_BANDS = ("no_inbound", "lt_35", "35_60", "60_120", "ge_120")

# Rotation-position hist grain cap. Source: int_aircraft_rotation.sql:218
# ("least(rotation_position, 6)").
ROTATION_POSITION_CAP = 6

# Linkage classes. Source: int_aircraft_rotation.sql:163-176 (link_class
# a/b/c) and the header block :110-140. Class c (swap-shaped) nulls EVERY
# rotation feature including the band keys.
LINK_CLASS_CONSISTENT = "a"
LINK_CLASS_CLEAN_FIRST = "b"
LINK_CLASS_SWAP = "c"

# What makes a linkage swap-shaped (class c): the 'else' arm of
# int_aircraft_rotation.sql:163-176, enumerated in its header (:128-133).
# A prior leg with unknown scheduled arrival is class c, NOT a clean first
# leg (:164-167). Unknown-tail legs are always class c (:224).
SWAP_CLASS_TRIGGERS = frozenset(
    {
        "negative_gap",  # raw_gap_min < 0
        "station_discontinuity",  # prior leg's dest != this leg's origin
        "schedule_overlap",  # prior leg schedule-overlaps this one
        "unknown_tail",  # tail_number NULL (0.34% of legs)
        "unknown_prior_scheduled_arrival",  # elapsed-null prior leg
    }
)

# Null-tail events ride this sentinel partition key and are always scored
# with the class-c NULL rotation block. Source: docs/schemas.md ("Keying and
# partitioning").
NULL_TAIL_SENTINEL_KEY = "NOTAIL"


def turnaround_band(has_inbound: bool, turnaround_min: float | None) -> str:
    """The band derivation, identical to ml/serving.py:_turnaround_band and
    int_aircraft_rotation.sql:210-217. Defined HERE so stream-side code has
    no reason to restate the edges; test_constants pins it against the
    serving mirror value-for-value."""
    if not has_inbound or turnaround_min is None:
        return TURNAROUND_BANDS[0]
    if turnaround_min < TURNAROUND_BAND_EDGES_MINUTES[0]:
        return TURNAROUND_BANDS[1]
    if turnaround_min < TURNAROUND_BAND_EDGES_MINUTES[1]:
        return TURNAROUND_BANDS[2]
    if turnaround_min < TURNAROUND_BAND_EDGES_MINUTES[2]:
        return TURNAROUND_BANDS[3]
    return TURNAROUND_BANDS[4]


# --- Weather timing ---------------------------------------------------------

# Origin weather is the LAST observation at or before scheduled departure,
# inside this lookback ceiling. Source: dbt/models/gold/ml/
# ml_flight_features.sql:246-250 ("interval 3 hour" lookback window) and its
# header :56-59; guarded by dbt/tests/assert_ml_weather_obs_before_departure.
WEATHER_STALENESS_HOURS = 3

# --- Historical-rate smoothing ----------------------------------------------

# hist = (n*entity_rate + m*global_rate) / (n + m). Source:
# dbt/dbt_project.yml:54 (hist_smoothing_prior_strength: 50) — the SOLE
# definition anywhere; serving reads VALUES, never this formula. Recorded
# here so the constant survives the dbt deletion.
HIST_SMOOTHING_PRIOR_STRENGTH = 50

# --- Alerting ---------------------------------------------------------------

# Calibrated p >= threshold raises an alert: "more likely delayed than not".
# Source: docs/PLAN.md kickoff decision 4 (sensitivity reported at 0.3/0.7,
# never tuned on the replay week).
ALERT_THRESHOLD = 0.5

# --- Risk banding -----------------------------------------------------------

# delay_risk.risk_band is the calibrated probability's decile bucket, rendered
# "0.5-0.6" (top band "0.9-1.0", inclusive at 1.0). Source: docs/schemas.md
# contract 3 ("band edges from streaming/constants.py") and the evaluator's
# fixture shape (streaming/test_evaluator.py). Banding is presentation only;
# alerting compares the calibrated p to ALERT_THRESHOLD directly.
RISK_BAND_WIDTH = 0.1


def risk_band(p: float) -> str:
    """Decile band label for a calibrated probability in [0, 1]."""
    lo = min(int(p * 10), 9)
    return f"{lo / 10:.1f}-{(lo + 1) / 10:.1f}"


# --- knowable_at field sets (docs/schemas.md) -------------------------------

# Fields knowable at booking from the published schedule. The departures
# event may contain ONLY these.
KNOWABLE_SCHEDULE = frozenset(
    {
        "flight_date",
        "carrier",
        "flight_number",
        "origin",
        "dest",
        "crs_dep_time",
        "crs_dep_ts_utc",
        "crs_arr_time",
        "crs_elapsed_min",
        "distance_mi",
        "tail_number",
        "mode",
        "is_warmup",
    }
)

# Fields derived in-stream strictly before the scheduled departure time T.
KNOWABLE_PRE_DEPARTURE_STREAM = frozenset(
    {
        "scored_at_ts_utc",
        "delay_probability",
        "risk_band",
        "alert",
        "model_run_id",
        "calibration",
        "rotation_state_basis",
        "weather_basis",
        "taf_horizon_bin",
        "pressure_late_arrivals",
        "pressure_cancellations",
        "threshold",
        "issued_at",
    }
)

# Realized outcomes: never a model input, never in the departures or
# delay_risk contracts. Event-level fields (docs/schemas.md outcomes topic)
# plus the full ml/features.py forbidden-column registry and the labels.
KNOWABLE_POST_DEPARTURE_EVENT_FIELDS = frozenset(
    {
        "arr_del15",
        "arr_delay_minutes",
        "cancelled",
        "diverted",
        "truth_ts_utc",
        "dep_time",
        "arr_time",
    }
)

FORBIDDEN_POST_DEPARTURE_FIELDS = (
    KNOWABLE_POST_DEPARTURE_EVENT_FIELDS | frozenset(_ML_FORBIDDEN) | frozenset(_ML_LABELS)
)

# Bases reported on every scored event (docs/schemas.md delay_risk contract).
ROTATION_STATE_BASES = ("consistent", "clean_first", "swap_null", "warmup")
WEATHER_BASES = ("observed", "taf_forecast", "null_path")
