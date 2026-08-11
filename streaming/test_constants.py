"""Pin streaming/constants.py to the 681 sources it was lifted from.

Every test parses the ORIGINAL definition site and fails if the constants
module drifts from it. When the deletion audit removes a parsed source, its
test skips with a reason rather than failing: from that point the constants
module is the sole definition (see the module docstring).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from streaming import constants as c

REPO = Path(__file__).resolve().parents[1]

ROTATION_SQL = REPO / "dbt/models/gold/shared/int_aircraft_rotation.sql"
MART_SQL = REPO / "dbt/models/gold/ml/ml_flight_features.sql"
DBT_PROJECT = REPO / "dbt/dbt_project.yml"
SERVING_PY = REPO / "ml/serving.py"
API_PY = REPO / "ml/api.py"
PLAN_MD = REPO / "docs/PLAN.md"


def _src(path: Path) -> str:
    if not path.exists():
        pytest.skip(f"{path.name} deleted by the audit; constants.py is now the sole definition")
    return path.read_text()


def test_duty_window_matches_rotation_sql():
    src = _src(ROTATION_SQL)
    lo, hi = c.DUTY_WINDOW_MIN_MINUTES, c.DUTY_WINDOW_MAX_MINUTES
    assert f"between {lo} and {hi}" in src, "consistent-link duty window drifted"
    assert f"raw_gap_min > {hi}" in src, "overnight-break threshold drifted"


def test_min_turnaround_matches_rotation_sql():
    src = _src(ROTATION_SQL)
    m = c.MIN_TURNAROUND_MINUTES
    assert f"sched_turnaround_min - {m} as sched_turnaround_slack_min" in src
    assert f"sched_turnaround_min < {m}" in src  # is_tight_turnaround + lt band


def test_band_edges_and_labels_match_rotation_sql():
    src = _src(ROTATION_SQL)
    e1, e2, e3 = c.TURNAROUND_BAND_EDGES_MINUTES
    for edge in (e1, e2, e3):
        assert f"sched_turnaround_min < {edge}" in src, f"band edge {edge} drifted"
    for label in c.TURNAROUND_BANDS:
        assert f"'{label}'" in src, f"band label {label} missing from the SQL"


def test_position_cap_matches_rotation_sql():
    src = _src(ROTATION_SQL)
    assert f"least(rotation_position, {c.ROTATION_POSITION_CAP})" in src


def test_link_classes_match_rotation_sql():
    src = _src(ROTATION_SQL)
    for cls in (c.LINK_CLASS_CONSISTENT, c.LINK_CLASS_CLEAN_FIRST, c.LINK_CLASS_SWAP):
        assert f"then '{cls}'" in src or f"= '{cls}'" in src or f"else '{cls}'" in src


def test_hist_smoothing_matches_dbt_project():
    src = _src(DBT_PROJECT)
    assert f"hist_smoothing_prior_strength: {c.HIST_SMOOTHING_PRIOR_STRENGTH}" in src


def test_weather_staleness_matches_mart_sql():
    src = _src(MART_SQL)
    assert f"interval {c.WEATHER_STALENESS_HOURS} hour" in src


def test_serving_duty_window_mirror():
    src = _src(SERVING_PY)
    assert f"{c.DUTY_WINDOW_MIN_MINUTES} <= t <= {c.DUTY_WINDOW_MAX_MINUTES}" in src


def test_api_duty_window_mirror():
    src = _src(API_PY)
    assert f"le={c.DUTY_WINDOW_MAX_MINUTES}" in src


def test_turnaround_band_agrees_with_serving_mirror():
    serving = pytest.importorskip("ml.serving", reason="ml extras not installed")
    grid = [None, -5.0, 0.0, 1.0, 34.9, 35.0, 59.9, 60.0, 119.9, 120.0, 500.0]
    for has_inbound in (True, False):
        for t in grid:
            assert c.turnaround_band(has_inbound, t) == serving._turnaround_band(
                has_inbound, t
            ), f"band mismatch at has_inbound={has_inbound}, t={t}"


def test_band_tuple_shapes():
    # no_inbound + one band per edge interval (below first, between each, above last)
    assert len(c.TURNAROUND_BANDS) == len(c.TURNAROUND_BAND_EDGES_MINUTES) + 2
    assert c.TURNAROUND_BAND_EDGES_MINUTES == tuple(sorted(c.TURNAROUND_BAND_EDGES_MINUTES))
    assert c.TURNAROUND_BAND_EDGES_MINUTES[0] == c.MIN_TURNAROUND_MINUTES


def test_forbidden_fields_cover_the_ml_registry():
    import ml.features as f

    forbidden = c.FORBIDDEN_POST_DEPARTURE_FIELDS
    assert set(f.FORBIDDEN_FEATURES) <= forbidden
    assert set(f.LABELS) <= forbidden
    assert c.KNOWABLE_POST_DEPARTURE_EVENT_FIELDS <= forbidden
    # no feature the model consumes is post-departure
    assert not (set(f.FEATURES) & forbidden)


def test_knowable_sets_are_disjoint():
    assert not (c.KNOWABLE_SCHEDULE & c.FORBIDDEN_POST_DEPARTURE_FIELDS)
    assert not (c.KNOWABLE_SCHEDULE & c.KNOWABLE_PRE_DEPARTURE_STREAM)
    assert not (c.KNOWABLE_PRE_DEPARTURE_STREAM & c.FORBIDDEN_POST_DEPARTURE_FIELDS)


def test_alert_threshold_matches_the_recorded_decision():
    src = _src(PLAN_MD)
    assert f"p >= {c.ALERT_THRESHOLD}" in src, "kickoff decision 4 drifted from constants"
    assert 0.0 < c.ALERT_THRESHOLD < 1.0


def test_swap_triggers_enumerated():
    # the five class-c triggers recorded from int_aircraft_rotation.sql's
    # header; H3's state machine must handle every one of them
    assert c.SWAP_CLASS_TRIGGERS == {
        "negative_gap",
        "station_discontinuity",
        "schedule_overlap",
        "unknown_tail",
        "unknown_prior_scheduled_arrival",
    }


def test_bases_match_schema_doc():
    src = _src(REPO / "docs/schemas.md")
    for basis in c.ROTATION_STATE_BASES + c.WEATHER_BASES:
        assert basis in src, f"basis {basis} missing from docs/schemas.md"
    assert c.NULL_TAIL_SENTINEL_KEY in src
