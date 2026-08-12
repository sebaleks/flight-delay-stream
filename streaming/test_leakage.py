"""H4 leakage suite: the boundary is demonstrated, not claimed.

Four kinds of proof (docs/HANDOFF_PROMPTS.md H4):
  a. swap parity — a swap-shaped link produces NULL cascade features at serve
     time exactly as in training, through the REAL enrichment path;
  b. post-departure exclusion — a forbidden column cannot reach the scorer:
     the frame gate raises on injection;
  c. guard-fails-when-violated — loosening a rule makes the tests FAIL,
     proving they detect violations rather than passing vacuously;
  d. contract — the registered departures and delay_risk schemas carry no
     post_departure field. Read from the live registry when it is up;
     otherwise the committed .avsc files stand in (auto-registration is OFF,
     so the registered copy can only ever be these bytes).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from ml import features as f
from streaming import constants as c
from streaming import rotation as rotation_module
from streaming.admin import registry_url
from streaming.consumer import SchemaMismatchError, build_frame, enrich, load_lookups
from streaming.rotation import RotationTracker

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
MIN = 60_000

ROTATION_FEATURES = [
    "rotation_position", "legs_today", "has_inbound_leg", "sched_turnaround_min",
    "sched_turnaround_slack_min", "is_tight_turnaround", "inbound_distance",
    "inbound_crs_elapsed_min",
]
ROTATION_HIST = [
    "hist_turnaround_band_arr_del15_rate", "hist_turnaround_band_avg_arr_delay_minutes",
    "hist_turnaround_band_n_flights", "hist_rotation_position_arr_del15_rate",
    "hist_rotation_position_avg_arr_delay_minutes", "hist_rotation_position_n_flights",
]


@pytest.fixture(scope="module")
def lookups():
    return load_lookups()


def _event(dep_min: int, origin: str, dest: str, tail: str | None = "N1") -> dict:
    import datetime as dt

    return {
        "tail_number": tail,
        "carrier": "AA",
        "flight_date": dt.date(2024, 9, 2),
        "origin": origin,
        "dest": dest,
        "crs_dep_time": "0900",
        "crs_arr_time": None,
        "crs_dep_ts_ms": 1725266000000 + dep_min * MIN,
        "crs_dep_ts_utc": None,
        "crs_elapsed_min": 90.0,
        "distance_mi": 500.0,
        "is_warmup": False,
    }


def _observe_swap(tracker: RotationTracker):
    """A synthetic tail with a station-continuity violation on leg 2."""
    tracker.observe(_event(0, "ORD", "DEN"))
    return tracker.observe(_event(150, "MDW", "SLC"))  # aircraft cannot be at MDW


# ---- a. swap parity through the real enrichment path -----------------------


def test_swap_shaped_link_nulls_every_rotation_feature(lookups):
    link = _observe_swap(RotationTracker({}))
    assert link.link_class == c.LINK_CLASS_SWAP
    row, _ = enrich(_event(150, "MDW", "SLC"), lookups, link)
    for col in ROTATION_FEATURES:
        assert math.isnan(row[col]), f"{col} must be NULL on a swap-shaped link"
    for col in ROTATION_HIST:
        assert math.isnan(row[col]), f"{col} must resolve to NaN (no band/position key)"
    assert link.band_key is None and link.position_key is None


# ---- b. post-departure exclusion at the frame gate --------------------------


def test_forbidden_columns_never_reach_the_scorer(lookups):
    row, _ = enrich(_event(0, "ORD", "DEN"), lookups)
    for forbidden in ("arr_delay", "dep_time", "cancelled", "inbound_arr_delay"):
        assert forbidden in c.FORBIDDEN_POST_DEPARTURE_FIELDS
        with pytest.raises(SchemaMismatchError):
            build_frame([{**row, forbidden: 12.0}], lookups, booster_names=list(f.FEATURES))


def test_no_forbidden_name_is_a_feature():
    assert not c.FORBIDDEN_POST_DEPARTURE_FIELDS & set(f.FEATURES)


# ---- c. the guards fail when a rule is violated -----------------------------


def _assert_overnight_is_clean_first() -> None:
    """The assertion the violation must break: an overnight break (> duty
    window) is a clean first leg, never a consistent inbound."""
    t = RotationTracker({})
    t.observe(_event(0, "ORD", "DEN"))
    link = t.observe(_event(90 + 900, "DEN", "SLC"))  # gap 900 > 840
    assert link.link_class == c.LINK_CLASS_CLEAN_FIRST
    assert math.isnan(link.features["sched_turnaround_min"])


def test_overnight_guard_holds_under_the_real_constants():
    _assert_overnight_is_clean_first()


def test_guard_fails_when_duty_window_is_widened(monkeypatch):
    # loosen the rule: a widened duty window turns the overnight break into a
    # "consistent" 900-minute turnaround — the guard above MUST now fail,
    # proving it detects violations rather than passing vacuously
    monkeypatch.setattr(rotation_module, "DUTY_WINDOW_MAX_MINUTES", 10_000)
    with pytest.raises(AssertionError):
        _assert_overnight_is_clean_first()


# ---- d. contract: no post_departure field in scorer-facing schemas ----------


def _schema_fields(subject: str, avsc: str) -> list[dict]:
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient

        sr = SchemaRegistryClient({"url": registry_url()})
        schema_str = sr.get_latest_version(subject).schema.schema_str
        source = f"registry subject {subject}"
    except Exception:  # registry down: the committed bytes are what registers
        schema_str = (SCHEMA_DIR / avsc).read_text()
        source = f"local {avsc} (registry unreachable; auto-registration is off)"
    fields = json.loads(schema_str)["fields"]
    print(f"contract test read {len(fields)} fields from {source}")
    return fields


@pytest.mark.parametrize(
    ("subject", "avsc"),
    [
        ("flight.departures.v1-value", "departure.avsc"),
        ("flight.delay_risk.v1-value", "delay_risk.avsc"),
    ],
)
def test_scorer_facing_contracts_carry_no_post_departure_field(subject, avsc):
    for field in _schema_fields(subject, avsc):
        assert field.get("knowable_at") != "post_departure", (
            f"{subject}: field {field['name']} is tagged post_departure"
        )
        assert field["name"] not in c.FORBIDDEN_POST_DEPARTURE_FIELDS, (
            f"{subject}: field {field['name']} is in the forbidden registry"
        )
