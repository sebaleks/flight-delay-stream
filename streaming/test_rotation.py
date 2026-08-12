"""H3 state-machine tests: one case per linkage class, per swap trigger, plus
carrier-change reset, warm-up flagging, and the three debugging-session
semantics (per-tail windows, trunc-toward-zero gaps, the overlap taint).

Synthetic tails only; no Kafka, no files (day-leg counts injected directly).
"""

from __future__ import annotations

import math

import pytest

from streaming.constants import (
    LINK_CLASS_CLEAN_FIRST,
    LINK_CLASS_CONSISTENT,
    LINK_CLASS_SWAP,
)
from streaming.rotation import RotationTracker, basis_for

MIN = 60_000  # one minute in ms
DAY = "2024-09-02"
NEXT_DAY = "2024-09-03"


def ev(tail, dep_min, origin, dest, *, carrier="AA", date=DAY, elapsed=90.0, distance=500.0):
    return {
        "tail_number": tail,
        "carrier": carrier,
        "flight_date": date,
        "origin": origin,
        "dest": dest,
        "crs_dep_ts_ms": dep_min * MIN,
        "crs_elapsed_min": elapsed,
        "distance_mi": distance,
    }


def tracker(day_legs=None):
    return RotationTracker(day_legs if day_legs is not None else {})


def test_clean_first_leg():
    t = tracker({("N1", DAY): 3})
    r = t.observe(ev("N1", 0, "ORD", "DEN"))
    assert r.link_class == LINK_CLASS_CLEAN_FIRST and r.trigger is None
    assert r.features["rotation_position"] == 1.0
    assert r.features["legs_today"] == 3.0
    assert r.features["has_inbound_leg"] == 0.0
    assert r.features["is_tight_turnaround"] == 0.0  # coalesce(NULL < 35, false)
    assert math.isnan(r.features["sched_turnaround_min"])
    assert math.isnan(r.features["inbound_distance"])
    assert r.band_key == "no_inbound" and r.position_key == "1"


def test_consistent_inbound_full_block():
    t = tracker({("N1", DAY): 2})
    t.observe(ev("N1", 0, "ORD", "DEN", elapsed=90.0, distance=888.0))
    # prior arrives at minute 90; departing DEN at minute 135 -> turnaround 45
    r = t.observe(ev("N1", 135, "DEN", "SLC"))
    assert r.link_class == LINK_CLASS_CONSISTENT
    f = r.features
    assert f["rotation_position"] == 2.0 and f["legs_today"] == 2.0
    assert f["has_inbound_leg"] == 1.0
    assert f["sched_turnaround_min"] == 45.0
    assert f["sched_turnaround_slack_min"] == 10.0
    assert f["is_tight_turnaround"] == 0.0
    assert f["inbound_distance"] == 888.0
    assert f["inbound_crs_elapsed_min"] == 90.0
    assert r.band_key == "35_60" and r.position_key == "2"


def test_tight_turnaround_band():
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN"))
    r = t.observe(ev("N1", 110, "DEN", "SLC"))  # arr 90, dep 110 -> gap 20
    assert r.link_class == LINK_CLASS_CONSISTENT
    assert r.features["is_tight_turnaround"] == 1.0
    assert r.band_key == "lt_35"


def test_gap_truncates_toward_zero():
    # dep 30 s BEFORE scheduled arrival: -0.5 min truncates to 0, inside the
    # duty window — class a, not a negative-gap swap (BigQuery timestamp_diff)
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN", elapsed=90.0))
    r = t.observe({**ev("N1", 0, "DEN", "SLC"), "crs_dep_ts_ms": 90 * MIN - 30_000})
    assert r.link_class == LINK_CLASS_CONSISTENT
    assert r.features["sched_turnaround_min"] == 0.0


def test_negative_gap_is_swap():
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN"))
    r = t.observe(ev("N1", 88, "DEN", "SLC"))  # dep 2 min before arrival
    assert (r.link_class, r.trigger) == (LINK_CLASS_SWAP, "negative_gap")
    assert all(math.isnan(v) for v in r.features.values())
    assert r.band_key is None and r.position_key is None


def test_overlap_taint_fails_closed():
    # leg2's own gap is negative; leg3 looks clean against leg2's schedule but
    # must not treat a tainted leg as its inbound
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN"))
    t.observe(ev("N1", 88, "DEN", "SLC", elapsed=60.0))  # negative gap, arr 148
    r = t.observe(ev("N1", 200, "SLC", "PHX"))  # gap 52, continuity holds
    assert (r.link_class, r.trigger) == (LINK_CLASS_SWAP, "schedule_overlap")


def test_station_discontinuity_is_swap():
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN"))
    r = t.observe(ev("N1", 150, "MDW", "SLC"))  # aircraft "teleported" DEN->MDW
    assert (r.link_class, r.trigger) == (LINK_CLASS_SWAP, "station_discontinuity")


def test_unknown_tail_always_swap_and_stateless():
    t = tracker()
    for _ in range(2):
        r = t.observe(ev(None, 0, "ORD", "DEN"))
        assert (r.link_class, r.trigger) == (LINK_CLASS_SWAP, "unknown_tail")
    assert not t._tails


def test_unknown_prior_scheduled_arrival():
    t = tracker()
    first = t.observe(ev("N1", 0, "ORD", "DEN", elapsed=None))
    assert first.link_class == LINK_CLASS_CLEAN_FIRST  # elapsed-null leg itself
    r = t.observe(ev("N1", 150, "DEN", "SLC"))
    assert (r.link_class, r.trigger) == (LINK_CLASS_SWAP, "unknown_prior_scheduled_arrival")


def test_overnight_break_parked_here_is_clean_first():
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN"))  # arr minute 90
    r = t.observe(ev("N1", 90 + 900, "DEN", "SLC", date=NEXT_DAY))  # gap 900 > 840
    assert r.link_class == LINK_CLASS_CLEAN_FIRST
    assert r.features["rotation_position"] == 1.0  # new service date restarts
    assert r.band_key == "no_inbound"


def test_overnight_break_elsewhere_is_swap():
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN"))
    r = t.observe(ev("N1", 90 + 900, "MDW", "SLC", date=NEXT_DAY))
    assert (r.link_class, r.trigger) == (LINK_CLASS_SWAP, "station_discontinuity")


def test_carrier_change_resets_state():
    t = tracker()
    t.observe(ev("N1", 0, "ORD", "DEN", carrier="AA"))
    r = t.observe(ev("N1", 135, "DEN", "SLC", carrier="OO"))
    assert r.link_class == LINK_CLASS_CLEAN_FIRST  # not a consistent inbound
    assert r.features["rotation_position"] == 1.0


def test_position_caps_at_six_for_hist_key_only():
    t = tracker({("N1", DAY): 8})
    r = None
    for i in range(8):  # back-to-back 90-min legs, 60-min turnarounds
        org, dst = ("A", "B") if i % 2 == 0 else ("B", "A")
        r = t.observe(ev("N1", i * 150, org, dst))
    assert r.features["rotation_position"] == 8.0  # the feature keeps counting
    assert r.position_key == "6"  # the hist grain is capped


def test_day_legs_miss_floors_at_position():
    t = tracker({})
    t.observe(ev("N1", 0, "ORD", "DEN"))
    r = t.observe(ev("N1", 150, "DEN", "SLC"))
    assert r.features["legs_today"] == 2.0  # floor: legs_today >= position
    assert t.day_legs_misses == 2


def test_basis_mapping_and_warmup_precedence():
    assert basis_for(LINK_CLASS_CONSISTENT, False) == "consistent"
    assert basis_for(LINK_CLASS_CLEAN_FIRST, False) == "clean_first"
    assert basis_for(LINK_CLASS_SWAP, False) == "swap_null"
    for cls in (LINK_CLASS_CONSISTENT, LINK_CLASS_CLEAN_FIRST, LINK_CLASS_SWAP):
        assert basis_for(cls, True) == "warmup"
    with pytest.raises(KeyError):
        basis_for("z", False)
