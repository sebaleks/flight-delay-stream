"""The origin-pressure window: boundaries, airport routing, truth semantics."""

from __future__ import annotations

from streaming.constants import PRESSURE_WINDOW_HOURS
from streaming.consumer import PressureIndex

T0 = 1725235200000  # 2024-09-02T00:00Z, ms
HOUR = 3_600_000
W = PRESSURE_WINDOW_HOURS * HOUR


def outcome(dest="ORD", origin="SFO", truth=T0, late=True, cancelled=False):
    return {
        "origin": origin,
        "dest": dest,
        "arr_del15": late,
        "cancelled": cancelled,
        "truth_ts_utc": truth,
    }


def test_window_boundaries_left_inclusive_right_exclusive():
    idx = PressureIndex(
        [
            outcome(truth=T0 - W),      # exactly window-old: counts
            outcome(truth=T0 - W - 1),  # one ms older: out
            outcome(truth=T0 - 1),      # just before T: counts
            outcome(truth=T0),          # exactly T: simultaneous, out
        ]
    )
    late, canc = idx.counts("ORD", T0)
    assert (late, canc) == (2, 0)


def test_late_arrivals_key_on_dest_cancellations_on_origin():
    idx = PressureIndex(
        [
            outcome(dest="ORD", origin="SFO", truth=T0 - HOUR, late=True),
            outcome(dest="SFO", origin="ORD", truth=T0 - HOUR, late=True),
            outcome(dest="MIA", origin="ORD", truth=T0 - HOUR, cancelled=True, late=None),
            outcome(dest="ORD", origin="MIA", truth=T0 - HOUR, cancelled=True, late=None),
        ]
    )
    # scoring a departure FROM ORD: late arrivals INTO ORD, cancellations of
    # departures FROM ORD
    assert idx.counts("ORD", T0) == (1, 1)
    assert idx.counts("SFO", T0) == (1, 0)
    assert idx.counts("JFK", T0) == (0, 0)


def test_ontime_and_unlabeled_outcomes_never_count():
    idx = PressureIndex(
        [
            outcome(truth=T0 - HOUR, late=False),          # on time
            outcome(truth=T0 - HOUR, late=None),           # diverted, no label
        ]
    )
    assert idx.counts("ORD", T0) == (0, 0)


def test_empty_index_is_zero_not_null():
    assert PressureIndex([]).counts("ORD", T0) == (0, 0)
