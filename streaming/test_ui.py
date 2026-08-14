"""Cascade exposure: chain construction, band weighting, and total ordering."""

from __future__ import annotations

import random

from streaming.constants import CASCADE_TIGHTNESS_WEIGHTS
from streaming.ui import cascade_score, downstream_legs, rank_cascade

T0 = 1725235200000  # 2024-09-02T00:00Z, ms
HOUR = 3_600_000


def leg(tail="N1", ts=T0, p=0.5, band="60_120", date="2024-09-02", fn="100", origin="SFO",
        dest="LAX"):
    return {
        "tail_number": tail,
        "flight_date": date,
        "scored_at_ts_utc": ts,
        "delay_probability": p,
        "turnaround_band": band,
        "carrier": "AA",
        "flight_number": fn,
        "origin": origin,
        "dest": dest,
        "risk_band": "0.5-0.6",
        "alert": False,
        "model_run_id": "test",
        "rotation_state_basis": "consistent",
    }


def test_chain_is_later_legs_of_the_same_tail_and_day():
    events = [
        leg(tail="N1", ts=T0 + HOUR, fn="2"),
        leg(tail="N1", ts=T0, fn="1"),
        leg(tail="N1", ts=T0 + 2 * HOUR, fn="3"),
        leg(tail="N2", ts=T0 + HOUR, fn="9"),            # other aircraft
        leg(tail="N1", ts=T0 + HOUR, fn="8", date="2024-09-03"),  # other day
    ]
    chains = downstream_legs(events)
    # the 09-02 first leg of N1 sees exactly its two later legs
    first = next(i for i, e in enumerate(events) if e["flight_number"] == "1")
    assert [e["flight_number"] for e in chains[first]] == ["2", "3"]


def test_last_leg_of_the_day_has_no_exposure():
    events = [leg(ts=T0, fn="1"), leg(ts=T0 + HOUR, fn="2")]
    chains = downstream_legs(events)
    last = next(i for i, e in enumerate(events) if e["flight_number"] == "2")
    score, by_band, unknown = cascade_score(events[last], chains[last])
    assert (score, by_band, unknown) == (0.0, {}, 0)


def test_score_is_probability_times_summed_tightness():
    upstream = leg(ts=T0, p=0.5, fn="1")
    down = [leg(ts=T0 + HOUR, band="lt_35", fn="2"),
            leg(ts=T0 + 2 * HOUR, band="ge_120", fn="3")]
    score, by_band, unknown = cascade_score(upstream, down)
    expected = 0.5 * (CASCADE_TIGHTNESS_WEIGHTS["lt_35"] + CASCADE_TIGHTNESS_WEIGHTS["ge_120"])
    assert score == expected
    assert by_band == {"lt_35": 1, "ge_120": 1}
    assert unknown == 0


def test_unknown_band_is_counted_never_imputed():
    upstream = leg(ts=T0, p=1.0, fn="1")
    down = [leg(ts=T0 + HOUR, band=None, fn="2"),      # swap-shaped downstream
            leg(ts=T0 + 2 * HOUR, band="lt_35", fn="3")]
    score, by_band, unknown = cascade_score(upstream, down)
    assert unknown == 1
    assert by_band == {"lt_35": 1}
    assert score == CASCADE_TIGHTNESS_WEIGHTS["lt_35"]  # the null leg added nothing


def test_untailed_events_are_excluded_and_counted():
    events = [leg(tail=None, fn="1"), leg(tail="N1", fn="2"), leg(tail="N1", ts=T0 + HOUR,
                                                                  fn="3")]
    rows, no_tail = rank_cascade(events)
    assert no_tail == 1
    assert all(r["event"]["tail_number"] for r in rows)


def test_chain_is_the_real_itinerary_not_the_filtered_one():
    """The bug this pins: filtering before chaining splices an itinerary that
    never happened. ORD-BWI is really followed by BWI-MEM; if an origin filter
    drops the BWI leg first, ORD-BWI appears to be followed by ORD-MEM."""
    itinerary = [
        leg(tail="N1", ts=T0, origin="ORD", dest="BWI", fn="1", band="lt_35"),
        leg(tail="N1", ts=T0 + HOUR, origin="BWI", dest="MEM", fn="2", band="lt_35"),
        leg(tail="N1", ts=T0 + 2 * HOUR, origin="ORD", dest="MEM", fn="3", band="lt_35"),
    ]
    full = {r["event"]["flight_number"]: r for r in rank_cascade(itinerary)[0]}
    assert full["1"]["chain"] == ["BWI-MEM", "ORD-MEM"]
    assert full["1"]["downstream"] == 2

    # chaining the ORD-only subset would report one leg and the wrong route
    ord_only = [e for e in itinerary if e["origin"] == "ORD"]
    spliced = {r["event"]["flight_number"]: r for r in rank_cascade(ord_only)[0]}
    assert spliced["1"]["chain"] == ["ORD-MEM"]  # the false chain, never displayed
    assert spliced["1"]["score"] != full["1"]["score"]


def test_ordering_is_total_and_input_order_independent():
    events = [
        leg(tail=f"N{i}", ts=T0 + i * HOUR, p=0.5, fn=str(i), band="lt_35")
        for i in range(8)
    ] + [
        leg(tail=f"N{i}", ts=T0 + i * HOUR + HOUR, p=0.5, fn=f"{i}b", band="lt_35")
        for i in range(8)
    ]
    baseline = [r["event"]["flight_number"] for r in rank_cascade(events)[0]]
    for seed in (1, 2, 3):
        shuffled = events[:]
        random.Random(seed).shuffle(shuffled)
        assert [r["event"]["flight_number"] for r in rank_cascade(shuffled)[0]] == baseline
