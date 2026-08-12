"""In-stream rotation state machine: the tail-swap restriction, per event.

The H3 replacement for the consumer's stub. Events arrive keyed by tail in
scheduled-departure order; per tail this tracker maintains the running day
state and classifies every leg's linkage exactly as the batch twin
(streaming/rotation_batch.py, validated against the mart) and the retired SQL
(dbt/models/gold/shared/int_aircraft_rotation.sql) do:

- class a, consistent inbound: known prior scheduled arrival, station
  continuity, gap inside the duty window, untainted prior -> full block.
- class b, clean first leg: no prior leg in state (first seen, or carrier
  reset), or an overnight break (> duty window) parked at this origin ->
  position/legs kept, inbound fields NULL, band no_inbound.
- class c, swap-shaped: everything else -> EVERY rotation feature NULL
  including the band keys. The linkage itself is a day-of outcome
  (CLAUDE.md section 3, linkage clause).

The three semantics that cost a debugging session (docs/HANDOFF_PROMPTS.md):
per-tail windows (a tail's first leg has a NULL gap), gap minutes truncated
toward zero (BigQuery timestamp_diff), and the fail-closed overlap taint (a
leg whose OWN gap is negative must never serve as its successor's inbound).

legs_today is the full service-day leg count for the tail — schedule data,
knowable at booking, but not derivable from a forward-only stream at score
time. It comes from the committed replay schedule, the stream's stand-in for
the airline planning feed serving assumed (ml/serving.py header). Every
other state input is the event itself. The prior leg's ACTUAL times never
enter: schedule columns only.
"""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from streaming.constants import (
    DUTY_WINDOW_MAX_MINUTES,
    DUTY_WINDOW_MIN_MINUTES,
    LINK_CLASS_CLEAN_FIRST,
    LINK_CLASS_CONSISTENT,
    LINK_CLASS_SWAP,
    MIN_TURNAROUND_MINUTES,
    ROTATION_POSITION_CAP,
    turnaround_band,
)

REPO = Path(__file__).resolve().parents[1]

NULL_FEATURES: dict[str, float] = dict.fromkeys(
    (
        "rotation_position",
        "legs_today",
        "has_inbound_leg",
        "sched_turnaround_min",
        "sched_turnaround_slack_min",
        "is_tight_turnaround",
        "inbound_distance",
        "inbound_crs_elapsed_min",
    ),
    math.nan,
)


@dataclass
class Linkage:
    """One classified leg: the nine features plus the hist keys and the audit
    fields (class + swap trigger) the parity report groups by."""

    link_class: str
    trigger: str | None  # set only for class c, from SWAP_CLASS_TRIGGERS
    features: dict[str, float]
    band_key: str | None
    position_key: str | None


@dataclass
class _TailState:
    carrier: str
    flight_date: object
    position: int
    prior_dest: str
    prior_arr_ms: float | None  # scheduled arrival; None = unknown elapsed
    prior_distance: float
    prior_elapsed: float
    prior_own_gap_negative: bool


def load_day_leg_counts(
    path: Path = REPO / "data/replay/departures_week.parquet",
) -> dict[tuple[str, str], int]:
    """(tail, flight_date) -> the day's scheduled leg count, known-tail legs
    only — the same count the batch twin takes from the whole frame."""
    dep = pd.read_parquet(path, columns=["tail_number", "flight_date"])
    known = dep[dep["tail_number"].notna() & (dep["tail_number"] != "")]
    return known.groupby(["tail_number", "flight_date"]).size().to_dict()


class RotationTracker:
    def __init__(self, day_legs: dict[tuple[str, str], int]) -> None:
        self._day_legs = day_legs
        self._tails: dict[str, _TailState] = {}
        self.day_legs_misses = 0  # replay coverage is complete; count anyway

    def observe(self, ev: dict) -> Linkage:
        """Classify one departure event and advance the tail's state.

        Must be called exactly once per event, in per-tail scheduled-departure
        order (the partition order the tail key guarantees).
        """
        tail = ev.get("tail_number")
        if not tail:
            # sentinel-keyed: always swap-shaped, no state (training semantics
            # for unknown tails; docs/schemas.md keying note)
            return Linkage(LINK_CLASS_SWAP, "unknown_tail", dict(NULL_FEATURES), None, None)

        state = self._tails.get(tail)
        if state is not None and state.carrier != ev["carrier"]:
            # carrier change on the same tail resets the state (settled on
            # measured grounds: 0 multi-carrier tails in the week)
            state = None

        date = ev["flight_date"]
        position = 1 if state is None or state.flight_date != date else state.position + 1

        # the leg's own gap, computed whenever the prior's scheduled arrival is
        # known — independent of classification, because even a misclassified
        # leg's negative gap must taint its successor (fail-closed)
        gap: float | None = None
        if state is not None and state.prior_arr_ms is not None:
            # BigQuery timestamp_diff(minute) semantics: truncate toward zero
            gap = math.trunc((ev["crs_dep_ts_ms"] - state.prior_arr_ms) / 60_000.0)

        link_class, trigger = self._classify(state, ev, gap)
        legs = self._legs_today(tail, date, position)
        linkage = self._emit(link_class, trigger, state, position, legs, gap)

        elapsed = ev.get("crs_elapsed_min")
        dist = ev.get("distance_mi")
        self._tails[tail] = _TailState(
            carrier=ev["carrier"],
            flight_date=date,
            position=position,
            prior_dest=ev["dest"],
            prior_arr_ms=(
                None if elapsed is None else ev["crs_dep_ts_ms"] + float(elapsed) * 60_000.0
            ),
            prior_distance=math.nan if dist is None else float(dist),
            prior_elapsed=math.nan if elapsed is None else float(elapsed),
            prior_own_gap_negative=gap is not None and gap < 0,
        )
        return linkage

    def _classify(
        self, state: _TailState | None, ev: dict, gap: float | None
    ) -> tuple[str, str | None]:
        if state is None:
            return LINK_CLASS_CLEAN_FIRST, None
        if state.prior_arr_ms is None:
            return LINK_CLASS_SWAP, "unknown_prior_scheduled_arrival"
        if gap < 0:
            return LINK_CLASS_SWAP, "negative_gap"
        if state.prior_own_gap_negative:
            return LINK_CLASS_SWAP, "schedule_overlap"
        if state.prior_dest != ev["origin"]:
            return LINK_CLASS_SWAP, "station_discontinuity"
        if DUTY_WINDOW_MIN_MINUTES <= gap <= DUTY_WINDOW_MAX_MINUTES:
            return LINK_CLASS_CONSISTENT, None
        return LINK_CLASS_CLEAN_FIRST, None  # overnight break, parked here

    def _legs_today(self, tail: str, date: object, position: int) -> float:
        # the lookup is keyed by the parquet's ISO string; events carry a date
        key = date.isoformat() if isinstance(date, dt.date) else str(date)
        legs = self._day_legs.get((tail, key))
        if legs is None:
            # defensive only: the committed schedule covers every replay leg.
            # The floor keeps the training invariant legs_today >= position.
            self.day_legs_misses += 1
            legs = position
        return float(max(legs, position))

    def _emit(
        self,
        link_class: str,
        trigger: str | None,
        state: _TailState | None,
        position: int,
        legs: float,
        gap: float | None,
    ) -> Linkage:
        if link_class == LINK_CLASS_SWAP:
            return Linkage(link_class, trigger, dict(NULL_FEATURES), None, None)
        feats = dict(NULL_FEATURES)
        feats["rotation_position"] = float(position)
        feats["legs_today"] = legs
        pos_key = str(min(position, ROTATION_POSITION_CAP))
        if link_class == LINK_CLASS_CONSISTENT:
            t = float(gap)
            feats["has_inbound_leg"] = 1.0
            feats["sched_turnaround_min"] = t
            feats["sched_turnaround_slack_min"] = t - MIN_TURNAROUND_MINUTES
            feats["is_tight_turnaround"] = 1.0 if t < MIN_TURNAROUND_MINUTES else 0.0
            feats["inbound_distance"] = state.prior_distance
            feats["inbound_crs_elapsed_min"] = state.prior_elapsed
            return Linkage(link_class, None, feats, turnaround_band(True, t), pos_key)
        feats["has_inbound_leg"] = 0.0
        feats["is_tight_turnaround"] = 0.0  # the mart's coalesce(NULL < 35, false)
        return Linkage(link_class, None, feats, turnaround_band(False, None), pos_key)


def basis_for(link_class: str, is_warmup: bool) -> str:
    """The delay_risk rotation_state_basis enum value for one scored event.
    Warm-up flagging wins: those legs still build state and score, but the
    evaluator excludes them (streaming/evaluator.py excluded_warmup)."""
    if is_warmup:
        return "warmup"
    return {
        LINK_CLASS_CONSISTENT: "consistent",
        LINK_CLASS_CLEAN_FIRST: "clean_first",
        LINK_CLASS_SWAP: "swap_null",
    }[link_class]
