"""Batch twin of the rotation rule: schedule-consistency from raw schedules.

Builds the nine rotation features for a frame of flights using ONLY schedule
columns and the constants module — the same rule the dbt model defined and
H3's stream state machine enforces per event. Exists because the 2026 drift
window has no mart to read rotation from; the drift script validates this
builder against data/golden/rotation_reference_week.parquet (the mart's own
columns for the replay week) before trusting it on unseen months.

Semantics (streaming/constants.py; prose in dbt/models/gold/shared/
int_aircraft_rotation.sql):
- consistent inbound (class a): known prior scheduled arrival, station
  continuity, gap inside the duty window [0, 840] minutes -> full block.
- clean first leg (class b): no prior leg, or an overnight break (> 840 min)
  parked at this origin -> position/legs kept, inbound fields NULL,
  band no_inbound.
- swap-shaped (class c): negative gap (schedule overlap), continuity
  violation, unknown tail, or a prior with unknown scheduled arrival ->
  EVERY rotation feature NULL including the band keys.
Scheduled arrival = departure UTC + crs_elapsed minutes: timezone-proof and
immune to the local-midnight wrap, exactly as the mart built it.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from streaming.constants import (
    DUTY_WINDOW_MAX_MINUTES,
    DUTY_WINDOW_MIN_MINUTES,
    MIN_TURNAROUND_MINUTES,
    ROTATION_POSITION_CAP,
    turnaround_band,
)

REQUIRED = ["flight_date", "carrier", "flight_number", "origin", "dest",
            "crs_dep_time", "tail_number", "crs_elapsed_min", "distance_mi",
            "dep_ts_utc_ms"]


def build_rotation_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Adds the rotation feature columns to a copy of df (any row order).

    df needs the REQUIRED columns; dep_ts_utc_ms is the scheduled departure
    in UTC ms (producer-identical construction). Returns the frame in the
    original row order with: rotation_position, legs_today, has_inbound_leg,
    sched_turnaround_min, sched_turnaround_slack_min, is_tight_turnaround,
    inbound_distance, inbound_crs_elapsed_min, turnaround_band_key,
    rotation_position_key, link_class.
    """
    missing = set(REQUIRED) - set(df.columns)
    if missing:
        raise ValueError(f"rotation frame needs columns {sorted(missing)}")

    out = df.copy()
    out["_order"] = np.arange(len(out))
    known = out["tail_number"].notna() & (out["tail_number"] != "")
    # the mart's exact total order (int_aircraft_rotation.sql:103-113):
    # dep_ts_utc, then carrier, flight_number, origin, dest
    out = out.sort_values(
        ["tail_number", "dep_ts_utc_ms", "carrier", "flight_number", "origin", "dest"],
        kind="mergesort", na_position="last",
    )

    tail = out["tail_number"].where(known)
    same_tail = tail.eq(tail.shift()) & known & known.shift(fill_value=False)

    # day position within the BTS service date (the schedule-publication
    # convention the mart uses), counted over known-tail legs only
    grp_day = out.groupby([tail, out["flight_date"]], dropna=True, sort=False)
    out["_position"] = grp_day.cumcount() + 1
    out["_legs_today"] = grp_day["dep_ts_utc_ms"].transform("size")

    prior_arr_ms = (out["dep_ts_utc_ms"] + out["crs_elapsed_min"] * 60_000).shift()
    prior_dest = out["dest"].shift()
    prior_has_arr = out["crs_elapsed_min"].notna().shift(fill_value=False)
    # BigQuery timestamp_diff(..., minute) truncates toward zero: a -30 s gap
    # is 0 minutes (inside the window), not negative. Match it exactly. The
    # SQL's lag() is partitioned BY TAIL, so a tail's first leg has a NULL
    # gap; mask the cross-tail shift or that first leg computes a garbage
    # gap against another tail's arrival and taints its successor through
    # prior_overlapped.
    gap_min = np.trunc((out["dep_ts_utc_ms"] - prior_arr_ms) / 60_000.0).where(same_tail)

    # fail-closed taint (int_aircraft_rotation.sql:129-143): a leg whose OWN
    # gap is negative overlaps its predecessor, and its SUCCESSOR must not
    # treat it as a trustworthy inbound
    prior_overlapped = (gap_min < 0).shift(fill_value=False) & same_tail

    continuity = prior_dest.eq(out["origin"])
    in_window = gap_min.ge(DUTY_WINDOW_MIN_MINUTES) & gap_min.le(DUTY_WINDOW_MAX_MINUTES)
    overnight = gap_min.gt(DUTY_WINDOW_MAX_MINUTES)

    cls = pd.Series("c", index=out.index)  # default: swap-shaped
    no_prior = ~same_tail
    cls[known & no_prior] = "b"
    cls[known & same_tail & ~prior_has_arr] = "c"  # unknown prior scheduled arrival
    cls[known & same_tail & prior_has_arr & in_window & continuity & ~prior_overlapped] = "a"
    cls[known & same_tail & prior_has_arr & overnight & continuity & ~prior_overlapped] = "b"
    cls[~known] = "c"  # unknown tail
    out["link_class"] = cls

    a, b, c = cls.eq("a"), cls.eq("b"), cls.eq("c")
    out["rotation_position"] = out["_position"].where(~c)
    out["legs_today"] = out["_legs_today"].where(~c)
    out["has_inbound_leg"] = pd.Series(pd.NA, index=out.index, dtype="boolean")
    out.loc[a, "has_inbound_leg"] = True
    out.loc[b, "has_inbound_leg"] = False
    out["sched_turnaround_min"] = gap_min.where(a)
    out["sched_turnaround_slack_min"] = (
        out["sched_turnaround_min"] - MIN_TURNAROUND_MINUTES
    )
    out["is_tight_turnaround"] = pd.Series(pd.NA, index=out.index, dtype="boolean")
    out.loc[a, "is_tight_turnaround"] = (
        out.loc[a, "sched_turnaround_min"] < MIN_TURNAROUND_MINUTES
    )
    out.loc[b, "is_tight_turnaround"] = False  # coalesce(NULL < 35, false)
    out["inbound_distance"] = out["distance_mi"].shift().where(a)
    out["inbound_crs_elapsed_min"] = out["crs_elapsed_min"].shift().where(a)

    out["turnaround_band_key"] = [
        None if is_c else turnaround_band(bool(hi) if hi is not pd.NA else False,
                                          None if pd.isna(t) else float(t))
        for is_c, hi, t in zip(
            c, out["has_inbound_leg"], out["sched_turnaround_min"], strict=True
        )
    ]
    out["rotation_position_key"] = [
        None if (is_c or pd.isna(p)) else str(int(min(p, ROTATION_POSITION_CAP)))
        for is_c, p in zip(c, out["_position"], strict=True)
    ]

    return (
        out.sort_values("_order")
        .drop(columns=["_order", "_position", "_legs_today"])
        .reset_index(drop=True)
    )
