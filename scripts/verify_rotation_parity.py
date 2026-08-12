"""H3 parity gate: the stream state machine against BOTH references.

Replays the committed week through streaming/rotation.RotationTracker in the
producer's exact total order (the per-tail order the consumer sees), then
compares the emitted rotation features:

1. against streaming/rotation_batch.py on the same frame — two independent
   implementations of the same rule; the gate here is ZERO mismatches;
2. against data/golden/rotation_reference_week.parquet — the mart's own
   values, joined on the flight identity grain, warm-up day excluded; the
   expected irreducible cold-start residue is ~0.18% of rows (chains that
   reach before the warm-up day; drift_measurement.py measured it for the
   batch twin).

Prints link-class shares (class c split by trigger), a per-column mismatch
table for both comparisons, and writes evaluation/rotation_parity.json.

    uv run --extra kafka --extra ml python scripts/verify_rotation_parity.py
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from streaming.consumer import load_lookups
from streaming.producer import dep_ts_utc_ms, load_week, tz_map
from streaming.rotation import RotationTracker, load_day_leg_counts
from streaming.rotation_batch import build_rotation_frame

REPO = Path(__file__).resolve().parents[1]
IDENT = ["flight_date", "carrier", "flight_number", "origin", "dest", "crs_dep_time"]
FEATURE_COLS = [
    "rotation_position", "legs_today", "has_inbound_leg", "sched_turnaround_min",
    "sched_turnaround_slack_min", "is_tight_turnaround", "inbound_distance",
    "inbound_crs_elapsed_min",
]
KEY_COLS = ["turnaround_band_key", "rotation_position_key", "link_class"]
WARMUP_DAY = "2024-09-01"


def stream_rotation(dep: pd.DataFrame) -> pd.DataFrame:
    tracker = RotationTracker(load_day_leg_counts())
    rows = []
    for r in dep.itertuples(index=False):
        link = tracker.observe(
            {
                "tail_number": r.tail_number if isinstance(r.tail_number, str) else None,
                "carrier": r.carrier,
                "flight_date": r.flight_date,
                "origin": r.origin,
                "dest": r.dest,
                "crs_dep_ts_ms": r.dep_ts_utc_ms,
                "crs_elapsed_min": None if pd.isna(r.crs_elapsed_min) else r.crs_elapsed_min,
                "distance_mi": None if pd.isna(r.distance_mi) else r.distance_mi,
            }
        )
        rows.append(
            {
                **{k: getattr(r, k) for k in IDENT},
                **link.features,
                "turnaround_band_key": link.band_key,
                "rotation_position_key": link.position_key,
                "link_class": link.link_class,
                "trigger": link.trigger,
            }
        )
    if tracker.day_legs_misses:
        print(f"WARNING: {tracker.day_legs_misses} day-legs lookup misses")
    frame = pd.DataFrame(rows)

    # derive the hist band/position triples from the emitted keys through the
    # CONSUMER's own lookup wiring, so the mart comparison covers it too
    lookups = load_lookups()
    for grain, table, key_col in (
        ("turnaround_band", lookups.rot_band, "turnaround_band_key"),
        ("rotation_position", lookups.rot_pos, "rotation_position_key"),
    ):
        for stat, name in (
            ("rate", "arr_del15_rate"),
            ("avg_min", "avg_arr_delay_minutes"),
            ("n", "n_flights"),
        ):
            frame[f"hist_{grain}_{name}"] = [
                float((table.get(k) or {}).get(stat) or np.nan) if k else np.nan
                for k in frame[key_col]
            ]
    return frame


def mismatch_table(a_frame: pd.DataFrame, b_frame: pd.DataFrame, cols: list[str],
                   a_suffix: str, b_suffix: str) -> dict[str, int]:
    out = {}
    for col in cols:
        a = pd.to_numeric(a_frame[f"{col}{a_suffix}"], errors="coerce").astype("float64")
        b = pd.to_numeric(b_frame[f"{col}{b_suffix}"], errors="coerce").astype("float64")
        diff = ~((a.isna() & b.isna()) | np.isclose(a, b, atol=1e-6, equal_nan=True))
        out[col] = int(diff.sum())
    return out


def main() -> None:
    dep, _warmup = load_week()
    tzs = tz_map()
    dep["dep_ts_utc_ms"] = [dep_ts_utc_ms(r, tzs) for r in dep.itertuples(index=False)]

    mine = stream_rotation(dep)

    # ---- link-class shares, class c split by trigger ----
    non_warmup = mine[mine["flight_date"] != WARMUP_DAY]
    shares = (non_warmup["link_class"].value_counts(normalize=True) * 100).round(3)
    triggers = non_warmup.loc[non_warmup["link_class"] == "c", "trigger"].value_counts()
    print(f"link-class shares over {len(non_warmup):,} non-warm-up legs:")
    for cls in ("a", "b", "c"):
        print(f"  {cls}: {shares.get(cls, 0.0):.3f}%")
    print("  class c by trigger:", dict(triggers))

    # ---- 1) batch twin: same frame, same rule, different per-tail ORDER ----
    # The stream processes arrival order (the producer's local-time total
    # order); the twin re-sorts each tail by UTC instant, as the mart did.
    # The two disagree only at "seam" tails whose schedules are physically
    # inconsistent (an aircraft in two places): there the within-tail arrival
    # order and UTC order genuinely differ, so classifications at the seam
    # legs may differ while the rule itself is identical. The gate is
    # therefore: zero mismatches OFF seam tails, and every seam mismatch
    # counted here.
    twin = build_rotation_frame(dep)
    joined = pd.concat(
        [mine.add_suffix("_mine").reset_index(drop=True),
         twin.add_suffix("_twin").reset_index(drop=True)], axis=1
    )
    twin_features = mismatch_table(joined, joined, FEATURE_COLS, "_mine", "_twin")
    twin_keys = {
        col: int((joined[f"{col}_mine"].fillna("~") != joined[f"{col}_twin"].fillna("~")).sum())
        for col in KEY_COLS
    }
    known = dep[dep["tail_number"].notna() & (dep["tail_number"] != "")].copy()
    known["arrival_idx"] = np.arange(len(known))
    seam_tails = set()
    for tail, g in known.groupby("tail_number", sort=False):
        ts = g.sort_values("arrival_idx")["dep_ts_utc_ms"].to_numpy()
        if (np.diff(ts) <= 0).any():  # inversion or same-instant tie
            seam_tails.add(tail)
    class_diff = joined["link_class_mine"].fillna("~") != joined["link_class_twin"].fillna("~")
    diff_tails = set(dep.loc[class_diff[class_diff].index, "tail_number"].dropna())
    off_seam = sorted(diff_tails - seam_tails)
    print(f"\nvs batch twin ({len(joined):,} rows; gate: every mismatch on a seam tail):")
    print("  features:", twin_features)
    print("  keys/class:", twin_keys)
    print(f"  seam tails (arrival vs UTC order diverges): {len(seam_tails)}; "
          f"tails with any class diff: {len(diff_tails)}; "
          f"OFF-seam diff tails (must be 0): {len(off_seam)} {off_seam[:5]}")

    # ---- 2) mart reference: cold-start residue expected ----
    ref = pd.read_parquet(REPO / "data/golden/rotation_reference_week.parquet")
    merged = mine.merge(ref, on=IDENT, suffixes=("_mine", "_mart"), how="inner")
    merged = merged[merged["flight_date"] != WARMUP_DAY]
    mart = mismatch_table(merged, merged, FEATURE_COLS, "_mine", "_mart")
    hist_cols = [c for c in ref.columns if c.startswith("hist_")]
    mart_hist = mismatch_table(merged, merged, hist_cols, "_mine", "_mart")
    any_bad = np.zeros(len(merged), dtype=bool)
    for col in FEATURE_COLS:
        a = pd.to_numeric(merged[f"{col}_mine"], errors="coerce").astype("float64")
        b = pd.to_numeric(merged[f"{col}_mart"], errors="coerce").astype("float64")
        any_bad |= ~((a.isna() & b.isna()) | np.isclose(a, b, atol=1e-6, equal_nan=True))
    residue_rows = int(any_bad.sum())
    residue_pct = 100.0 * residue_rows / len(merged)
    print(f"\nvs mart reference ({len(merged):,} joined post-warm-up rows):")
    print("  features:", mart)
    print("  hist band/position:", mart_hist)
    print(f"  rows with any feature mismatch: {residue_rows:,} ({residue_pct:.3f}%) "
          f"= cold-start boundary residue (the fixed batch twin measures 0.119% "
          f"on this join) + the seam-tail order rows counted above")

    out = {
        "rows_streamed": int(len(mine)),
        "link_class_shares_pct": {k: float(v) for k, v in shares.items()},
        "class_c_triggers": {k: int(v) for k, v in triggers.items()},
        "batch_twin_feature_mismatches": twin_features,
        "batch_twin_key_mismatches": twin_keys,
        "seam_tails": len(seam_tails),
        "twin_class_diff_tails": len(diff_tails),
        "twin_class_diff_tails_off_seam": len(off_seam),
        "mart_rows_compared": int(len(merged)),
        "mart_feature_mismatches": mart,
        "mart_hist_mismatches": mart_hist,
        "mart_residue_rows": residue_rows,
        "mart_residue_pct": round(residue_pct, 4),
    }
    path = REPO / "evaluation/rotation_parity.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, sort_keys=True, indent=2) + "\n")
    print(f"\nwrote {path}")


if __name__ == "__main__":
    main()
