"""H5: the forecast-for-observation substitution cost, per horizon.

The model was trained on OBSERVED weather (last ISD reading at or before
scheduled departure). At serve time only a FORECAST exists. This study
rescores the replay week with TAF-substituted weather and reports the cost
next to the observed-weather baseline, per forecast horizon — the project's
second core finding.

Method (docs/HANDOFF_PROMPTS.md H5):
- Parse the IEM TAF export into prevailing forecast groups (TEMPO/vicinity
  refinements excluded — a TEMPO fluctuation window and a VC 'in the
  vicinity' phenomenon are not station-hour prevailing conditions).
- Per departure: the latest TAF issued at or before scheduled departure
  (standing guard, asserted) whose prevailing group covers the scheduled
  departure HOUR — falling back one issuance when a mid-hour TAF's first
  group starts after that hour. Horizon = departure minus issuance,
  binned 0-3h / 3-12h / 12-30h.
- 8 of the 12 weather features are mappable (wind, gust + indicator,
  visibility [P6SM encoded 6.01], fog/rain/snow/thunder from presentwx,
  matched to the silver ISD code semantics: mist counts as fog). Temp,
  dewpoint, and precip go NaN — part of the measured cost, never imputed.
- Rescore the week with everything else identical (same rotation state, same
  frame gates, same frozen model), join outcomes, report ROC-AUC / PR-AUC /
  ECE per bin against the observed baseline on the same rows.
- Evaluate the pre-registered trigger: short-bin (0-3h) PR-AUC degradation
  > 0.010 means representation mismatch; the first response is
  harmonization, not retraining (docs/PLAN.md decision 5). Evaluated only.

    uv run --extra kafka --extra ml python scripts/taf_substitution_study.py
"""

from __future__ import annotations

import ast
import bisect
import datetime as dt
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from streaming.consumer import build_frame, enrich, load_lookups, load_scoring_artifacts
from streaming.evaluator import _ece, _pr_auc
from streaming.producer import dep_ts_utc_ms, load_week, tz_map
from streaming.rotation import RotationTracker, load_day_leg_counts

REPO = Path(__file__).resolve().parents[1]
IDENT = ["flight_date", "carrier", "flight_number", "origin", "dest", "crs_dep_time"]
WARMUP_DAY = "2024-09-01"
HORIZON_BINS = (("0-3h", 0, 3), ("3-12h", 3, 12), ("12-30h", 12, 30))
MAX_HORIZON_MS = 30 * 3_600_000
TRIGGER_PR_AUC_DROP = 0.010
CHUNK = 20_000

WEATHER_COLS = [
    "origin_temp_f", "origin_dewpoint_f", "origin_wind_speed_kn", "origin_gust_kn",
    "origin_gust_reported", "origin_visibility_mi", "origin_precip_1h_in",
    "origin_had_fog", "origin_had_rain_drizzle", "origin_had_snow_ice_pellets",
    "origin_had_thunder", "has_origin_weather",
]


def _wx_flags(presentwx: str) -> tuple[float, float, float, float]:
    """fog / rain-drizzle / snow-ice / thunder from TAF phenomena tokens,
    mirroring the silver ISD code semantics (silver_isd_hourly.sql:184-211):
    BR (mist) counts as fog; VC 'vicinity' tokens are not at-station."""
    try:
        tokens = ast.literal_eval(presentwx) if isinstance(presentwx, str) else []
    except (ValueError, SyntaxError):
        tokens = []
    fog = rain = snow = thunder = 0.0
    for token in tokens:
        t = token.lstrip("+-")
        if t.startswith("VC"):
            continue
        if "FG" in t or t == "BR":
            fog = 1.0
        if "RA" in t or "DZ" in t:
            rain = 1.0
        if any(s in t for s in ("SN", "SG", "PL", "IC", "GS", "GR")):
            snow = 1.0
        if "TS" in t:
            thunder = 1.0
    return fog, rain, snow, thunder


def load_taf_groups() -> dict[str, list]:
    """station -> sorted list of (issued_ms, [group fx_ms array], [feature rows]).

    One entry per issuance (amendments are issuances and supersede naturally
    by time order); groups are the prevailing (non-TEMPO) segments sorted by
    start time."""
    taf = pd.read_csv(REPO / "data/weather/taf_week.csv", low_memory=False)
    taf = taf[~taf["is_tempo"].astype(str).str.lower().eq("true")].copy()
    taf["issued_ms"] = (
        pd.to_datetime(taf["valid"], utc=True).astype("int64") // 1_000_000
    )
    taf["fx_ms"] = (
        pd.to_datetime(taf["fx_valid"], utc=True).astype("int64") // 1_000_000
    )
    taf["vis"] = pd.to_numeric(taf["visibility"], errors="coerce")
    taf["sknt_n"] = pd.to_numeric(taf["sknt"], errors="coerce")
    taf["gust_n"] = pd.to_numeric(taf["gust"], errors="coerce")
    taf = taf.sort_values(["station", "issued_ms", "fx_ms"], kind="mergesort")

    stations: dict[str, list] = {}
    for (station, issued_ms), g in taf.groupby(["station", "issued_ms"], sort=True):
        fx = g["fx_ms"].to_numpy()
        feats = []
        for r in g.itertuples(index=False):
            fog, rain, snow, thunder = _wx_flags(r.presentwx)
            gust_reported = 1.0 if (not math.isnan(r.gust_n) and r.gust_n > 0) else 0.0
            feats.append(
                {
                    "origin_temp_f": math.nan,       # TAF carries no temperature
                    "origin_dewpoint_f": math.nan,   # nor dewpoint
                    "origin_precip_1h_in": math.nan,  # nor precip amounts
                    "origin_wind_speed_kn": (
                        float(r.sknt_n) if not math.isnan(r.sknt_n) else math.nan
                    ),
                    "origin_gust_kn": float(r.gust_n) if gust_reported else 0.0,
                    "origin_gust_reported": gust_reported,
                    "origin_visibility_mi": float(r.vis) if not math.isnan(r.vis) else math.nan,
                    "origin_had_fog": fog,
                    "origin_had_rain_drizzle": rain,
                    "origin_had_snow_ice_pellets": snow,
                    "origin_had_thunder": thunder,
                    "has_origin_weather": 1.0,
                }
            )
        stations.setdefault(station, []).append((int(issued_ms), fx, feats))
    for entries in stations.values():
        entries.sort(key=lambda e: e[0])
    return stations


def taf_station_for(iata: str, stations: dict) -> str | None:
    for prefix in ("K", "P"):
        if (icao := prefix + iata) in stations:
            return icao
    return None


def select_taf(stations: dict, icao: str, dep_ms: int) -> tuple[dict | None, int | None]:
    """(feature row, horizon_ms) for the scheduled departure, or (None, None).

    Latest issuance at or before departure whose prevailing groups cover the
    departure HOUR; a mid-hour issuance whose first group starts after that
    hour falls back one issuance. Horizon capped at 30h."""
    entries = stations[icao]
    hour_ms = dep_ms - (dep_ms % 3_600_000)
    i = bisect.bisect_right([e[0] for e in entries], dep_ms) - 1
    while i >= 0:
        issued_ms, fx, feats = entries[i]
        assert issued_ms <= dep_ms  # the standing pre-departure guard
        if dep_ms - issued_ms > MAX_HORIZON_MS:
            return None, None
        j = int(np.searchsorted(fx, hour_ms, side="right")) - 1
        if j >= 0:
            return feats[j], dep_ms - issued_ms
        i -= 1  # TAF issued mid-hour: the prior issuance covers this hour
    return None, None


def horizon_bin(horizon_ms: int) -> str:
    hours = horizon_ms / 3_600_000
    for name, lo, hi in HORIZON_BINS:
        if lo <= hours < hi or (name == "12-30h" and hours == hi):
            return name
    return "12-30h"


def roc_auc(pairs: list[tuple[float, bool]]) -> float | None:
    if not pairs or len({y for _, y in pairs}) < 2:
        return None
    from sklearn.metrics import roc_auc_score

    return round(float(roc_auc_score([y for _, y in pairs], [p for p, _ in pairs])), 6)


def metrics(pairs: list[tuple[float, bool]]) -> dict:
    return {
        "n": len(pairs),
        "roc_auc": roc_auc(pairs),
        "pr_auc": _pr_auc(pairs),
        "ece": _ece(pairs),
    }


def main() -> None:
    dep, _ = load_week()
    tzs = tz_map()
    dep["dep_ts_utc_ms"] = [dep_ts_utc_ms(r, tzs) for r in dep.itertuples(index=False)]

    stations = load_taf_groups()
    lookups = load_lookups()
    tracker = RotationTracker(load_day_leg_counts())
    clf, calibrator, run_id = load_scoring_artifacts()
    booster_names = list(clf.get_booster().feature_names)

    obs_rows, taf_rows, meta = [], [], []
    for r in dep.itertuples(index=False):
        ev = {
            "tail_number": r.tail_number if isinstance(r.tail_number, str) else None,
            "carrier": r.carrier,
            "flight_date": dt.date.fromisoformat(r.flight_date),
            "flight_number": r.flight_number,
            "origin": r.origin,
            "dest": r.dest,
            "crs_dep_time": r.crs_dep_time,
            "crs_arr_time": r.crs_arr_time if isinstance(r.crs_arr_time, str) else None,
            "crs_dep_ts_ms": r.dep_ts_utc_ms,
            "crs_elapsed_min": None if pd.isna(r.crs_elapsed_min) else r.crs_elapsed_min,
            "distance_mi": None if pd.isna(r.distance_mi) else r.distance_mi,
            "is_warmup": r.flight_date == WARMUP_DAY,
        }
        link = tracker.observe(ev)
        row, _basis = enrich(ev, lookups, link)
        obs_rows.append(row)

        icao = taf_station_for(r.origin, stations)
        wx, horizon_ms = (None, None) if icao is None else select_taf(
            stations, icao, r.dep_ts_utc_ms
        )
        taf_row = dict(row)
        if wx is not None:
            taf_row.update(wx)
            meta.append({"bin": horizon_bin(horizon_ms), "covered": True})
        else:
            for col in WEATHER_COLS:
                taf_row[col] = math.nan
            taf_row["has_origin_weather"] = 0.0
            meta.append({"bin": None, "covered": False})
        taf_rows.append(taf_row)

    def score(rows: list[dict]) -> np.ndarray:
        out = []
        for lo in range(0, len(rows), CHUNK):
            x = build_frame(rows[lo : lo + CHUNK], lookups, booster_names)
            out.append(calibrator.transform(clf.predict_proba(x)[:, 1]))
        return np.concatenate(out)

    p_obs, p_taf = score(obs_rows), score(taf_rows)

    outcomes = pd.read_parquet(REPO / "data/replay/outcomes_week.parquet")
    labels = {
        tuple(getattr(o, k) for k in IDENT): o.arr_del15
        for o in outcomes.itertuples(index=False)
        if not o.cancelled and o.arr_del15 is not None and not pd.isna(o.arr_del15)
    }

    per_bin: dict[str, dict[str, list]] = {
        name: {"obs": [], "taf": []} for name, _, _ in HORIZON_BINS
    }
    uncovered = {"obs": [], "taf": []}
    covered_n = uncovered_n = 0
    for i, r in enumerate(dep.itertuples(index=False)):
        if r.flight_date == WARMUP_DAY:
            continue
        y = labels.get(tuple(getattr(r, k) for k in IDENT))
        if y is None:
            continue
        m = meta[i]
        if m["covered"]:
            covered_n += 1
            per_bin[m["bin"]]["obs"].append((float(p_obs[i]), bool(y)))
            per_bin[m["bin"]]["taf"].append((float(p_taf[i]), bool(y)))
        else:
            uncovered_n += 1
            uncovered["obs"].append((float(p_obs[i]), bool(y)))
            uncovered["taf"].append((float(p_taf[i]), bool(y)))

    report = {
        "model_run_id": run_id,
        "labeled_departures": covered_n + uncovered_n,
        "taf_covered": covered_n,
        "taf_uncovered_null_path": uncovered_n,
        "coverage_pct": round(100.0 * covered_n / (covered_n + uncovered_n), 3),
        "bins": {},
        "uncovered": {"observed": metrics(uncovered["obs"]), "taf": metrics(uncovered["taf"])},
    }
    print(f"TAF substitution study — model {run_id}")
    print(f"coverage: {covered_n:,} of {covered_n + uncovered_n:,} labeled departures "
          f"({report['coverage_pct']}%); {uncovered_n:,} take the NULL weather path")
    print(f"{'bin':>8} {'n':>8}   {'ROC obs':>8} {'ROC taf':>8}   {'PR obs':>8} "
          f"{'PR taf':>8} {'dPR':>8}   {'ECE obs':>8} {'ECE taf':>8}")
    for name, _, _ in HORIZON_BINS:
        mo, mt = metrics(per_bin[name]["obs"]), metrics(per_bin[name]["taf"])
        d_pr = (
            None if mo["pr_auc"] is None or mt["pr_auc"] is None
            else round(mo["pr_auc"] - mt["pr_auc"], 6)
        )
        report["bins"][name] = {"observed": mo, "taf": mt, "pr_auc_degradation": d_pr}

        def _s(v: float | None) -> str:  # sparse bins can have None metrics
            return f"{v:>8}" if v is not None else f"{'-':>8}"

        print(f"{name:>8} {mo['n']:>8,}   {_s(mo['roc_auc'])} {_s(mt['roc_auc'])}   "
              f"{_s(mo['pr_auc'])} {_s(mt['pr_auc'])} {_s(d_pr)}   "
              f"{_s(mo['ece'])} {_s(mt['ece'])}")

    short = report["bins"]["0-3h"]["pr_auc_degradation"]
    triggered = short is not None and short > TRIGGER_PR_AUC_DROP
    report["trigger"] = {
        "short_bin_pr_auc_degradation": short,
        "threshold": TRIGGER_PR_AUC_DROP,
        "verdict": (
            "representation mismatch — harmonize first, never retrain on this alone"
            if triggered
            else "within threshold — no representation-mismatch response required"
        ),
    }
    print(f"\ntrigger (0-3h ΔPR-AUC > {TRIGGER_PR_AUC_DROP}): {short} -> "
          f"{'TRIGGERED' if triggered else 'not triggered'}")
    print(f"  {report['trigger']['verdict']}")

    # data/, not evaluation/: evaluation/ is git-ignored runtime output, and
    # this is a committed finding (the drift_report.json precedent)
    path = REPO / "data/taf_study.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()
