"""The harmonize-first follow-up: decompose the TAF gap at its source.

The TAF study's trigger fired (0-3h dPR-AUC 0.0430 > 0.010,
data/taf_study.json), and the pre-registered response is to harmonize the
REPRESENTATION before considering any retrain (docs/PLAN.md decision 5). This
control does the measurement that decision needs, with the frozen model and
no feature change:

Three regimes on the SAME covered, labeled, non-warm-up rows:
  observed    the training representation (the study's baseline)
  harmonized  the OBSERVED values degraded to TAF's representation: temp,
              dewpoint, precip -> NaN (TAF never carries them), visibility
              censored at the TAF encoding (> 6 mi -> 6.01; training saw ISD
              censored at 10.0). Same weather truth, TAF's vocabulary.
  taf         the study's TAF substitution (real forecasts)

Per bin: observed - harmonized = the REPRESENTATION share of the gap (cost of
TAF's vocabulary with perfect foresight); harmonized - taf = the FORECAST
share (cost of actually forecasting, at matched representation). If the
representation share dominates the short bin, the mismatch is confirmed at
the source and the harmonized-feature retrain becomes justified future work
under the adoption rule (CLAUDE.md section 4). Executed as an EVALUATION
only: nothing here fits, tunes, or selects.

Also reported: flight-rules category agreement between observed and TAF
visibility per covered departure (VFR > 5 mi, MVFR 3-5, IFR 1-3, LIFR < 1).
Ceiling is absent from both exports, so this is the visibility half of the
standard category, disclosed as such.

Both the study's numbers and this control are logged to MLflow as evaluation
runs (CLAUDE.md section 4 adaptation note), pure-side-effect.

    uv run --extra kafka --extra ml python scripts/harmonization_control.py
"""

from __future__ import annotations

import datetime as dt
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.taf_substitution_study import (  # noqa: E402
    HORIZON_BINS,
    IDENT,
    WARMUP_DAY,
    horizon_bin,
    load_taf_groups,
    metrics,
    select_taf,
    taf_station_for,
)
from streaming.consumer import (  # noqa: E402
    build_frame,
    enrich,
    load_lookups,
    load_scoring_artifacts,
)
from streaming.producer import dep_ts_utc_ms, load_week, tz_map  # noqa: E402
from streaming.rotation import RotationTracker, load_day_leg_counts  # noqa: E402

CHUNK = 20_000
TAF_VIS_CENSOR = 6.01  # IEM encodes P6SM (> 6 statute miles) as 6.01

# visibility half of the standard flight-rules category (ceiling absent from
# both exports); edges in statute miles
FLIGHT_RULES_EDGES = ((5.0, "VFR"), (3.0, "MVFR"), (1.0, "IFR"), (-math.inf, "LIFR"))


def flight_rules(vis: float) -> str | None:
    if vis is None or (isinstance(vis, float) and math.isnan(vis)):
        return None
    for edge, name in FLIGHT_RULES_EDGES:
        if vis > edge:
            return name
    return "LIFR"


def harmonize(row: dict) -> dict:
    """Observed values in TAF's vocabulary; weather truth unchanged."""
    h = dict(row)
    h["origin_temp_f"] = math.nan
    h["origin_dewpoint_f"] = math.nan
    h["origin_precip_1h_in"] = math.nan
    vis = h["origin_visibility_mi"]
    if isinstance(vis, float) and not math.isnan(vis) and vis > 6.0:
        h["origin_visibility_mi"] = TAF_VIS_CENSOR
    return h


def main() -> None:
    dep, _ = load_week()
    tzs = tz_map()
    dep["dep_ts_utc_ms"] = [dep_ts_utc_ms(r, tzs) for r in dep.itertuples(index=False)]

    stations = load_taf_groups()
    lookups = load_lookups()
    tracker = RotationTracker(load_day_leg_counts())
    clf, calibrator, run_id = load_scoring_artifacts()
    booster_names = list(clf.get_booster().feature_names)

    obs_rows, harm_rows, taf_rows, meta = [], [], [], []
    agree = {"n": 0, "match": 0, "confusion": {}}
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
        harm_rows.append(harmonize(row))

        icao = taf_station_for(r.origin, stations)
        wx, horizon_ms = (None, None) if icao is None else select_taf(
            stations, icao, r.dep_ts_utc_ms
        )
        taf_row = dict(row)
        if wx is not None:
            taf_row.update(wx)
            meta.append({"bin": horizon_bin(horizon_ms), "covered": True})
            cat_obs = flight_rules(row["origin_visibility_mi"])
            cat_taf = flight_rules(wx["origin_visibility_mi"])
            if cat_obs is not None and cat_taf is not None:
                agree["n"] += 1
                agree["match"] += int(cat_obs == cat_taf)
                key = f"{cat_obs}->{cat_taf}"
                agree["confusion"][key] = agree["confusion"].get(key, 0) + 1
        else:
            from scripts.taf_substitution_study import WEATHER_COLS

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

    p = {"observed": score(obs_rows), "harmonized": score(harm_rows), "taf": score(taf_rows)}

    outcomes = pd.read_parquet(REPO / "data/replay/outcomes_week.parquet")
    labels = {
        tuple(getattr(o, k) for k in IDENT): o.arr_del15
        for o in outcomes.itertuples(index=False)
        if not o.cancelled and o.arr_del15 is not None and not pd.isna(o.arr_del15)
    }

    per_bin = {name: {k: [] for k in p} for name, _, _ in HORIZON_BINS}
    for i, r in enumerate(dep.itertuples(index=False)):
        if r.flight_date == WARMUP_DAY:
            continue
        y = labels.get(tuple(getattr(r, k) for k in IDENT))
        if y is None or not meta[i]["covered"]:
            continue
        for k in p:
            per_bin[meta[i]["bin"]][k].append((float(p[k][i]), bool(y)))

    report = {
        "model_run_id": run_id,
        "taf_vis_censor": TAF_VIS_CENSOR,
        "flight_rules_agreement": {
            "n": agree["n"],
            "rate": round(agree["match"] / agree["n"], 6) if agree["n"] else None,
            "confusion_offdiagonal": {
                k: v
                for k, v in sorted(agree["confusion"].items())
                if k.split("->")[0] != k.split("->")[1]
            },
        },
        "bins": {},
    }
    print(f"harmonization control — model {run_id}")
    print(f"{'bin':>8} {'n':>8}   {'PR obs':>8} {'PR harm':>8} {'PR taf':>8}   "
          f"{'repr share':>10} {'fcst share':>10}   {'ECE harm':>8} {'ECE taf':>8}")
    for name, _, _ in HORIZON_BINS:
        m = {k: metrics(v) for k, v in per_bin[name].items()}
        pr = {k: m[k]["pr_auc"] for k in m}
        rep = None if None in (pr["observed"], pr["harmonized"]) else round(
            pr["observed"] - pr["harmonized"], 6)
        fcst = None if None in (pr["harmonized"], pr["taf"]) else round(
            pr["harmonized"] - pr["taf"], 6)
        report["bins"][name] = {
            **{k: m[k] for k in m},
            "representation_share_pr_auc": rep,
            "forecast_share_pr_auc": fcst,
        }

        def _s(v, w=8):
            return f"{v:>{w}}" if v is not None else f"{'-':>{w}}"

        print(f"{name:>8} {m['observed']['n']:>8,}   {_s(pr['observed'])} "
              f"{_s(pr['harmonized'])} {_s(pr['taf'])}   {_s(rep, 10)} {_s(fcst, 10)}   "
              f"{_s(m['harmonized']['ece'])} {_s(m['taf']['ece'])}")

    short = report["bins"]["0-3h"]
    rep, fcst = short["representation_share_pr_auc"], short["forecast_share_pr_auc"]
    dominated = rep is not None and fcst is not None and rep >= fcst
    report["verdict"] = {
        "short_bin_representation_share": rep,
        "short_bin_forecast_share": fcst,
        "representation_dominates": bool(dominated),
        "conclusion": (
            "representation mismatch confirmed at the source: the harmonized-"
            "feature retrain (VFR/MVFR/IFR/LIFR from both sides) is justified "
            "future work under the adoption rule — validation selects, test "
            "confirms once. Nothing here fits or selects."
            if dominated
            else "forecast error dominates at matched representation: harmonization "
            "alone cannot close the short-bin gap, and retraining on forecast "
            "inputs remains the (unexercised) next consideration."
        ),
    }
    print(f"\nverdict: representation share {rep} vs forecast share {fcst} -> "
          f"{'REPRESENTATION dominates' if dominated else 'forecast error dominates'}")
    print(f"  {report['verdict']['conclusion']}")

    path = REPO / "data/taf_harmonization.json"
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    print(f"wrote {path}")

    try:  # MLflow evaluation runs: the study + this control, pure side effect
        import mlflow

        mlflow.set_tracking_uri(f"sqlite:///{REPO / 'mlflow.db'}")
        mlflow.set_experiment("flight-delay-taf")
        study = json.loads((REPO / "data/taf_study.json").read_text())
        with mlflow.start_run(run_name="taf-substitution-study"):
            mlflow.log_params({"model_run": run_id})
            for b, v in study["bins"].items():
                if v["pr_auc_degradation"] is not None:
                    mlflow.log_metric(f"dpr_auc_{b.replace('-', '_')}", v["pr_auc_degradation"])
        with mlflow.start_run(run_name="harmonization-control"):
            mlflow.log_params({"model_run": run_id})
            if rep is not None:
                mlflow.log_metric("repr_share_0_3h", rep)
            if fcst is not None:
                mlflow.log_metric("forecast_share_0_3h", fcst)
        print("logged study + control to MLflow (sqlite:///mlflow.db)")
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: mlflow logging skipped ({e})")


if __name__ == "__main__":
    main()
