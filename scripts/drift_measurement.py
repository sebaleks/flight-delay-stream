"""Drift measurement: the frozen 2024-06-30 model on 2024-H2 vs May 2026.

Batch, no Kafka. PRE-REGISTERED PREDICTION (stated before any scoring runs):
calibration degrades faster than ranking, i.e. ECE worsens by a larger
relative factor than the ROC-AUC margin over chance shrinks.

Regime, disclosed: the 12 weather features are NULLed in BOTH windows
(has_origin_weather = 0, the trained NULL path), so the comparison is
apples-to-apples on schedule + rotation + historical-rate features.

Pipelines, disclosed: the 2024-H2 features come from the audited mart
(weather then NULLed); the 2026 features are built from the raw BTS file
with streaming/rotation_batch.py (the constants-module rule). Before any
2026 scoring, the builder and the full assembly are VALIDATED against the
mart's own values for the replay week (data/golden/*): rotation columns
against rotation_reference_week.parquet and every non-weather feature
against features_week_sample.parquet. Validation results print with the
report; mismatches are counted, never hidden.

Honest limitation, disclosed: the 2024-H2 side was examined throughout 681
development; the 2026 side never was. The measured gap is drift PLUS
whatever the held-out numbers were optimistic by.

    GCP_PROJECT_ID=... BQ_GOLD_DATASET=... uv run --extra kafka --extra ml \\
        --extra serve --extra ingestion python scripts/drift_measurement.py
"""

from __future__ import annotations

import datetime as dt
import io
import json
import sys
import zipfile
from pathlib import Path
from zoneinfo import ZoneInfo

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import ml.features as f  # noqa: E402
from streaming.evaluator import _ece  # noqa: E402
from streaming.rotation_batch import build_rotation_frame  # noqa: E402

PREREGISTERED = ("calibration degrades faster than ranking: ECE worsens by a "
                 "larger relative factor than the ROC-AUC margin over chance shrinks")
TRAIN_END = dt.date(2024, 6, 30)
WEATHER_NULL_NOTE = "12 weather features NULLed in both windows (trained NULL path)"
RUN_DIR = REPO / "ml/artifacts/20260730_145241"  # read-only: release upload running
HIST_COLS = [c for c in f.FEATURES if c.startswith("hist_")]
WEATHER_COLS = [c for c in f.FEATURES if c.startswith("origin_") and "density" not in c] + [
    "has_origin_weather"
]


def log(msg: str) -> None:
    print(msg, flush=True)


# ---- lookups (frozen on the training window; staleness is the point) -------


def load_lookups() -> dict:
    ep = pd.read_parquet(REPO / "data/lookups/entity_profile.parquet")
    hist: dict[str, dict] = {}
    vocab: dict[str, list] = {}
    route_distance: dict[str, float] = {}
    for level, sub in ep.groupby("entity_level"):
        table = {
            r.entity_key: (r.hist_arr_del15_rate, r.hist_avg_arr_delay_minutes,
                           r.hist_n_flights)
            for r in sub.itertuples()
        }
        hist[level] = table
        if level in ("route", "carrier", "origin", "dest"):
            vocab[level] = sorted(table)
        if level == "route":
            route_distance = {
                r.entity_key: float(r.distance) for r in sub.itertuples()
                if pd.notna(r.distance)
            }
    airports = pd.read_parquet(REPO / "data/lookups/airports.parquet")
    tz = {r.iata: ZoneInfo(r.tz) for r in airports.itertuples() if isinstance(r.tz, str)}
    return {"hist": hist, "vocab": vocab, "route_distance": route_distance, "tz": tz}


def holiday_flags(dates: pd.Series) -> pd.DataFrame:
    import holidays as hol

    uniq = pd.to_datetime(dates.unique())
    years = range(min(d.year for d in uniq) - 1, max(d.year for d in uniq) + 2)
    us = hol.country_holidays("US", years=years)
    rows = {
        d: (float(d.date() in us),
            float((d + pd.Timedelta(days=1)).date() in us),
            float((d - pd.Timedelta(days=1)).date() in us))
        for d in uniq
    }
    flags = dates.map(rows)
    return pd.DataFrame(
        flags.tolist(), index=dates.index,
        columns=["is_holiday", "is_day_before_holiday", "is_day_after_holiday"],
    )


# ---- assembly from a raw schedule frame ------------------------------------


def dep_ts_ms(df: pd.DataFrame, tz: dict) -> pd.Series:
    known = df["origin"].map(lambda o: o in tz)
    if not known.all():
        n = int((~known).sum())
        log(f"  dropping {n} rows with origins missing a timezone "
            f"({sorted(df.loc[~known, 'origin'].unique())[:6]})")
    out = pd.Series(np.nan, index=df.index)
    dts = pd.to_datetime(df["flight_date"])
    hh = df["crs_dep_time"].str[:2].astype(int)
    mm = df["crs_dep_time"].str[2:].astype(int)
    for origin, sub in df[known].groupby("origin", sort=False):
        z = tz[origin]
        local = [
            dt.datetime(d.year, d.month, d.day, h, m, tzinfo=z)
            for d, h, m in zip(dts[sub.index], hh[sub.index], mm[sub.index], strict=True)
        ]
        out[sub.index] = [int(x.timestamp() * 1000) for x in local]
    return out


def assemble(sched: pd.DataFrame, lk: dict) -> pd.DataFrame:
    """Raw schedule frame -> the 51-feature frame (weather NULLed)."""
    sched = sched[sched["dep_ts_utc_ms"].notna()].copy()
    # origin departure density: a schedule aggregate over the whole window,
    # computed BEFORE any label filtering, kept for every row incl. class c
    sched["crs_dep_hour"] = sched["crs_dep_time"].str[:2].astype(int)
    sched["origin_dep_density_hour"] = sched.groupby(
        ["origin", "flight_date", "crs_dep_hour"]
    )["dep_ts_utc_ms"].transform("size").astype(float)

    rot = build_rotation_frame(sched)
    x = pd.DataFrame(index=rot.index)
    x["carrier"], x["origin"], x["dest"] = rot["carrier"], rot["origin"], rot["dest"]
    x["route"] = rot["origin"] + "-" + rot["dest"]
    x["distance"] = pd.to_numeric(rot["distance_mi"])
    x["crs_dep_hour"] = rot["crs_dep_time"].str[:2].astype(int)
    x["crs_arr_hour"] = pd.to_numeric(
        rot["crs_arr_time"].str[:2], errors="coerce"
    )
    x["day_of_week"] = pd.to_numeric(rot["day_of_week"])
    x["month"] = pd.to_numeric(rot["month"])

    for level, key in (("route", x["route"]), ("carrier", x["carrier"]),
                       ("origin", x["origin"]), ("dest", x["dest"])):
        cols = (f"hist_{level}_arr_del15_rate", f"hist_{level}_avg_arr_delay_minutes",
                f"hist_{level}_n_flights")
        mapped = key.map(lk["hist"][level])
        for i, c in enumerate(cols):
            x[c] = mapped.map(lambda t, i=i: t[i] if isinstance(t, tuple) else np.nan)
    for level, key in (("turnaround_band", rot["turnaround_band_key"]),
                       ("rotation_position", rot["rotation_position_key"])):
        cols = (f"hist_{level}_arr_del15_rate", f"hist_{level}_avg_arr_delay_minutes",
                f"hist_{level}_n_flights")
        mapped = key.map(lk["hist"][level])
        for i, c in enumerate(cols):
            x[c] = mapped.map(lambda t, i=i: t[i] if isinstance(t, tuple) else np.nan)

    for c in WEATHER_COLS:
        x[c] = np.nan
    x["has_origin_weather"] = 0.0

    x["rotation_position"] = pd.to_numeric(rot["rotation_position"], errors="coerce")
    x["legs_today"] = pd.to_numeric(rot["legs_today"], errors="coerce")
    x["origin_dep_density_hour"] = rot["origin_dep_density_hour"]
    x["has_inbound_leg"] = rot["has_inbound_leg"].map(
        {True: 1.0, False: 0.0}).astype(float)
    x["sched_turnaround_min"] = rot["sched_turnaround_min"]
    x["sched_turnaround_slack_min"] = rot["sched_turnaround_slack_min"]
    x["is_tight_turnaround"] = rot["is_tight_turnaround"].map(
        {True: 1.0, False: 0.0}).astype(float)
    x["inbound_distance"] = pd.to_numeric(rot["inbound_distance"], errors="coerce")
    x["inbound_crs_elapsed_min"] = pd.to_numeric(
        rot["inbound_crs_elapsed_min"], errors="coerce")

    x = pd.concat([x, holiday_flags(rot["flight_date"])], axis=1)
    return x[list(f.FEATURES)], rot


def coerce(x: pd.DataFrame, clf: xgb.XGBClassifier, lk: dict) -> pd.DataFrame:
    """ml/serving.py:750's coercion, context-free: training vocab, float32,
    column order, booster schema gate."""
    x = x.copy()
    for c in f.CATEGORICAL_FEATURES:
        vocab = lk["vocab"].get(c if c != "route" else "route")
        x[c] = pd.Categorical(x[c], categories=vocab)
    for c in f.NUMERIC_FEATURES:
        x[c] = pd.to_numeric(x[c]).astype("float32")
    assert list(x.columns) == list(f.FEATURES)
    assert list(x.columns) == clf.get_booster().feature_names
    return x


# ---- scoring + metrics -----------------------------------------------------


def metrics(p: np.ndarray, y: np.ndarray) -> dict:
    from sklearn.metrics import average_precision_score, roc_auc_score

    pairs = list(zip(p.tolist(), (y > 0).tolist(), strict=True))
    return {
        "n": int(len(y)),
        "base_rate": round(float(y.mean()), 6),
        "roc_auc": round(float(roc_auc_score(y, p)), 6),
        "pr_auc": round(float(average_precision_score(y, p)), 6),
        "ece": _ece(pairs),
    }


def nan_rates(x: pd.DataFrame) -> dict:
    return {
        "hist_all_cols_nan_share": round(float(x[HIST_COLS].isna().mean().mean()), 6),
        "hist_route_nan_share": round(
            float(x["hist_route_arr_del15_rate"].isna().mean()), 6),
        "hist_carrier_nan_share": round(
            float(x["hist_carrier_arr_del15_rate"].isna().mean()), 6),
        "hist_rotation_grain_nan_share": round(
            float(x["hist_turnaround_band_arr_del15_rate"].isna().mean()), 6),
    }


# ---- window builders -------------------------------------------------------


def window_2024h2(clf, lk) -> tuple[pd.DataFrame, np.ndarray, str]:
    from google.cloud import bigquery

    from ingestion.config import require_env

    project = require_env("GCP_PROJECT_ID")
    gold = require_env("BQ_GOLD_DATASET")
    bq = bigquery.Client(project=project)
    log("loading the 2024-H2 held-out mart (3.56M rows) ...")
    cols = ", ".join([*f.FEATURES, f.LABELS[0]])
    df = bq.query(
        f"select {cols} from `{project}.{gold}.ml_flight_features` "
        "where not is_training_row"
    ).result().to_dataframe()
    y = df.pop(f.LABELS[0]).astype(float).to_numpy()
    for c in WEATHER_COLS:
        df[c] = np.nan
    df["has_origin_weather"] = 0.0
    return coerce(df[list(f.FEATURES)], clf, lk), y, "mart features, weather NULLed"


BTS_COLS = {
    "FlightDate": "flight_date", "Reporting_Airline": "carrier",
    "Flight_Number_Reporting_Airline": "flight_number", "Origin": "origin",
    "Dest": "dest", "CRSDepTime": "crs_dep_time", "CRSArrTime": "crs_arr_time",
    "CRSElapsedTime": "crs_elapsed_min", "Distance": "distance_mi",
    "Tail_Number": "tail_number", "DayOfWeek": "day_of_week", "Month": "month",
    "ArrDel15": "arr_del15", "Cancelled": "cancelled", "Diverted": "diverted",
}


def window_2026(clf, lk) -> tuple[pd.DataFrame, np.ndarray, str, dict]:
    zpath = REPO / "data/bts/bts_2026_05.zip"
    log(f"parsing {zpath.name} ...")
    with zipfile.ZipFile(zpath) as z:
        member = next(n for n in z.namelist() if n.endswith(".csv"))
        raw = pd.read_csv(io.BytesIO(z.read(member)), usecols=list(BTS_COLS),
                          dtype=str, low_memory=False)
    raw = raw.rename(columns=BTS_COLS)
    for c in ("crs_dep_time", "crs_arr_time"):
        raw[c] = raw[c].str.replace("2400", "0000").str.zfill(4)
    for c in ("crs_elapsed_min", "distance_mi", "arr_del15", "cancelled",
              "diverted", "day_of_week", "month"):
        raw[c] = pd.to_numeric(raw[c], errors="coerce")
    raw["tail_number"] = raw["tail_number"].str.strip().replace("", np.nan)
    raw["dep_ts_utc_ms"] = dep_ts_ms(raw, lk["tz"])

    x_all, rot = assemble(raw, lk)
    labeled = (
        rot["arr_del15"].notna() & (rot["cancelled"] == 0) & (rot["diverted"] == 0)
        # May 1 is the cold-start day for rotation state (no April context),
        # excluded from metrics exactly as the replay's warm-up day is
        & (rot["flight_date"] != "2026-05-01")
    )
    stats = {
        "rows_raw": int(len(raw)),
        "rows_scored": int(labeled.sum()),
        "excluded_cancelled_or_diverted_or_unlabeled": int(
            (~labeled & (rot["flight_date"] != "2026-05-01")).sum()),
        "excluded_cold_start_2026_05_01": int((rot["flight_date"] == "2026-05-01").sum()),
        "link_class_shares": {
            k: round(float(v), 4)
            for k, v in rot.loc[labeled, "link_class"].value_counts(normalize=True)
            .to_dict().items()
        },
    }
    y = rot.loc[labeled, "arr_del15"].astype(float).to_numpy()
    return coerce(x_all[labeled], clf, lk), y, "raw BTS via rotation_batch", stats


# ---- golden validation (runs BEFORE any 2026 claim) ------------------------


def validate_against_golden(lk) -> dict:
    log("validating the batch builder against the mart's replay-week values ...")
    dep = pd.read_parquet(REPO / "data/replay/departures_week.parquet")
    dep["dep_ts_utc_ms"] = dep_ts_ms(dep, lk["tz"])
    _, rot = assemble(dep, lk)
    ident = ["flight_date", "carrier", "flight_number", "origin", "dest", "crs_dep_time"]

    ref = pd.read_parquet(REPO / "data/golden/rotation_reference_week.parquet")
    merged = rot.merge(ref, on=ident, suffixes=("_mine", "_mart"), how="inner")
    merged = merged[merged["flight_date"] != "2024-09-01"]  # warm-up: cold start
    rows = int(len(merged))
    mismatches = {}
    for c in ("rotation_position", "legs_today", "has_inbound_leg",
              "sched_turnaround_min", "sched_turnaround_slack_min",
              "is_tight_turnaround", "inbound_distance", "inbound_crs_elapsed_min"):
        a = merged[f"{c}_mine"].astype("float64", errors="ignore")
        b = merged[f"{c}_mart"].astype("float64", errors="ignore")
        a = pd.to_numeric(a, errors="coerce")
        b = pd.to_numeric(b, errors="coerce")
        diff = ~((a.isna() & b.isna()) | (np.isclose(a, b, atol=1e-6, equal_nan=True)))
        mismatches[c] = int(diff.sum())
    log(f"  rotation parity on {rows:,} post-warm-up rows: "
        f"{sum(mismatches.values())} total mismatched cells {mismatches}")

    # feature-level: assembled values must match the mart sample byte-exact
    # for the hist columns and to tolerance for the rest (weather excluded:
    # the sample carries real observations, this regime NULls them)
    sample = pd.read_parquet(REPO / "data/golden/features_week_sample.parquet")
    x_full, rot_full = assemble(dep, lk)
    # carrier/origin/dest are FEATURES (already in x_full); take only the
    # remaining identity columns from the rotation frame or the merge keys
    # collide on duplicate labels
    slim = ["flight_date", "flight_number", "crs_dep_time"]
    joined = pd.concat(
        [rot_full[slim].reset_index(drop=True), x_full.reset_index(drop=True)], axis=1
    ).merge(sample, on=ident, suffixes=("_mine", "_mart"), how="inner")
    feat_mismatch = {}
    check_cols = [c for c in f.NUMERIC_FEATURES
                  if c not in WEATHER_COLS and c != "origin_dep_density_hour"]
    for c in check_cols:
        a = pd.to_numeric(joined[f"{c}_mine"], errors="coerce").astype("float64")
        b = pd.to_numeric(joined[f"{c}_mart"], errors="coerce").astype("float64")
        diff = ~((a.isna() & b.isna()) | (np.isclose(a, b, atol=1e-4, equal_nan=True)))
        if int(diff.sum()):
            feat_mismatch[c] = int(diff.sum())
    dens_a = pd.to_numeric(
        joined["origin_dep_density_hour_mine"], errors="coerce").astype("float64")
    dens_b = pd.to_numeric(
        joined["origin_dep_density_hour_mart"], errors="coerce").astype("float64")
    dens_diff = int((~np.isclose(dens_a, dens_b, atol=0.5, equal_nan=True)).sum())
    log(f"  feature parity on {len(joined):,} sampled rows: "
        f"{feat_mismatch or 'all non-weather features match'}; "
        f"density mismatches {dens_diff}")
    return {
        "rotation_rows_compared": rows,
        "rotation_mismatched_cells": mismatches,
        "feature_rows_compared": int(len(joined)),
        "feature_mismatches": feat_mismatch,
        "density_mismatches": dens_diff,
    }


def main() -> None:
    log(f"PRE-REGISTERED PREDICTION: {PREREGISTERED}")
    log(f"REGIME: {WEATHER_NULL_NOTE}\n")

    clf = xgb.XGBClassifier()
    clf.load_model(RUN_DIR / "xgb_classifier.ubj")
    calibrator = joblib.load(RUN_DIR / "calibrator.joblib")
    lk = load_lookups()

    validation = validate_against_golden(lk)

    x24, y24, src24 = window_2024h2(clf, lk)
    p24 = calibrator.transform(clf.predict_proba(x24)[:, 1])
    m24 = {**metrics(p24, y24), **nan_rates(x24), "features": src24}
    log(f"2024-H2: {m24}")

    x26, y26, src26, stats26 = window_2026(clf, lk)
    p26 = calibrator.transform(clf.predict_proba(x26)[:, 1])
    m26 = {**metrics(p26, y26), **nan_rates(x26), "features": src26}
    log(f"2026-05: {m26}")

    roc_margin_24 = m24["roc_auc"] - 0.5
    roc_margin_26 = m26["roc_auc"] - 0.5
    ranking_decay = 1 - roc_margin_26 / roc_margin_24
    calib_decay = m26["ece"] / m24["ece"] - 1 if m24["ece"] else float("inf")
    held = calib_decay > ranking_decay

    report = {
        "preregistered_prediction": PREREGISTERED,
        "prediction_held": bool(held),
        "decay": {
            "roc_margin_relative_loss": round(float(ranking_decay), 4),
            "ece_relative_increase": round(float(calib_decay), 4),
        },
        "regime": WEATHER_NULL_NOTE,
        "elapsed": {
            "train_end": str(TRAIN_END),
            "window_2024h2": "2024-07-01..2024-12-31 (0-6 months after train end)",
            "window_2026": "2026-05-02..2026-05-31 (22-23 months after train end)",
        },
        "windows": {"2024H2": m24, "2026-05": m26},
        "window_2026_build": stats26,
        "builder_validation": validation,
        "limitation": (
            "the 2024-H2 side was examined throughout 681 development; the 2026 "
            "side was not. This measures drift plus whatever the held-out numbers "
            "were optimistic by."
        ),
        "calibration_caveat": (
            "both ECEs sit far above the calibrated 0.017 headline by "
            "construction: the weather-NULL regime shifts the score distribution "
            "the Platt map was fit on. The regime is common to both windows, so "
            "the RELATIVE comparison is the finding; the absolute values are not "
            "comparable to the headline."
        ),
        "model_run": RUN_DIR.name,
    }
    out = REPO / "data/drift_report.json"
    out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    log(f"\nwrote {out}")
    log(f"prediction held: {held} (ranking margin lost {ranking_decay:.1%}, "
        f"ECE grew {calib_decay:.1%})")

    try:  # MLflow: pure side effect, degrades to a warning
        import mlflow

        mlflow.set_tracking_uri(f"sqlite:///{REPO / 'mlflow.db'}")
        mlflow.set_experiment("flight-delay-drift")
        for name, m in (("2024H2-weather-null", m24), ("2026-05-weather-null", m26)):
            with mlflow.start_run(run_name=name):
                mlflow.log_params({"model_run": RUN_DIR.name, "regime": "weather_null"})
                mlflow.log_metrics({k: v for k, v in m.items()
                                    if isinstance(v, (int, float)) and v is not None})
        log("logged both windows to MLflow (sqlite:///mlflow.db)")
    except Exception as e:  # noqa: BLE001
        log(f"WARNING: mlflow logging skipped ({e})")


if __name__ == "__main__":
    main()
