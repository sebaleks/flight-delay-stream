"""One-time export of every replay/streaming asset out of the 681 warehouse.

Runs ONCE while the BigQuery path is still live (docs/PLAN.md Step 0), lands
everything the streaming consumer and handoff prompts need under data/, then
has no further role. Requires ADC + GCP_PROJECT_ID/BQ_GOLD_DATASET/
BQ_SILVER_DATASET env vars and the downloaded model artifacts under
ml/artifacts/<run>/ (the replay-week choice scores flights).

Outputs (all parquet unless noted):
  data/replay/departures_week.parquet    schedule fields, warm-up day included
  data/replay/outcomes_week.parquet      truth fields, same window
  data/weather/isd_week.parquet          silver ISD hourly rows for the window
  data/lookups/entity_profile.parquet    serving_entity_profile (8,316 rows)
  data/lookups/density_profile.parquet   serving_density_profile
  data/lookups/typical_rotation.parquet  serving_typical_rotation (1 row)
  data/lookups/airports.parquet          dim_airport (iata, lat, lon, tz)
  data/lookups/airport_station_map.parquet  airport <-> ISD station map
  data/golden/rotation_reference_week.parquet  mart rotation columns, the H3
                                         parity target, keyed by flight identity
  data/golden/features_week_sample.parquet     5,000 full 51-feature mart rows,
                                         the H2 enrichment golden reference
  data/weather/taf_week.csv              IEM archived TAFs, week + 30h lookback
  data/exceedance.json                   copied from the published artifact run
  data/exports_report.json               week choice, tail cardinality, row counts
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
import time
from pathlib import Path

import pandas as pd
import requests

REPO = Path(__file__).resolve().parents[1]

# The replay week is chosen by the day-typicality z-band method (ml/
# day_typicality.py): every day of the chosen Monday-Sunday week must sit
# inside the central 80% of ORD's held-out z distribution; ties break on the
# smallest mean |z|. ORD because it is the origin the 681 demo tooling used.
TYPICALITY_ORIGIN = "ORD"

HOLDOUT_START = dt.date(2024, 7, 1)
HOLDOUT_END = dt.date(2024, 12, 31)


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def choose_week(ctx) -> tuple[dt.date, dt.date]:
    cache = REPO / "data/week_choice.json"
    if cache.exists():  # resume guard: never re-score for a choice already made
        c = json.loads(cache.read_text())
        log(f"week choice cached: {c['week_start']} .. {c['week_end']}")
        return dt.date.fromisoformat(c["week_start"]), dt.date.fromisoformat(c["week_end"])

    from ml.day_typicality import QUANTILE_HI, QUANTILE_LO, daily_moments
    from ml.replay import load_holdout, score

    log(f"scoring {TYPICALITY_ORIGIN}'s whole held-out window for the week choice ...")
    daily = daily_moments(score(ctx, load_holdout(ctx, sample=None, origin=TYPICALITY_ORIGIN)))
    z = daily["z"]
    lo, hi = float(z.quantile(QUANTILE_LO)), float(z.quantile(QUANTILE_HI))
    log(f"typicality band z in [{lo:+.2f}, {hi:+.2f}] over {len(daily)} days")

    candidates = []
    day = HOLDOUT_START
    while day.weekday() != 0:  # first Monday
        day += dt.timedelta(days=1)
    while day + dt.timedelta(days=6) <= HOLDOUT_END:
        days = [day + dt.timedelta(days=i) for i in range(7)]
        zs = [daily["z"].get(d) for d in days]
        if all(v is not None and lo <= v <= hi for v in zs):
            candidates.append((sum(abs(v) for v in zs) / 7, day))
        day += dt.timedelta(days=7)
    if not candidates:
        raise SystemExit("no Monday-Sunday week has all 7 days inside the typicality band")
    candidates.sort()
    start = candidates[0][1]
    log(f"chosen week {start} .. {start + dt.timedelta(days=6)} "
        f"(mean |z| {candidates[0][0]:.3f}; {len(candidates)} candidate weeks)")
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps({
        "week_start": start.isoformat(),
        "week_end": (start + dt.timedelta(days=6)).isoformat(),
        "origin": TYPICALITY_ORIGIN,
        "mean_abs_z": candidates[0][0],
        "candidate_weeks": len(candidates),
    }, indent=2) + "\n")
    return start, start + dt.timedelta(days=6)


def export(bq, sql: str, path: Path, label: str) -> int:
    if path.exists():  # resume guard: a rerun never re-queries what landed
        log(f"{label}: exists, skipped ({path})")
        return -1
    path.parent.mkdir(parents=True, exist_ok=True)
    df = bq.query(sql).result().to_dataframe()
    df.to_parquet(path, index=False)
    log(f"{label}: {len(df):,} rows -> {path}")
    return len(df)


def main() -> None:
    from ingestion.config import require_env
    from ml.serving import build_context

    project = require_env("GCP_PROJECT_ID")
    gold = require_env("BQ_GOLD_DATASET")
    silver = require_env("BQ_SILVER_DATASET")

    ctx = build_context()
    bq = ctx.bq
    counts: dict[str, int] = {}

    week_start, week_end = choose_week(ctx)
    # one warm-up day precedes the scored window so per-tail rotation state
    # hydrates before evaluation begins (docs/PLAN.md, docs/schemas.md)
    warmup_start = week_start - dt.timedelta(days=1)

    flights = f"`{project}.{silver}.silver_flights`"
    mart = f"`{project}.{gold}.ml_flight_features`"

    sched_cols = """
        cast(flight_date as string) as flight_date,
        reporting_airline as carrier,
        cast(flight_number_reporting_airline as string) as flight_number,
        origin, dest,
        format_time('%H%M', crs_dep_time) as crs_dep_time,
        format_time('%H%M', crs_arr_time) as crs_arr_time,
        crs_elapsed_time as crs_elapsed_min,
        distance as distance_mi,
        tail_number,
        day_of_week, month
    """
    win = (f"flight_date between '{warmup_start.isoformat()}' "
           f"and '{week_end.isoformat()}'")

    counts["departures_week"] = export(
        bq,
        f"select {sched_cols} from {flights} where {win} "
        "order by flight_date, crs_dep_time, reporting_airline, "
        "flight_number_reporting_airline, origin, dest",
        REPO / "data/replay/departures_week.parquet",
        "departures_week",
    )

    counts["outcomes_week"] = export(
        bq,
        f"""select
              cast(flight_date as string) as flight_date,
              reporting_airline as carrier,
              cast(flight_number_reporting_airline as string) as flight_number,
              origin, dest,
              format_time('%H%M', crs_dep_time) as crs_dep_time,
              tail_number,
              arr_del15, arr_delay_minutes, cancelled, diverted,
              format_time('%H%M', dep_time) as dep_time,
              format_time('%H%M', arr_time) as arr_time,
              crs_elapsed_time as crs_elapsed_min
            from {flights} where {win}""",
        REPO / "data/replay/outcomes_week.parquet",
        "outcomes_week",
    )

    counts["isd_week"] = export(
        bq,
        f"""select * from `{project}.{silver}.silver_isd_hourly`
            where obs_date between '{(warmup_start - dt.timedelta(days=1)).isoformat()}'
                               and '{week_end.isoformat()}'""",
        REPO / "data/weather/isd_week.parquet",
        "isd_week",
    )

    for table, fname in [
        ("serving_entity_profile", "entity_profile"),
        ("serving_density_profile", "density_profile"),
        ("serving_typical_rotation", "typical_rotation"),
    ]:
        counts[fname] = export(
            bq, f"select * from `{project}.{gold}.{table}`",
            REPO / f"data/lookups/{fname}.parquet", fname,
        )
    counts["airports"] = export(
        bq,
        f"select airport_key as iata, latitude, longitude, tz "
        f"from `{project}.{gold}.dim_airport`",
        REPO / "data/lookups/airports.parquet",
        "airports (dim_airport)",
    )
    counts["airport_station_map"] = export(
        bq, f"select * from `{project}.{silver}.airport_station_map`",
        REPO / "data/lookups/airport_station_map.parquet", "airport_station_map",
    )

    rotation_cols = """
        rotation_position, legs_today, has_inbound_leg,
        sched_turnaround_min, sched_turnaround_slack_min, is_tight_turnaround,
        inbound_distance, inbound_crs_elapsed_min, origin_dep_density_hour,
        hist_turnaround_band_arr_del15_rate, hist_turnaround_band_avg_arr_delay_minutes,
        hist_turnaround_band_n_flights,
        hist_rotation_position_arr_del15_rate, hist_rotation_position_avg_arr_delay_minutes,
        hist_rotation_position_n_flights
    """
    identity = """
        cast(flight_date as string) as flight_date, carrier,
        cast(flight_number as string) as flight_number, origin, dest,
        format_time('%H%M', crs_dep_time) as crs_dep_time
    """
    counts["rotation_reference_week"] = export(
        bq,
        f"select {identity}, {rotation_cols} from {mart} where {win} and not is_training_row",
        REPO / "data/golden/rotation_reference_week.parquet",
        "rotation_reference_week",
    )

    import ml.features as f

    # carrier / origin / dest are already IN f.FEATURES (ml/replay.py:77-78
    # warns about exactly this duplicate); identity here carries only the
    # remaining grain columns
    slim_identity = """
        cast(flight_date as string) as flight_date,
        cast(flight_number as string) as flight_number,
        format_time('%H%M', crs_dep_time) as crs_dep_time
    """
    counts["features_week_sample"] = export(
        bq,
        f"select {slim_identity}, {', '.join(f.FEATURES)}, {', '.join(f.LABELS)} "
        f"from {mart} where {win} and not is_training_row "
        "order by farm_fingerprint(concat(cast(flight_date as string), carrier, "
        "cast(flight_number as string), origin, dest, cast(crs_dep_time as string))) "
        "limit 5000",
        REPO / "data/golden/features_week_sample.parquet",
        "features_week_sample",
    )

    log("tail-cardinality query ...")
    tails = list(bq.query(f"""
        select
          count(distinct tail_number) as distinct_tails,
          count(distinct if({win}, tail_number, null)) as week_tails,
          (select count(*) from (
             select tail_number from {flights}
             where tail_number is not null
             group by tail_number
             having count(distinct reporting_airline) > 1
          )) as multi_carrier_tails_ever,
          (select count(*) from (
             select tail_number from {flights}
             where tail_number is not null and {win}
             group by tail_number
             having count(distinct reporting_airline) > 1
          )) as multi_carrier_tails_week,
          countif(tail_number is null) as null_tail_rows
        from {flights}
    """).result())[0]
    tail_report = dict(tails.items())
    log(f"tails: {tail_report}")

    run_dir = sorted((REPO / "ml/artifacts").glob("[0-9]*_[0-9]*"))[-1]
    shutil.copy(run_dir / "exceedance.json", REPO / "data/exceedance.json")
    log(f"exceedance.json copied from {run_dir.name}")

    # ---- IEM TAF fetch: chosen week + 30h lookback, per origin station ----
    seed = pd.read_csv(REPO / "dbt/seeds/airports.csv")
    dep = pd.read_parquet(REPO / "data/replay/departures_week.parquet")
    origins = sorted(dep["origin"].dropna().unique())
    icao = seed.set_index("iata")["icao"].to_dict()
    stations = [icao[o] for o in origins if o in icao and isinstance(icao[o], str)]
    unmapped = [o for o in origins if o not in icao or not isinstance(icao.get(o), str)]
    log(f"TAF fetch: {len(stations)} stations ({len(unmapped)} unmapped: {unmapped[:8]} ...)")

    sts = dt.datetime.combine(warmup_start, dt.time()) - dt.timedelta(hours=30)
    ets = dt.datetime.combine(week_end + dt.timedelta(days=1), dt.time())
    frames, failed = [], []
    def fetch_taf(station_id: str) -> pd.DataFrame | None:
        url = ("https://mesonet.agron.iastate.edu/cgi-bin/request/taf.py"
               f"?station={station_id}&fmt=csv"
               f"&sts={sts:%Y-%m-%dT%H:%M}Z&ets={ets:%Y-%m-%dT%H:%M}Z")
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return pd.read_csv(pd.io.common.StringIO(r.text)) if r.text.count("\n") > 1 else None

    for i, st in enumerate(stations):
        try:
            # IEM indexes some networks by 4-char ICAO and some by the
            # 3-char id; try the ICAO form first, fall back to stripped
            got = fetch_taf(st)
            if got is None and len(st) == 4 and st[0] in "KP":
                got = fetch_taf(st[1:])
            if got is not None:
                frames.append(got)
            else:
                failed.append(st)
        except Exception as e:  # noqa: BLE001 - per-station failure is data, not fatal
            failed.append(st)
            log(f"  TAF {st} failed: {e}")
        if i % 50 == 0:
            log(f"  TAF {i}/{len(stations)} ...")
        time.sleep(0.2)
    taf = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    out = REPO / "data/weather/taf_week.csv"
    taf.to_csv(out, index=False)
    counts["taf_rows"] = len(taf)
    log(f"TAF: {len(taf):,} rows from {len(frames)} stations -> {out}; "
        f"{len(failed)} stations empty/failed")

    report = {
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "warmup_day": warmup_start.isoformat(),
        "typicality_origin": TYPICALITY_ORIGIN,
        "artifact_run": run_dir.name,
        "row_counts": counts,
        "tails": tail_report,
        "taf_stations_no_data": failed,
        "origins_unmapped_to_icao": unmapped,
    }
    (REPO / "data/exports_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    log("EXPORTS DONE")


if __name__ == "__main__":
    main()
