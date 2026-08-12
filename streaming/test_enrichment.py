"""H2 golden check: consumer enrichment against the mart's own feature rows.

The gate (docs/HANDOFF_PROMPTS.md H2): for 20 rows of
data/golden/features_week_sample.parquet, the hist_* values enrichment
produces match the golden values exactly. The four base grains are asserted
here; the rotation grains are all-NULL by design under the H2 stub and are
asserted AS the stub (H3 replaces that assertion with real parity).

Weather parity is asserted for rows whose 3-hour lookback the ISD export
covers. The export truncates observations at week_end 23:59 UTC
(scripts/export_replay_assets.py), so last-evening local departures with a
UTC-next-day scheduled departure can lack their latest observations; those
rows take the training-legal NULL/stale path and are excluded here. Flagged
for the export owner at Sync 2.

No Kafka, no credentials, no model artifacts required.
"""

from __future__ import annotations

import datetime as dt
import math
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from ml import features as f
from streaming import constants as c
from streaming.consumer import REPO, build_frame, enrich, load_lookups

HIST_BASE = [
    f"hist_{g}_{s}"
    for g in ("route", "carrier", "origin", "dest")
    for s in ("arr_del15_rate", "avg_arr_delay_minutes", "n_flights")
]
WEATHER = [
    "origin_temp_f", "origin_dewpoint_f", "origin_wind_speed_kn", "origin_gust_kn",
    "origin_gust_reported", "origin_visibility_mi", "origin_precip_1h_in",
    "origin_had_fog", "origin_had_rain_drizzle", "origin_had_snow_ice_pellets",
    "origin_had_thunder", "has_origin_weather",
]
ROTATION_STUB_NULL = [
    "rotation_position", "legs_today", "has_inbound_leg", "sched_turnaround_min",
    "sched_turnaround_slack_min", "is_tight_turnaround", "inbound_distance",
    "inbound_crs_elapsed_min",
    "hist_turnaround_band_arr_del15_rate", "hist_turnaround_band_avg_arr_delay_minutes",
    "hist_turnaround_band_n_flights", "hist_rotation_position_arr_del15_rate",
    "hist_rotation_position_avg_arr_delay_minutes", "hist_rotation_position_n_flights",
]
GOLDEN_ROWS = 20


@pytest.fixture(scope="module")
def lookups():
    return load_lookups()


@pytest.fixture(scope="module")
def golden():
    return pd.read_parquet(REPO / "data/golden/features_week_sample.parquet").head(GOLDEN_ROWS)


@pytest.fixture(scope="module")
def tzs():
    air = pd.read_parquet(REPO / "data/lookups/airports.parquet")
    return {r.iata: ZoneInfo(r.tz) for r in air.itertuples() if isinstance(r.tz, str)}


@pytest.fixture(scope="module")
def obs_export_end_ms():
    import pyarrow.parquet as pq

    wx = pq.read_table(
        REPO / "data/weather/isd_week.parquet", columns=["obs_ts_utc"]
    ).to_pandas(ignore_metadata=True)
    return int(wx["obs_ts_utc"].max().timestamp() * 1000)


def _event(row, tzs) -> dict:
    d = dt.date.fromisoformat(row.flight_date)
    local = dt.datetime(
        d.year, d.month, d.day,
        int(row.crs_dep_time[:2]), int(row.crs_dep_time[2:]),
        tzinfo=tzs[row.origin],
    )
    return {
        "flight_date": d,
        "carrier": row.carrier,
        "flight_number": row.flight_number,
        "origin": row.origin,
        "dest": row.dest,
        "crs_dep_time": row.crs_dep_time,
        "crs_dep_ts_ms": int(local.timestamp() * 1000),
        "crs_arr_time": None,
        "distance_mi": None if pd.isna(row.distance) else float(row.distance),
        "is_warmup": False,
        "tail_number": None,
    }


def _num(v) -> float:
    return math.nan if v is None or pd.isna(v) else float(v)


def _assert_exact(golden_row, enriched: dict, columns: list[str]) -> None:
    for col in columns:
        want, have = _num(getattr(golden_row, col)), _num(enriched[col])
        if math.isnan(want) and math.isnan(have):
            continue
        assert want == have, (
            f"{col} mismatch for {golden_row.flight_date} {golden_row.carrier} "
            f"{golden_row.flight_number} {golden_row.origin}: golden {want} != enriched {have}"
        )


def test_hist_base_grains_match_golden_exactly(golden, lookups, tzs):
    for row in golden.itertuples(index=False):
        enriched, _ = enrich(_event(row, tzs), lookups)
        _assert_exact(row, enriched, HIST_BASE)


def test_weather_matches_golden_where_export_covers(golden, lookups, tzs, obs_export_end_ms):
    checked = 0
    for row in golden.itertuples(index=False):
        ev = _event(row, tzs)
        if ev["crs_dep_ts_ms"] >= obs_export_end_ms:  # lookback outruns the export
            continue
        enriched, basis = enrich(ev, lookups)
        _assert_exact(row, enriched, WEATHER)
        assert basis == ("observed" if enriched["has_origin_weather"] == 1.0 else "null_path")
        checked += 1
    assert checked > 0, "every golden row fell past the export boundary; widen the sample"


def test_rotation_block_is_the_swap_shaped_stub(golden, lookups, tzs):
    for row in golden.itertuples(index=False):
        enriched, _ = enrich(_event(row, tzs), lookups)
        for col in ROTATION_STUB_NULL:
            assert math.isnan(_num(enriched[col])), f"{col} must be NULL under the H2 stub"
        # the one rotation-block column training keeps on every row
        assert math.isfinite(_num(enriched["origin_dep_density_hour"]))


def test_frame_gate_and_dtypes(golden, lookups, tzs):
    rows = [enrich(_event(r, tzs), lookups)[0] for r in golden.itertuples(index=False)]
    x = build_frame(rows, lookups, booster_names=list(f.FEATURES))
    assert list(x.columns) == list(f.FEATURES)
    assert all(str(x[col].dtype) == "category" for col in f.CATEGORICAL_FEATURES)
    assert all(str(x[col].dtype) == "float32" for col in f.NUMERIC_FEATURES)


def test_frame_gate_rejects_missing_and_extra_columns(golden, lookups, tzs):
    from streaming.consumer import SchemaMismatchError

    row, _ = enrich(_event(next(golden.itertuples(index=False)), tzs), lookups)
    short = {k: v for k, v in row.items() if k != "origin_temp_f"}
    with pytest.raises(SchemaMismatchError):
        build_frame([short], lookups, booster_names=list(f.FEATURES))
    with pytest.raises(SchemaMismatchError):
        build_frame([{**row, "arr_delay": 12.0}], lookups, booster_names=list(f.FEATURES))


def test_risk_band_deciles():
    assert c.risk_band(0.0) == "0.0-0.1"
    assert c.risk_band(0.55) == "0.5-0.6"
    assert c.risk_band(0.999) == "0.9-1.0"
    assert c.risk_band(1.0) == "0.9-1.0"
