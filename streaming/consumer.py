"""Scoring consumer: flight.departures.v1 -> 51-feature frame -> frozen model
-> flight.delay_risk.v1.

Consumes Avro departure events against the registry, enriches each to the
canonical feature frame (ml/features.FEATURES) from the committed serving
lookups, scores with the frozen XGBoost classifier + Platt calibrator, and
produces one Avro delay-risk event per departure. The rotation block comes
from the per-tail state machine (streaming/rotation.py): schedule-consistent
linkages carry the full cascade block, swap-shaped linkages are all-NULL
(the tail-swap restriction, enforced in-stream).

Enrichment is a port of ml/serving.py's assembly (build_context /
assemble_features / coerce_feature_frame) with the BigQuery reads replaced by
the exported lookup parquets and the NDFD forecast path replaced by the
observed-weather join the mart used in training: the last ISD observation at
or before scheduled departure inside the staleness ceiling
(streaming/constants.WEATHER_STALENESS_HOURS; ml_flight_features.sql:246-250).

Batch mode (the `make demo` contract): consume to EOF, score, produce, EXIT.
Offsets are committed only AFTER the scored events for a chunk are produced
and flushed — process first, commit second — so a crash replays unscored
events (at-least-once) and can never lose one.

    uv run --extra kafka --extra ml python -m streaming.consumer            # batch, to EOF
    uv run --extra kafka --extra ml python -m streaming.consumer --follow   # continuous
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
from bisect import bisect_right
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import xgboost as xgb
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from ml import features as f
from streaming import constants as c
from streaming.admin import bootstrap, registry_url
from streaming.producer import partition_for
from streaming.rotation import Linkage, RotationTracker, basis_for, load_day_leg_counts

REPO = Path(__file__).resolve().parents[1]
IN_TOPIC = "flight.departures.v1"
OUT_TOPIC = "flight.delay_risk.v1"
GROUP_ID = "delay-risk-scorer"
IN_SCHEMA = (Path(__file__).resolve().parent / "schemas/departure.avsc").read_text()
OUT_SCHEMA = (Path(__file__).resolve().parent / "schemas/delay_risk.avsc").read_text()
ARTIFACT_ROOT = REPO / "ml/artifacts"
CHUNK = 5_000
EPOCH_UTC = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)

# mart weather column <- ISD export column (silver names; unit conversion,
# visibility censoring, and the gust indicator are already applied upstream)
WEATHER_COLUMNS = {
    "origin_temp_f": "temp_f",
    "origin_dewpoint_f": "dewpoint_f",
    "origin_wind_speed_kn": "wind_speed_kn",
    "origin_gust_kn": "gust_kn",
    "origin_gust_reported": "gust_reported",
    "origin_visibility_mi": "visibility_mi",
    "origin_precip_1h_in": "precip_1h_in",
    "origin_had_fog": "had_fog",
    "origin_had_rain_drizzle": "had_rain_drizzle",
    "origin_had_snow_ice_pellets": "had_snow_ice_pellets",
    "origin_had_thunder": "had_thunder",
}

# the swap-shaped NULL rotation block: enrich()'s default when no linkage is
# supplied (H2 stub semantics, kept for the credential-free tests); the live
# consumer overlays the state machine's real block per event. Everything in
# the rotation feature set is NULL except origin_dep_density_hour, the
# schedule aggregate training keeps for every row (ml/features.py comment).
ROTATION_STUB = (
    "hist_turnaround_band_arr_del15_rate",
    "hist_turnaround_band_avg_arr_delay_minutes",
    "hist_turnaround_band_n_flights",
    "hist_rotation_position_arr_del15_rate",
    "hist_rotation_position_avg_arr_delay_minutes",
    "hist_rotation_position_n_flights",
    "rotation_position",
    "legs_today",
    "has_inbound_leg",
    "sched_turnaround_min",
    "sched_turnaround_slack_min",
    "is_tight_turnaround",
    "inbound_distance",
    "inbound_crs_elapsed_min",
)

HIST_GRAINS = ("route", "carrier", "origin", "dest")
HIST_SUFFIXES = ("arr_del15_rate", "avg_arr_delay_minutes", "n_flights")


class SchemaMismatchError(RuntimeError):
    """The assembled features do not match the model's stored schema."""


@dataclass
class Lookups:
    hist: dict = field(default_factory=dict)
    route_distance: dict = field(default_factory=dict)
    category_order: dict = field(default_factory=dict)
    category_vocab: dict = field(default_factory=dict)
    density: dict = field(default_factory=dict)
    typical_density: float = math.nan
    station_for: dict = field(default_factory=dict)
    # station_id -> (sorted obs ts ms int64 array, feature matrix float64)
    weather: dict = field(default_factory=dict)
    # band key / position key -> {rate, avg_min, n} (entity_profile's
    # turnaround_band and rotation_position levels; ml/serving.py:294-299)
    rot_band: dict = field(default_factory=dict)
    rot_pos: dict = field(default_factory=dict)


def load_lookups() -> Lookups:
    L = Lookups()
    ep = pd.read_parquet(REPO / "data/lookups/entity_profile.parquet")
    L.hist = {g: {} for g in HIST_GRAINS}

    def _f(v: object) -> float:  # nullable pandas scalars -> plain float
        return math.nan if pd.isna(v) else float(v)

    rotation_levels = {"turnaround_band": L.rot_band, "rotation_position": L.rot_pos}
    for r in ep.itertuples(index=False):
        if r.entity_level in L.hist:
            L.hist[r.entity_level][r.entity_key] = {
                f"hist_{r.entity_level}_arr_del15_rate": _f(r.hist_arr_del15_rate),
                f"hist_{r.entity_level}_avg_arr_delay_minutes": _f(r.hist_avg_arr_delay_minutes),
                f"hist_{r.entity_level}_n_flights": _f(r.hist_n_flights),
            }
            if r.entity_level == "route" and r.distance is not None and not pd.isna(r.distance):
                L.route_distance[r.entity_key] = float(r.distance)
        elif r.entity_level in rotation_levels:
            rotation_levels[r.entity_level][r.entity_key] = {
                "rate": _f(r.hist_arr_del15_rate),
                "avg_min": _f(r.hist_avg_arr_delay_minutes),
                "n": _f(r.hist_n_flights),
            }
    missing = [g for g in HIST_GRAINS if not L.hist[g]]
    if missing:
        raise SystemExit(f"entity_profile has no rows for level(s) {missing}")
    # training vocabulary per categorical = the entity keys at that level;
    # sorted once so pd.Categorical gets a stable order (ml/serving.py:310)
    L.category_vocab = {g: set(L.hist[g]) for g in ("carrier", "origin", "dest", "route")}
    L.category_order = {g: sorted(v) for g, v in L.category_vocab.items()}

    dp = pd.read_parquet(REPO / "data/lookups/density_profile.parquet")
    L.density = {
        (r.origin, int(r.crs_dep_hour), int(r.day_of_week)): float(r.density_median)
        for r in dp.itertuples(index=False)
    }
    typ = pd.read_parquet(REPO / "data/lookups/typical_rotation.parquet")
    L.typical_density = float(typ["typical_density"].iloc[0])

    smap = pd.read_parquet(REPO / "data/lookups/airport_station_map.parquet")
    L.station_for = dict(zip(smap["iata"], smap["station_id"], strict=True))

    # obs_date is a BigQuery dbdate extension column; ignore_metadata stops
    # pyarrow consulting the embedded pandas schema, so the read needs no
    # db-dtypes dependency
    wx = (
        pq.read_table(
            REPO / "data/weather/isd_week.parquet",
            columns=["station_id", "obs_ts_utc", *WEATHER_COLUMNS.values()],
        )
        .to_pandas(ignore_metadata=True)
        .sort_values(["station_id", "obs_ts_utc"], kind="mergesort")
    )
    # timestamp[us] -> epoch milliseconds (the event-time unit everywhere here)
    ts_ms = (wx["obs_ts_utc"].astype("int64") // 1_000).to_numpy()
    mat = wx[list(WEATHER_COLUMNS.values())].astype("float64").to_numpy()
    sid = wx["station_id"].to_numpy()
    starts = np.flatnonzero(np.r_[True, sid[1:] != sid[:-1]])
    bounds = np.r_[starts, len(sid)]
    L.weather = {
        sid[a]: (ts_ms[a:b], mat[a:b]) for a, b in zip(bounds[:-1], bounds[1:], strict=True)
    }
    return L


def load_scoring_artifacts(run_dir: Path | None = None) -> tuple[xgb.XGBClassifier, object, str]:
    """The two artifacts scoring needs, from the newest run that has both.

    A deliberate port of ml/serving.load_models rather than a reuse: that
    loader requires all FOUR artifacts (including the 438 MB regressor this
    consumer never calls), which a fresh clone fetched with phase 1 of
    scripts/fetch_artifacts.sh does not have. The stored-schema assertion is
    kept identical (ml/serving.py:202-208).
    """
    if run_dir is None:
        wanted = ("xgb_classifier.ubj", "calibrator.joblib")
        runs = sorted(
            d
            for d in ARTIFACT_ROOT.iterdir()
            if d.is_dir() and all((d / n).exists() for n in wanted)
        ) if ARTIFACT_ROOT.is_dir() else []
        if not runs:
            raise SystemExit(
                f"no artifact run with classifier + calibrator under {ARTIFACT_ROOT} — "
                "run `bash scripts/fetch_artifacts.sh` (phase 1 suffices)"
            )
        run_dir = runs[-1]
    clf = xgb.XGBClassifier()
    clf.load_model(run_dir / "xgb_classifier.ubj")
    calibrator = joblib.load(run_dir / "calibrator.joblib")
    stored = clf.get_booster().feature_names
    if stored != list(f.FEATURES):
        raise SchemaMismatchError(
            f"xgb classifier stored schema != canonical FEATURES; "
            f"stored={stored} expected={list(f.FEATURES)}"
        )
    return clf, calibrator, run_dir.name


@lru_cache(maxsize=512)
def _holiday_flags(d: dt.date) -> tuple[float, float, float]:
    """Same library and window as the training calendar (ml/serving.py:540)."""
    import holidays

    us = holidays.country_holidays("US", years=range(d.year - 1, d.year + 2))
    return (
        float(d in us),
        float(d + dt.timedelta(days=1) in us),
        float(d - dt.timedelta(days=1) in us),
    )


def _weather_at(L: Lookups, origin: str, dep_ms: int) -> np.ndarray | None:
    """Last observation at or before scheduled departure, inside the staleness
    window (obs > dep - ceiling, obs <= dep; ml_flight_features.sql:246-250)."""
    station = L.station_for.get(origin)
    if station is None:
        return None
    hit = L.weather.get(station)
    if hit is None:
        return None
    ts, mat = hit
    i = bisect_right(ts, dep_ms) - 1
    if i < 0 or ts[i] <= dep_ms - c.WEATHER_STALENESS_HOURS * 3_600_000:
        return None
    return mat[i]


def enrich(ev: dict, L: Lookups, rot: Linkage | None = None) -> tuple[dict, str]:
    """One departure event -> (51-feature row dict, weather_basis).

    rot is the state machine's classified linkage for this event; None means
    the swap-shaped NULL default (the H2 stub shape, kept for tests that
    exercise enrichment without rotation state)."""
    route = ev["origin"] + "-" + ev["dest"]
    d: dt.date = ev["flight_date"]
    hour = int(ev["crs_dep_time"][:2])
    dist = ev.get("distance_mi")
    row: dict[str, object] = {
        "carrier": ev["carrier"],
        "origin": ev["origin"],
        "dest": ev["dest"],
        "route": route,
        "distance": (
            float(dist)
            if dist is not None and math.isfinite(dist) and dist > 0
            else L.route_distance.get(route, math.nan)
        ),
        "crs_dep_hour": float(hour),
        "crs_arr_hour": (
            float(int(ev["crs_arr_time"][:2])) if ev.get("crs_arr_time") else math.nan
        ),
        "day_of_week": float(d.isoweekday()),  # BTS: 1 = Monday
        "month": float(d.month),
    }
    for grain in HIST_GRAINS:
        key = route if grain == "route" else ev[grain]
        entity = L.hist[grain].get(key, {})
        for s in HIST_SUFFIXES:
            # `or nan` matches serving exactly; smoothed rates are never 0.0
            row[f"hist_{grain}_{s}"] = float(entity.get(f"hist_{grain}_{s}", math.nan) or math.nan)

    wx = _weather_at(L, ev["origin"], ev["crs_dep_ts_ms"])
    if wx is not None:
        row.update(zip(WEATHER_COLUMNS, wx, strict=True))
        # calm-hours encoding: an observation without a gust group is 0.0, the
        # indicator carries the distinction (ml_flight_features.sql:308-311)
        if pd.isna(row["origin_gust_kn"]):
            row["origin_gust_kn"] = 0.0
        row["has_origin_weather"] = 1.0
        weather_basis = "observed"
    else:
        row.update(dict.fromkeys(WEATHER_COLUMNS, math.nan))
        row["has_origin_weather"] = 0.0
        weather_basis = "null_path"

    hol = _holiday_flags(d)
    row["is_holiday"], row["is_day_before_holiday"], row["is_day_after_holiday"] = hol

    # kept for every row, swap-shaped or not (ml/features.py block comment)
    row["origin_dep_density_hour"] = L.density.get(
        (ev["origin"], hour, d.isoweekday()), L.typical_density
    )
    row.update(dict.fromkeys(ROTATION_STUB, math.nan))
    if rot is not None:
        row.update(rot.features)
        for key, table, grain in (
            (rot.band_key, L.rot_band, "turnaround_band"),
            (rot.position_key, L.rot_pos, "rotation_position"),
        ):
            entity = table.get(key, {}) if key is not None else {}
            # `or nan` matches ml/serving.py:_rotation_features exactly
            row[f"hist_{grain}_arr_del15_rate"] = float(entity.get("rate") or math.nan)
            row[f"hist_{grain}_avg_arr_delay_minutes"] = float(entity.get("avg_min") or math.nan)
            row[f"hist_{grain}_n_flights"] = float(entity.get("n") or math.nan)
    return row, weather_basis


def build_frame(rows: list[dict], L: Lookups, booster_names: list[str]) -> pd.DataFrame:
    """Frame construction + the serving schema gates (ml/serving.py:744-789)."""
    unpopulated = [col for col in f.FEATURES if col not in rows[0]]
    if unpopulated:
        raise SchemaMismatchError(f"assembly did not populate features: {unpopulated}")
    extra = set(rows[0]) - set(f.FEATURES)
    if extra:
        raise SchemaMismatchError(f"assembly produced non-FEATURES columns: {sorted(extra)}")
    x = pd.DataFrame(rows, columns=list(f.FEATURES))
    for col in f.CATEGORICAL_FEATURES:
        # unseen values become the missing category (xgboost >= 3 raises on
        # categories absent from the trained encoder)
        x[col] = pd.Categorical(x[col], categories=L.category_order[col])
    for col in f.NUMERIC_FEATURES:
        x[col] = pd.to_numeric(x[col]).astype("float32")
    if list(x.columns) != list(f.FEATURES):
        raise SchemaMismatchError(f"assembled columns {list(x.columns)} != FEATURES")
    if list(x.columns) != booster_names:
        raise SchemaMismatchError(f"assembled columns != booster schema {booster_names}")
    return x


def _decode_event(raw: dict) -> dict:
    """Normalize the Avro-decoded departure: logical types arrive as
    datetime.date / tz-aware datetime; keep both the datetime and epoch ms."""
    ts = raw["crs_dep_ts_utc"]
    if isinstance(ts, dt.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=dt.UTC)
        ms = int(ts.timestamp() * 1000)
    else:  # already epoch ms
        ms = int(ts)
        ts = EPOCH_UTC + dt.timedelta(milliseconds=ms)
    fd = raw["flight_date"]
    if not isinstance(fd, dt.date):
        fd = (EPOCH_UTC + dt.timedelta(days=int(fd))).date()
    return {**raw, "flight_date": fd, "crs_dep_ts_utc": ts, "crs_dep_ts_ms": ms}


class Scorer:
    def __init__(self, alerts_path: Path | None = None) -> None:
        self.lookups = load_lookups()
        self.rotation = RotationTracker(load_day_leg_counts())
        self.clf, self.calibrator, self.model_run_id = load_scoring_artifacts()
        # the run's whole artifact: truncate on start so two identical replay
        # runs produce byte-identical files (docs/schemas.md contract 4)
        self.alerts_path = alerts_path or REPO / "alerts.jsonl"
        self._alerts = self.alerts_path.open("w", encoding="utf-8")
        self.booster_names = list(self.clf.get_booster().feature_names)
        sr = SchemaRegistryClient({"url": registry_url()})
        self.deserialize = AvroDeserializer(sr, IN_SCHEMA)
        self.serialize = AvroSerializer(sr, OUT_SCHEMA, conf={"auto.register.schemas": False})
        self.serialize_key = StringSerializer("utf_8")
        self.out_ctx = SerializationContext(OUT_TOPIC, MessageField.VALUE)
        self.key_ctx = SerializationContext(OUT_TOPIC, MessageField.KEY)
        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap(),
                "enable.idempotence": True,
                "compression.type": "none",
            }
        )
        self.delivery_errors: list[str] = []
        self.scored = 0
        self.alerts = 0
        self.bases: dict[str, int] = {}

    def _on_delivery(self, err, _msg) -> None:
        if err is not None:
            self.delivery_errors.append(str(err))

    def finalize_alerts(self) -> None:
        """Rewrite the completed run's artifact in canonical event-time order.

        Alert lines stream out in Kafka poll order, and partition interleave
        varies run to run — the double-run gate caught two identical replays
        producing identical LINES in different ORDER. The mid-run file stays
        append-ordered (crash-safe, flushed before each offset commit); a
        cleanly completed batch run rewrites it sorted by (issued_at, flight
        identity), which is where the byte-identical guarantee lives.
        """
        self._alerts.close()
        records = [
            json.loads(line)
            for line in self.alerts_path.read_text(encoding="utf-8").splitlines()
        ]
        records.sort(
            key=lambda r: (r["issued_at"], r["carrier"], r["flight_number"],
                           r["origin"], r["dest"])
        )
        with self.alerts_path.open("w", encoding="utf-8") as out:
            for r in records:
                out.write(json.dumps(r, sort_keys=True) + "\n")

    def _write_alert(self, ev: dict, p: float) -> None:
        """One JSON line per alert (docs/schemas.md contract 4). issued_at is
        T, event time — never wall clock — so replays are byte-identical."""
        line = json.dumps(
            {
                "flight_date": ev["flight_date"].isoformat(),
                "carrier": ev["carrier"],
                "flight_number": ev["flight_number"],
                "origin": ev["origin"],
                "dest": ev["dest"],
                "crs_dep_time": ev["crs_dep_time"],
                "delay_probability": p,
                "risk_band": c.risk_band(p),
                "threshold": c.ALERT_THRESHOLD,
                "issued_at": ev["crs_dep_ts_utc"].isoformat(),
                "mode": ev["mode"],
            },
            sort_keys=True,
        )
        self._alerts.write(line + "\n")

    def score_chunk(self, events: list[dict]) -> None:
        if not events:
            return
        rows, weather_bases, bases = [], [], []
        for ev in events:
            # exactly one observe() per event, in poll order — per-tail
            # ordering within a partition is what makes the state valid
            rot = self.rotation.observe(ev)
            row, wb = enrich(ev, self.lookups, rot)
            rows.append(row)
            weather_bases.append(wb)
            bases.append(basis_for(rot.link_class, ev["is_warmup"]))
        x = build_frame(rows, self.lookups, self.booster_names)
        # raw scores are recall-inflated; the Platt map is strictly monotonic,
        # so calibrated p is a frequency and ranking metrics are unchanged
        p_cal = self.calibrator.transform(self.clf.predict_proba(x)[:, 1])
        for ev, wb, basis, p in zip(events, weather_bases, bases, p_cal, strict=True):
            p = float(p)
            alert = p >= c.ALERT_THRESHOLD
            if alert:
                self._write_alert(ev, p)
            out = {
                "flight_date": ev["flight_date"],
                "carrier": ev["carrier"],
                "flight_number": ev["flight_number"],
                "origin": ev["origin"],
                "dest": ev["dest"],
                "crs_dep_time": ev["crs_dep_time"],
                "tail_number": ev["tail_number"],
                "scored_at_ts_utc": ev["crs_dep_ts_utc"],  # T, event time
                "delay_probability": p,
                "risk_band": c.risk_band(p),
                "alert": alert,
                "model_run_id": self.model_run_id,
                "calibration": "platt",
                "rotation_state_basis": basis,
                "weather_basis": wb,
                "taf_horizon_bin": None,
                "pressure_late_arrivals": None,
                "pressure_cancellations": None,
            }
            key = ev["tail_number"] or c.NULL_TAIL_SENTINEL_KEY
            self.producer.produce(
                topic=OUT_TOPIC,
                key=self.serialize_key(key, self.key_ctx),
                value=self.serialize(out, self.out_ctx),
                partition=partition_for(key),
                timestamp=ev["crs_dep_ts_ms"],
                on_delivery=self._on_delivery,
            )
            self.producer.poll(0)
            self.scored += 1
            self.alerts += int(alert)
            self.bases[basis] = self.bases.get(basis, 0) + 1
        self.producer.flush()
        # alert lines land on disk before the chunk's offsets can commit —
        # the same process-first-commit-second discipline as the risk topic
        self._alerts.flush()
        if self.delivery_errors:
            raise SystemExit(f"delivery failed for {len(self.delivery_errors)} events: "
                             f"{self.delivery_errors[:3]}")


def run(follow: bool, group: str) -> None:
    scorer = Scorer()
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap(),
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    assigned: set[tuple[str, int]] = set()
    consumer.subscribe(
        [IN_TOPIC],
        on_assign=lambda _c, parts: assigned.update((p.topic, p.partition) for p in parts),
    )
    at_eof: set[tuple[str, int]] = set()
    buffer: list[dict] = []
    uncommitted = 0

    def drain() -> None:
        # process first, commit second: the committed offset only ever moves
        # past events whose scored results are flushed to the risk topic
        nonlocal uncommitted
        scorer.score_chunk(buffer)
        buffer.clear()
        if uncommitted:  # commit with nothing consumed raises _NO_OFFSET
            consumer.commit(asynchronous=False)
            uncommitted = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if follow and buffer:
                    drain()
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    at_eof.add((msg.topic(), msg.partition()))
                    if not follow and assigned and at_eof >= assigned:
                        drain()
                        break
                    continue
                raise SystemExit(f"consumer error: {msg.error()}")
            at_eof.discard((msg.topic(), msg.partition()))
            buffer.append(_decode_event(scorer.deserialize(msg.value(),
                          SerializationContext(IN_TOPIC, MessageField.VALUE))))
            uncommitted += 1
            if len(buffer) >= CHUNK:
                drain()
    finally:
        consumer.close()

    scorer.finalize_alerts()  # clean batch completion: canonical order
    print(
        f"scored {scorer.scored} events to {OUT_TOPIC} "
        f"(alerts {scorer.alerts} at p>={c.ALERT_THRESHOLD} -> {scorer.alerts_path.name}, "
        f"bases {scorer.bases}, model {scorer.model_run_id}, day-legs lookup misses "
        f"{scorer.rotation.day_legs_misses})"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--follow", action="store_true",
                    help="keep consuming after EOF instead of exiting (live mode)")
    ap.add_argument("--group", default=GROUP_ID)
    args = ap.parse_args()
    run(follow=args.follow, group=args.group)


if __name__ == "__main__":
    main()
