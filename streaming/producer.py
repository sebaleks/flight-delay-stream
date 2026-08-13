"""Replay producer: the committed week onto flight.departures.v1.

Reads data/replay/departures_week.parquet in scheduled-departure order and
produces one Avro event per flight against the registered contract (the
registry validates; auto-registration is OFF, so an unregistered or drifted
schema fails loudly). Deterministic by construction: a fixed total order, no
wall-clock values in any field, event timestamps = scheduled departure UTC.
Two runs with the same seed produce byte-identical event sequences; the gate
in scripts/verify_producer_gate.py proves it.

    uv run --extra kafka --extra ml python -m streaming.producer            # full week
    uv run --extra kafka --extra ml python -m streaming.producer --limit 500
    uv run --extra kafka --extra ml python -m streaming.producer --speed 600 --resume

--speed N replays scheduled time N times faster than real time (0 = as fast
as possible, the default). --resume continues from var/producer_checkpoint.json.
--seed is recorded in the checkpoint and reserved for future jitter; the
event stream itself carries no randomness.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import time
import zlib
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from streaming.admin import bootstrap, registry_url
from streaming.constants import KNOWABLE_SCHEDULE, NULL_TAIL_SENTINEL_KEY

REPO = Path(__file__).resolve().parents[1]
TOPIC = "flight.departures.v1"
SCHEMA = (Path(__file__).resolve().parent / "schemas/departure.avsc").read_text()
CHECKPOINT = REPO / "var/producer_checkpoint.json"
CHECKPOINT_EVERY = 5_000
EPOCH = dt.date(1970, 1, 1)


def load_week() -> tuple[pd.DataFrame, dt.date]:
    dep = pd.read_parquet(REPO / "data/replay/departures_week.parquet")
    # deterministic total order: scheduled-departure order, then the mart's
    # tested-unique grain as the tiebreak (crs_dep_time is zero-padded HHMM,
    # so lexicographic == chronological)
    dep = dep.sort_values(
        ["flight_date", "crs_dep_time", "carrier", "flight_number", "origin", "dest"],
        kind="mergesort",
    ).reset_index(drop=True)
    warmup_day = dt.date.fromisoformat(
        json.loads((REPO / "data/week_choice.json").read_text())["week_start"]
    ) - dt.timedelta(days=1)
    return dep, warmup_day


def tz_map() -> dict[str, ZoneInfo]:
    air = pd.read_parquet(REPO / "data/lookups/airports.parquet")
    return {r.iata: ZoneInfo(r.tz) for r in air.itertuples() if isinstance(r.tz, str)}


def dep_ts_utc_ms(row, tzs: dict[str, ZoneInfo]) -> int:
    tz = tzs.get(row.origin)
    if tz is None:
        # every flown airport carries a tz (the 681 timezone guard); failing
        # loudly beats silently shifting a schedule
        raise SystemExit(f"origin {row.origin} has no timezone in data/lookups/airports.parquet")
    d = dt.date.fromisoformat(row.flight_date)
    local = dt.datetime(d.year, d.month, d.day, int(row.crs_dep_time[:2]),
                        int(row.crs_dep_time[2:]), tzinfo=tz)
    return int(local.timestamp() * 1000)


def partition_for(key: str) -> int:
    """Partition 0 is DEDICATED to the NOTAIL sentinel; real tails hash over
    1..5. crc32 is stable across runs, platforms, and Python versions, which
    the determinism gate depends on."""
    if key == NULL_TAIL_SENTINEL_KEY:
        return 0
    return 1 + zlib.crc32(key.encode()) % 5


def to_event(row, tzs: dict[str, ZoneInfo], warmup_day: dt.date) -> dict:
    d = dt.date.fromisoformat(row.flight_date)
    event = {
        "flight_date": (d - EPOCH).days,
        "carrier": row.carrier,
        "flight_number": row.flight_number,
        "origin": row.origin,
        "dest": row.dest,
        "crs_dep_time": row.crs_dep_time,
        "crs_dep_ts_utc": dep_ts_utc_ms(row, tzs),
        "crs_arr_time": row.crs_arr_time if isinstance(row.crs_arr_time, str) else None,
        "crs_elapsed_min": None if pd.isna(row.crs_elapsed_min) else float(row.crs_elapsed_min),
        "distance_mi": None if pd.isna(row.distance_mi) else float(row.distance_mi),
        "tail_number": row.tail_number if isinstance(row.tail_number, str) else None,
        "mode": "replay",
        "is_warmup": d == warmup_day,
    }
    forbidden = set(event) - KNOWABLE_SCHEDULE
    if forbidden:  # the producer-side leakage assertion: schedule fields only
        raise SystemExit(f"departure event carries non-schedule field(s): {sorted(forbidden)}")
    return event


def run(seed: int, limit: int | None, speed: float, resume: bool) -> int:
    dep, warmup_day = load_week()
    tzs = tz_map()

    start_index = 0
    if resume and CHECKPOINT.exists():
        cp = json.loads(CHECKPOINT.read_text())
        if cp.get("seed") != seed:
            raise SystemExit(f"checkpoint seed {cp.get('seed')} != --seed {seed}")
        start_index = cp["next_index"]
        print(f"resuming from event {start_index}")

    sr = SchemaRegistryClient({"url": registry_url()})
    serialize = AvroSerializer(sr, SCHEMA, conf={"auto.register.schemas": False})
    serialize_key = StringSerializer("utf_8")
    producer = Producer({
        "bootstrap.servers": bootstrap(),
        "enable.idempotence": True,
        "compression.type": "none",
    })
    delivery_errors: list[str] = []

    def on_delivery(err, _msg) -> None:
        # without a callback, failed deliveries vanish and "produced N" lies
        if err is not None:
            delivery_errors.append(str(err))

    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    kctx = SerializationContext(TOPIC, MessageField.KEY)
    produced, prev_ts = 0, None
    rows = dep.iloc[start_index : (start_index + limit) if limit else None]
    for i, row in enumerate(rows.itertuples(index=False), start=start_index):
        event = to_event(row, tzs, warmup_day)
        key = event["tail_number"] or NULL_TAIL_SENTINEL_KEY
        if speed > 0 and prev_ts is not None:
            time.sleep(max(0.0, (event["crs_dep_ts_utc"] - prev_ts) / 1000.0 / speed))
        prev_ts = event["crs_dep_ts_utc"]
        producer.produce(
            topic=TOPIC,
            key=serialize_key(key, kctx),
            value=serialize(event, ctx),
            partition=partition_for(key),
            timestamp=event["crs_dep_ts_utc"],
            on_delivery=on_delivery,
        )
        producer.poll(0)
        produced += 1
        if produced % CHECKPOINT_EVERY == 0:
            producer.flush()
            CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
            CHECKPOINT.write_text(json.dumps({"next_index": i + 1, "seed": seed}))
    producer.flush()
    if delivery_errors:
        raise SystemExit(f"delivery failed for {len(delivery_errors)} events: "
                         f"{delivery_errors[:3]}")
    CHECKPOINT.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT.write_text(json.dumps({"next_index": start_index + produced, "seed": seed}))
    print(f"produced {produced} events to {TOPIC} "
          f"(seed {seed}, speed {speed or 'max'}, warmup day {warmup_day})")
    return produced


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--speed", type=float, default=0.0)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    run(seed=args.seed, limit=args.limit, speed=args.speed, resume=args.resume)


if __name__ == "__main__":
    main()
