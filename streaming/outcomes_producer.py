"""Outcomes producer: realized truth onto flight.outcomes.v1, late by design.

Emits one Avro event per flight of the replay week (warm-up day included),
keyed exactly like the departures producer (tail or the NOTAIL sentinel, same
explicit partitioner) so a flight's truth lands in the same partition as its
departure. Emission order is ACTUAL-ARRIVAL event time (truth_ts_utc), so
outcomes arrive hours after the departure events they match and interleave
across flights: the late/out-of-order join pattern is the data's own shape,
not injected randomness. Deterministic like the departures producer: fixed
total order, no wall-clock values, event timestamps = truth_ts_utc.

truth_ts_utc construction (approximation, disclosed in docs/schemas.md):
scheduled arrival = scheduled departure UTC + crs_elapsed minutes (the same
timezone-proof construction int_aircraft_rotation.sql uses); actual arrival =
scheduled arrival + arr_delay_minutes (clamped at 0, so early flights read as
on time); cancellations carry scheduled arrival.

    uv run --extra kafka --extra ml python -m streaming.outcomes_producer
    uv run --extra kafka --extra ml python -m streaming.outcomes_producer --limit 500
"""

from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from streaming.admin import bootstrap, registry_url
from streaming.constants import (
    FORBIDDEN_POST_DEPARTURE_FIELDS,
    KNOWABLE_POST_DEPARTURE_EVENT_FIELDS,
    KNOWABLE_SCHEDULE,
    NULL_TAIL_SENTINEL_KEY,
)
from streaming.producer import EPOCH, dep_ts_utc_ms, partition_for, tz_map

REPO = Path(__file__).resolve().parents[1]
TOPIC = "flight.outcomes.v1"
SCHEMA = (Path(__file__).resolve().parent / "schemas/outcome.avsc").read_text()


def load_outcomes() -> pd.DataFrame:
    out = pd.read_parquet(REPO / "data/replay/outcomes_week.parquet")
    tzs = tz_map()
    dep_ms = [dep_ts_utc_ms(r, tzs) for r in out.itertuples(index=False)]
    elapsed = out["crs_elapsed_min"].fillna(0.0)
    delay = out["arr_delay_minutes"].fillna(0.0)  # cancelled/diverted: scheduled arrival
    out["truth_ts_utc"] = (
        pd.Series(dep_ms, index=out.index)
        + ((elapsed + delay) * 60_000).astype("int64")
    )
    # deterministic total order: truth event time, then the unique grain
    return out.sort_values(
        ["truth_ts_utc", "flight_date", "crs_dep_time", "carrier", "flight_number",
         "origin", "dest"],
        kind="mergesort",
    ).reset_index(drop=True)


def to_event(row) -> dict:
    event = {
        "flight_date": (dt.date.fromisoformat(row.flight_date) - EPOCH).days,
        "carrier": row.carrier,
        "flight_number": row.flight_number,
        "origin": row.origin,
        "dest": row.dest,
        "crs_dep_time": row.crs_dep_time,
        "tail_number": row.tail_number if isinstance(row.tail_number, str) else None,
        "arr_del15": None if pd.isna(row.arr_del15) else bool(row.arr_del15),
        "arr_delay_minutes": (
            None if pd.isna(row.arr_delay_minutes) else float(row.arr_delay_minutes)
        ),
        "cancelled": bool(row.cancelled),
        "diverted": bool(row.diverted),
        "truth_ts_utc": int(row.truth_ts_utc),
    }
    # contract sanity: outcome fields are exactly identity + post-departure truth
    outside = set(event) - KNOWABLE_SCHEDULE - KNOWABLE_POST_DEPARTURE_EVENT_FIELDS
    if outside:
        raise SystemExit(f"outcome event carries unclassified field(s): {sorted(outside)}")
    assert set(event) & FORBIDDEN_POST_DEPARTURE_FIELDS, "outcome event must carry truth fields"
    return event


def run(seed: int, limit: int | None, speed: float) -> int:
    out = load_outcomes()
    sr = SchemaRegistryClient({"url": registry_url()})
    serialize = AvroSerializer(sr, SCHEMA, conf={"auto.register.schemas": False})
    serialize_key = StringSerializer("utf_8")
    producer = Producer({
        "bootstrap.servers": bootstrap(),
        "enable.idempotence": True,
        "compression.type": "none",
    })
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    kctx = SerializationContext(TOPIC, MessageField.KEY)

    produced, prev_ts = 0, None
    for row in out.iloc[:limit].itertuples(index=False) if limit else out.itertuples(index=False):
        event = to_event(row)
        key = event["tail_number"] or NULL_TAIL_SENTINEL_KEY
        if speed > 0 and prev_ts is not None:
            time.sleep(max(0.0, (event["truth_ts_utc"] - prev_ts) / 1000.0 / speed))
        prev_ts = event["truth_ts_utc"]
        while True:
            try:
                producer.produce(
                    topic=TOPIC,
                    key=serialize_key(key, kctx),
                    value=serialize(event, ctx),
                    partition=partition_for(key),
                    timestamp=event["truth_ts_utc"],
                )
                break
            except BufferError:  # local queue full: drain, then retry
                producer.flush()
        producer.poll(0)
        produced += 1
    producer.flush()
    print(f"produced {produced} outcome events to {TOPIC} (seed {seed}, "
          f"speed {speed or 'max'})")
    return produced


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--seed", type=int, default=0, help="recorded; the stream has no randomness")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--speed", type=float, default=0.0)
    args = ap.parse_args()
    run(seed=args.seed, limit=args.limit, speed=args.speed)


if __name__ == "__main__":
    main()
