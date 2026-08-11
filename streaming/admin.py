"""Topic + contract administration for the local stack.

Creates the three topics with the tail-keying partition layout, registers the
topic value contracts from streaming/schemas/*.avsc at the Schema Registry,
and pins BACKWARD compatibility per subject (enforced by the registry, not by
application code; CLAUDE.md section 2). alert_row.avsc is the file-artifact
contract and is deliberately NOT registered (docs/schemas.md contract 4).

    uv run --extra kafka python -m streaming.admin            # create + register
    uv run --extra kafka python -m streaming.admin --recreate flight.departures.v1

--recreate deletes and re-creates ONE topic (used by the producer determinism
gate to compare two clean runs); contracts are left untouched.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"

# 6 partitions: partition 0 is the NOTAIL sentinel partition (dedicated by the
# producer's explicit partitioner, near-empty by measurement: 104 events in
# the replay week, all cancellations); tails hash across 1-5.
PARTITIONS = 6
TOPICS = {
    "flight.departures.v1": "departure.avsc",
    "flight.outcomes.v1": "outcome.avsc",
    "flight.delay_risk.v1": "delay_risk.avsc",
}
COMPATIBILITY = "BACKWARD"


def bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def create_topics(admin: AdminClient, names: list[str]) -> None:
    existing = set(admin.list_topics(timeout=10).topics)
    wanted = [n for n in names if n not in existing]
    if not wanted:
        print(f"topics already exist: {sorted(names)}")
        return
    futures = admin.create_topics(
        [NewTopic(n, num_partitions=PARTITIONS, replication_factor=1) for n in wanted]
    )
    for name, fut in futures.items():
        fut.result(timeout=30)
        print(f"created topic {name} ({PARTITIONS} partitions)")


def recreate_topic(admin: AdminClient, name: str) -> None:
    if name not in TOPICS:
        sys.exit(f"unknown topic {name}")
    if name in set(admin.list_topics(timeout=10).topics):
        admin.delete_topics([name])[name].result(timeout=30)
        # topic deletion is asynchronous on the broker; wait until gone
        for _ in range(30):
            if name not in set(admin.list_topics(timeout=10).topics):
                break
            time.sleep(1)
        print(f"deleted topic {name}")
    create_topics(admin, [name])


def register_contracts(sr: SchemaRegistryClient) -> None:
    for topic, avsc in TOPICS.items():
        subject = f"{topic}-value"
        schema_str = (SCHEMA_DIR / avsc).read_text()
        json.loads(schema_str)  # fail loudly on malformed JSON before the registry sees it
        schema_id = sr.register_schema(subject, Schema(schema_str, "AVRO"))
        sr.set_compatibility(subject_name=subject, level=COMPATIBILITY)
        got = sr.get_compatibility(subject_name=subject)
        print(f"registered {subject}: schema id {schema_id}, compatibility {got}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--recreate", metavar="TOPIC", default=None)
    args = ap.parse_args()

    admin = AdminClient({"bootstrap.servers": bootstrap()})
    if args.recreate:
        recreate_topic(admin, args.recreate)
        return
    create_topics(admin, list(TOPICS))
    register_contracts(SchemaRegistryClient({"url": registry_url()}))


if __name__ == "__main__":
    main()
