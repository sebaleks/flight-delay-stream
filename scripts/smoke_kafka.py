"""Stack smoke test: prove the local Kafka + Schema Registry round-trip works.

Creates a THROWAWAY topic, produces one message and consumes it back, registers
a trivial Avro schema under a THROWAWAY subject and fetches it back, printing
PASS per step, then deletes both. Never touches the three real topics or their
subjects (H1 constraint); the guard below enforces that by construction.

    uv run --extra kafka python scripts/smoke_kafka.py
"""

from __future__ import annotations

import json
import os
import sys
import time

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

SMOKE_TOPIC = "smoke.kafka.throwaway.v1"
SMOKE_SUBJECT = "smoke.kafka.throwaway-value"
REAL_TOPICS = {"flight.departures.v1", "flight.outcomes.v1", "flight.delay_risk.v1"}

SMOKE_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "SmokeCheck",
        "namespace": "smoke",
        "fields": [{"name": "message", "type": "string"}],
    }
)

PAYLOAD = b'{"message": "smoke"}'


def bootstrap() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def registry_url() -> str:
    return os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def guard() -> None:
    if SMOKE_TOPIC in REAL_TOPICS or SMOKE_SUBJECT.startswith(tuple(REAL_TOPICS)):
        sys.exit("smoke names collide with real topics; refusing to run")


def ensure_fresh_topic(admin: AdminClient) -> None:
    if SMOKE_TOPIC in set(admin.list_topics(timeout=10).topics):
        admin.delete_topics([SMOKE_TOPIC])[SMOKE_TOPIC].result(timeout=30)
        for _ in range(30):
            if SMOKE_TOPIC not in set(admin.list_topics(timeout=10).topics):
                break
            time.sleep(1)
    admin.create_topics([NewTopic(SMOKE_TOPIC, num_partitions=1, replication_factor=1)])[
        SMOKE_TOPIC
    ].result(timeout=30)


def produce_one() -> None:
    errors: list[str] = []
    p = Producer({"bootstrap.servers": bootstrap()})
    p.produce(
        SMOKE_TOPIC,
        value=PAYLOAD,
        on_delivery=lambda err, msg: errors.append(str(err)) if err else None,
    )
    remaining = p.flush(10)
    if remaining or errors:
        sys.exit(f"FAIL produce: {remaining} undelivered, errors={errors}")
    print("PASS produce: 1 message delivered to", SMOKE_TOPIC)


def consume_one() -> None:
    c = Consumer(
        {
            "bootstrap.servers": bootstrap(),
            # unique group so reruns never inherit committed offsets
            "group.id": f"smoke-{int(time.time())}",
            "auto.offset.reset": "earliest",
        }
    )
    c.subscribe([SMOKE_TOPIC])
    deadline = time.time() + 30
    try:
        while time.time() < deadline:
            msg = c.poll(1.0)
            if msg is None or msg.error():
                continue
            if msg.value() != PAYLOAD:
                sys.exit(f"FAIL consume: payload mismatch {msg.value()!r}")
            print("PASS consume: message read back byte-identical")
            return
        sys.exit("FAIL consume: no message within 30s")
    finally:
        c.close()


def register_schema(sr: SchemaRegistryClient) -> None:
    schema_id = sr.register_schema(SMOKE_SUBJECT, Schema(SMOKE_SCHEMA, "AVRO"))
    print(f"PASS register: {SMOKE_SUBJECT} schema id {schema_id}")


def fetch_schema(sr: SchemaRegistryClient) -> None:
    got = sr.get_latest_version(SMOKE_SUBJECT)
    want = json.loads(SMOKE_SCHEMA)
    have = json.loads(got.schema.schema_str)
    if (have.get("name"), have.get("fields")) != (want["name"], want["fields"]):
        sys.exit(f"FAIL fetch: schema mismatch {have}")
    print(f"PASS fetch: {SMOKE_SUBJECT} version {got.version} matches")


def cleanup(admin: AdminClient, sr: SchemaRegistryClient) -> None:
    admin.delete_topics([SMOKE_TOPIC])[SMOKE_TOPIC].result(timeout=30)
    sr.delete_subject(SMOKE_SUBJECT)
    print("cleanup: throwaway topic and subject deleted")


def main() -> None:
    guard()
    admin = AdminClient({"bootstrap.servers": bootstrap()})
    sr = SchemaRegistryClient({"url": registry_url()})
    ensure_fresh_topic(admin)
    produce_one()
    consume_one()
    register_schema(sr)
    fetch_schema(sr)
    cleanup(admin, sr)


if __name__ == "__main__":
    main()
