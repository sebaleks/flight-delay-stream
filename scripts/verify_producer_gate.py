"""The step-4 gate: two same-seed replay runs are byte-identical.

Recreates flight.departures.v1, runs the full producer, consumes everything
back, and hashes the (key, value) byte sequence canonically (partitions in
order 0..5, offsets ascending within each). Repeats, compares the digests,
and prints PASS/FAIL. The registry state is untouched between runs, so the
Avro wire format (magic byte + schema id + payload) is comparable.

    uv run --extra kafka --extra ml python scripts/verify_producer_gate.py
"""

from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, TopicPartition

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from streaming.admin import PARTITIONS, bootstrap  # noqa: E402

TOPIC = "flight.departures.v1"
SEED = 42


def sh(*args: str) -> None:
    subprocess.run(
        ["uv", "run", "--extra", "kafka", "--extra", "ml", *args],
        check=True, cwd=REPO,
    )


def consume_digest() -> tuple[str, int]:
    consumer = Consumer({
        "bootstrap.servers": bootstrap(),
        "group.id": f"gate-{hashlib.sha1(str(id(object())).encode()).hexdigest()[:8]}",
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
    })
    per_partition: dict[int, list[bytes]] = {p: [] for p in range(PARTITIONS)}
    consumer.assign([TopicPartition(TOPIC, p, 0) for p in range(PARTITIONS)])
    eof, count = set(), 0
    while len(eof) < PARTITIONS:
        msg = consumer.poll(10.0)
        if msg is None:
            raise SystemExit("consumer stalled before reaching every partition EOF")
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof.add(msg.partition())
                continue
            raise SystemExit(f"consume error: {msg.error()}")
        per_partition[msg.partition()].append(
            (msg.key() or b"") + b"\x00" + (msg.value() or b"") + b"\x01"
        )
        count += 1
    consumer.close()
    digest = hashlib.sha256()
    for p in range(PARTITIONS):
        for chunk in per_partition[p]:
            digest.update(chunk)
    return digest.hexdigest(), count


def one_run() -> tuple[str, int]:
    sh("python", "-m", "streaming.admin", "--recreate", TOPIC)
    sh("python", "-m", "streaming.producer", "--seed", str(SEED))
    return consume_digest()


def main() -> None:
    d1, n1 = one_run()
    print(f"run 1: {n1} events, sha256 {d1}")
    d2, n2 = one_run()
    print(f"run 2: {n2} events, sha256 {d2}")
    if (d1, n1) == (d2, n2):
        print(f"GATE PASS: {n1} events, byte-identical across same-seed runs")
    else:
        print("GATE FAIL: runs differ")
        sys.exit(1)


if __name__ == "__main__":
    main()
