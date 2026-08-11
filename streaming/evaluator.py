"""Outcome-join evaluator: alert precision/recall from risk x truth.

The join is TTL-bounded and nothing is ever silently dropped: every risk and
outcome event ends up in exactly one counter. Cancelled flights are matched
to their outcome and then EXCLUDED from the metrics into their own category
(`excluded_cancelled`): ArrDel15 is undefined for a cancellation, so it is
neither a hit nor a false alarm, and the counter split keeps "correctly
unmatched because cancelled" visibly distinct from "missing or late outcome"
(docs/PLAN.md kickoff decision 8). Warm-up-day events are excluded from
evaluation as specified (`excluded_warmup`).

DETERMINISM. The replay is finite, so the shell consumes BOTH topics to EOF,
then the pure core replays the merged event sequence in EVENT-TIME order
(scored_at for risk, truth_ts for outcomes) with an event-time TTL. No wall
clock enters anywhere, so two identical runs produce byte-identical reports.

Metrics on the scored pairs: alert precision and recall at the p >= 0.5
threshold (the headline; streaming/constants.ALERT_THRESHOLD), sensitivity at
0.3 and 0.7, PR-AUC and 10-bin ECE beneath.

    uv run --extra kafka --extra ml python -m streaming.evaluator
    uv run --extra kafka --extra ml python -m streaming.evaluator \\
        --out evaluation/streaming_eval.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from streaming.constants import ALERT_THRESHOLD

REPO = Path(__file__).resolve().parents[1]
IDENTITY = ("flight_date", "carrier", "flight_number", "origin", "dest", "crs_dep_time")
SENSITIVITY_THRESHOLDS = (0.3, 0.7)
DEFAULT_TTL_HOURS = 48.0
ECE_BINS = 10


def _ident(e: dict) -> tuple:
    return tuple(e[k] for k in IDENTITY)


def evaluate(
    risk_events: list[dict],
    outcome_events: list[dict],
    ttl_hours: float = DEFAULT_TTL_HOURS,
) -> dict:
    """Pure event-time replay of the join; returns the report dict.

    Every event lands in exactly one counter:
      risk:    scored | excluded_cancelled | excluded_diverted_no_label |
               excluded_warmup | unmatched_missing_or_late (ttl / end-of-stream)
      outcome: consumed by one of the above | orphan_outcome
    """
    ttl_ms = int(ttl_hours * 3_600_000)
    merged = sorted(
        [("risk", e["scored_at_ts_utc"], _ident(e), e) for e in risk_events]
        + [("outcome", e["truth_ts_utc"], _ident(e), e) for e in outcome_events],
        key=lambda t: (t[1], t[0] != "risk", t[2]),  # event time; risk first on ties
    )

    open_risk: dict[tuple, dict] = {}
    pending_outcome: dict[tuple, dict] = {}
    settled: set[tuple] = set()
    counters = {
        "scored": 0,
        "excluded_cancelled": 0,
        "excluded_diverted_no_label": 0,
        "excluded_warmup": 0,
        "unmatched_missing_or_late_ttl": 0,
        "unmatched_missing_or_late_end_of_stream": 0,
        "orphan_outcome": 0,
        # at-least-once delivery makes redelivery normal; a duplicate is a
        # counted category, never a silent dict overwrite (first one wins)
        "duplicate_risk": 0,
        "duplicate_outcome": 0,
    }
    pairs: list[tuple[float, bool]] = []  # (delay_probability, arr_del15)

    def settle(risk: dict, outcome: dict) -> None:
        if risk.get("rotation_state_basis") == "warmup":
            counters["excluded_warmup"] += 1
        elif outcome["cancelled"]:
            counters["excluded_cancelled"] += 1
        elif outcome["arr_del15"] is None:
            counters["excluded_diverted_no_label"] += 1
        else:
            counters["scored"] += 1
            pairs.append((float(risk["delay_probability"]), bool(outcome["arr_del15"])))

    for kind, ts, ident, event in merged:
        # event-time TTL eviction before every step
        expired = [k for k, r in open_risk.items() if r["scored_at_ts_utc"] + ttl_ms < ts]
        for k in expired:
            del open_risk[k]
            counters["unmatched_missing_or_late_ttl"] += 1
        if kind == "risk":
            if ident in settled or ident in open_risk:
                counters["duplicate_risk"] += 1
            elif ident in pending_outcome:  # out-of-order robustness
                settled.add(ident)
                settle(event, pending_outcome.pop(ident))
            else:
                open_risk[ident] = event
        else:
            if ident in settled or ident in pending_outcome:
                counters["duplicate_outcome"] += 1
            elif ident in open_risk:
                settled.add(ident)
                settle(open_risk.pop(ident), event)
            else:
                pending_outcome[ident] = event

    counters["unmatched_missing_or_late_end_of_stream"] += len(open_risk)
    counters["orphan_outcome"] += len(pending_outcome)

    report = {
        "join": {
            "ttl_hours": ttl_hours,
            "counters": counters,
            "risk_events": len(risk_events),
            "outcome_events": len(outcome_events),
        },
        "headline": _threshold_metrics(pairs, ALERT_THRESHOLD),
        "sensitivity": {str(t): _threshold_metrics(pairs, t) for t in SENSITIVITY_THRESHOLDS},
        "pr_auc": _pr_auc(pairs),
        "ece": _ece(pairs),
        "n_scored": len(pairs),
        "base_rate": (
            round(sum(1 for _, y in pairs if y) / len(pairs), 6) if pairs else None
        ),
    }
    return report


def _threshold_metrics(pairs: list[tuple[float, bool]], threshold: float) -> dict:
    tp = sum(1 for p, y in pairs if p >= threshold and y)
    fp = sum(1 for p, y in pairs if p >= threshold and not y)
    fn = sum(1 for p, y in pairs if p < threshold and y)
    return {
        "threshold": threshold,
        "alerts": tp + fp,
        "precision": round(tp / (tp + fp), 6) if tp + fp else None,
        "recall": round(tp / (tp + fn), 6) if tp + fn else None,
    }


def _pr_auc(pairs: list[tuple[float, bool]]) -> float | None:
    if not pairs or len({y for _, y in pairs}) < 2:
        return None
    from sklearn.metrics import average_precision_score

    return round(float(average_precision_score([y for _, y in pairs], [p for p, _ in pairs])), 6)


def _ece(pairs: list[tuple[float, bool]]) -> float | None:
    if not pairs:
        return None
    total, ece = len(pairs), 0.0
    for b in range(ECE_BINS):
        lo, hi = b / ECE_BINS, (b + 1) / ECE_BINS
        binned = [(p, y) for p, y in pairs if (lo <= p < hi) or (b == ECE_BINS - 1 and p == hi)]
        if binned:
            mean_p = sum(p for p, _ in binned) / len(binned)
            frac = sum(1 for _, y in binned if y) / len(binned)
            ece += abs(mean_p - frac) * len(binned) / total
    return round(ece, 6)


def render(report: dict) -> str:
    """Canonical byte-stable serialization of the report."""
    return json.dumps(report, sort_keys=True, indent=2) + "\n"


# ---- Kafka shell -----------------------------------------------------------


def _consume_all(topic: str) -> list[dict]:
    import io

    from confluent_kafka import Consumer, KafkaError, TopicPartition
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from fastavro import schemaless_reader
    from fastavro.schema import parse_schema

    from streaming.admin import PARTITIONS, bootstrap, registry_url

    sr = SchemaRegistryClient({"url": registry_url()})
    schemas: dict[int, dict] = {}
    consumer = Consumer({
        "bootstrap.servers": bootstrap(),
        "group.id": f"evaluator-{topic}",
        "auto.offset.reset": "earliest",
        "enable.partition.eof": True,
        "enable.auto.commit": False,
    })
    consumer.assign([TopicPartition(topic, p, 0) for p in range(PARTITIONS)])
    events, eof = [], set()
    while len(eof) < PARTITIONS:
        msg = consumer.poll(10.0)
        if msg is None:
            raise SystemExit(f"consumer stalled on {topic}")
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                eof.add(msg.partition())
                continue
            raise SystemExit(f"consume error on {topic}: {msg.error()}")
        raw = msg.value()
        schema_id = int.from_bytes(raw[1:5], "big")  # confluent wire format
        if schema_id not in schemas:
            schemas[schema_id] = parse_schema(json.loads(sr.get_schema(schema_id).schema_str))
        events.append(schemaless_reader(io.BytesIO(raw[5:]), schemas[schema_id]))
    consumer.close()
    return events


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ttl-hours", type=float, default=DEFAULT_TTL_HOURS)
    ap.add_argument("--out", type=Path, default=REPO / "evaluation/streaming_eval.json")
    args = ap.parse_args()

    risk = _consume_all("flight.delay_risk.v1")
    outcomes = _consume_all("flight.outcomes.v1")
    report = evaluate(risk, outcomes, ttl_hours=args.ttl_hours)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(render(report))
    print(render(report))
    print(summary(report))
    print(f"wrote {args.out}")


def summary(report: dict) -> str:
    """One screen: the headline next to where every other event went."""
    h, c = report["headline"], report["join"]["counters"]
    lines = [
        "STREAMING EVALUATION",
        f"  headline  p >= {h['threshold']}: precision {h['precision']}  "
        f"recall {h['recall']}  ({h['alerts']} alerts, {report['n_scored']} scored, "
        f"base rate {report['base_rate']})",
        f"  beneath   pr_auc {report['pr_auc']}  ece {report['ece']}  " + "  ".join(
            f"p>={t}: P {m['precision']} R {m['recall']}"
            for t, m in sorted(report["sensitivity"].items())
        ),
        "  where every event went (nothing silently dropped):",
    ]
    lines += [f"    {k:<42} {v:>8,}" for k, v in sorted(c.items())]
    lines.append(
        f"    {'risk / outcome events consumed':<42} "
        f"{report['join']['risk_events']:>8,} / {report['join']['outcome_events']:,}"
    )
    return "\n".join(lines)


if __name__ == "__main__":
    main()
