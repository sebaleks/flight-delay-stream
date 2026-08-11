"""Synthetic-event gate for the outcome-join evaluator.

Hand-written risk events (validated against the registered delay_risk
contract via its .avsc source) and outcomes exercise every counter category:
scored pairs, cancelled, diverted-without-label, warm-up exclusion, TTL
eviction, end-of-stream unmatched, and orphan outcomes. Determinism is
asserted at the byte level: two runs render identical reports.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastavro.schema import parse_schema
from fastavro.validation import validate

from streaming.evaluator import evaluate, render

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
RISK_SCHEMA = parse_schema(json.loads((SCHEMA_DIR / "delay_risk.avsc").read_text()))
OUTCOME_SCHEMA = parse_schema(json.loads((SCHEMA_DIR / "outcome.avsc").read_text()))

T0 = 1725235200000  # 2024-09-02T00:00Z, ms
HOUR = 3_600_000
DAY_2024_09_02 = 19968  # days since epoch


def mk_risk(n: int, p: float, basis: str = "consistent", scored_at: int | None = None) -> dict:
    event = {
        "flight_date": DAY_2024_09_02,
        "carrier": "UA",
        "flight_number": str(n),
        "origin": "ORD",
        "dest": "SFO",
        "crs_dep_time": "0900",
        "tail_number": f"N{n:05d}",
        "scored_at_ts_utc": T0 + n * HOUR if scored_at is None else scored_at,
        "delay_probability": p,
        "risk_band": "0.5-0.6",
        "alert": p >= 0.5,
        "model_run_id": "20260730_145241",
        "calibration": "platt",
        "rotation_state_basis": basis,
        "weather_basis": "observed",
        "taf_horizon_bin": None,
        "pressure_late_arrivals": None,
        "pressure_cancellations": None,
    }
    assert validate(event, RISK_SCHEMA), f"synthetic risk {n} violates the registered contract"
    return event


def mk_outcome(
    n: int,
    arr_del15: bool | None,
    cancelled: bool = False,
    diverted: bool = False,
    truth_ts: int | None = None,
) -> dict:
    event = {
        "flight_date": DAY_2024_09_02,
        "carrier": "UA",
        "flight_number": str(n),
        "origin": "ORD",
        "dest": "SFO",
        "crs_dep_time": "0900",
        "tail_number": f"N{n:05d}",
        "arr_del15": arr_del15,
        "arr_delay_minutes": 30.0 if arr_del15 else 0.0,
        "cancelled": cancelled,
        "diverted": diverted,
        "truth_ts_utc": (T0 + n * HOUR + 2 * HOUR) if truth_ts is None else truth_ts,
    }
    assert validate(event, OUTCOME_SCHEMA), f"synthetic outcome {n} violates the contract"
    return event


def build_streams() -> tuple[list[dict], list[dict]]:
    scored = [(1, 0.9, True), (2, 0.8, False), (3, 0.6, False),
              (4, 0.35, True), (5, 0.2, True), (6, 0.1, False)]
    risks = [mk_risk(n, p) for n, p, _ in scored]
    outcomes = [mk_outcome(n, y) for n, _, y in scored]

    risks.append(mk_risk(7, 0.7))                       # cancelled -> excluded
    outcomes.append(mk_outcome(7, None, cancelled=True))
    risks.append(mk_risk(8, 0.4))                       # diverted, no label
    outcomes.append(mk_outcome(8, None, diverted=True))
    risks.append(mk_risk(9, 0.9, basis="warmup"))       # warm-up pair, excluded
    outcomes.append(mk_outcome(9, True))
    risks.append(mk_risk(10, 0.6, scored_at=T0))        # outcome 49h late -> TTL
    outcomes.append(mk_outcome(10, True, truth_ts=T0 + 49 * HOUR))
    risks.append(mk_risk(11, 0.5))                      # outcome never arrives
    outcomes.append(mk_outcome(12, False))              # risk never existed
    risks.append(mk_risk(1, 0.9))                       # redelivery -> duplicate
    outcomes.append(mk_outcome(2, False))               # redelivery -> duplicate
    return risks, outcomes


def test_counters_and_conservation():
    risks, outcomes = build_streams()
    report = evaluate(risks, outcomes, ttl_hours=48.0)
    c = report["join"]["counters"]
    assert c == {
        "scored": 6,
        "excluded_cancelled": 1,
        "excluded_diverted_no_label": 1,
        "excluded_warmup": 1,
        "unmatched_missing_or_late_ttl": 1,
        "unmatched_missing_or_late_end_of_stream": 1,
        "orphan_outcome": 2,  # the 49h-late outcome and the riskless one
        "duplicate_risk": 1,
        "duplicate_outcome": 1,
    }
    # nothing silently dropped: every risk event is in exactly one category
    risk_side = (c["scored"] + c["excluded_cancelled"] + c["excluded_diverted_no_label"]
                 + c["excluded_warmup"] + c["unmatched_missing_or_late_ttl"]
                 + c["unmatched_missing_or_late_end_of_stream"] + c["duplicate_risk"])
    assert risk_side == len(risks) == report["join"]["risk_events"]
    outcome_side = (c["scored"] + c["excluded_cancelled"] + c["excluded_diverted_no_label"]
                    + c["excluded_warmup"] + c["orphan_outcome"] + c["duplicate_outcome"])
    assert outcome_side == len(outcomes) == report["join"]["outcome_events"]


def test_headline_and_sensitivity_metrics():
    risks, outcomes = build_streams()
    report = evaluate(risks, outcomes, ttl_hours=48.0)
    assert report["n_scored"] == 6
    assert report["base_rate"] == 0.5
    # pairs: (.9,T) (.8,F) (.6,F) (.35,T) (.2,T) (.1,F)
    assert report["headline"]["threshold"] == 0.5
    assert report["headline"]["alerts"] == 3
    assert report["headline"]["precision"] == round(1 / 3, 6)
    assert report["headline"]["recall"] == round(1 / 3, 6)
    assert report["sensitivity"]["0.3"]["precision"] == 0.5
    assert report["sensitivity"]["0.3"]["recall"] == round(2 / 3, 6)
    assert report["sensitivity"]["0.7"]["precision"] == 0.5
    assert report["sensitivity"]["0.7"]["recall"] == round(1 / 3, 6)
    assert report["pr_auc"] is not None
    assert report["ece"] is not None


def test_out_of_order_risk_after_outcome_still_settles():
    # robustness: a risk arriving after its outcome (same event time window)
    # matches the pending outcome instead of stranding it
    risk = mk_risk(1, 0.9, scored_at=T0 + 3 * HOUR)
    outcome = mk_outcome(1, True, truth_ts=T0 + 2 * HOUR)
    report = evaluate([risk], [outcome], ttl_hours=48.0)
    assert report["join"]["counters"]["scored"] == 1
    assert report["join"]["counters"]["orphan_outcome"] == 0


def test_report_is_byte_identical_across_runs():
    risks, outcomes = build_streams()
    a = render(evaluate(risks, outcomes, ttl_hours=48.0))
    b = render(evaluate(list(reversed(risks)), list(reversed(outcomes)), ttl_hours=48.0))
    assert a == b, "input order must not affect the rendered report"
