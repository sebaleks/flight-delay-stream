"""Dashboard data layer: mode split, summaries, and the reference fallback.

No Kafka, no credentials: live mode is exercised on fixture events, fallback
mode against the committed data/reference_output/ pair, which the rulebook
guarantees present (CLAUDE.md section 5).
"""

from __future__ import annotations

from streaming import constants as c
from streaming.dashboard import DashboardData, create_app, load_reference, meta, summarize

T0 = 1725235200000  # 2024-09-02T00:00Z, ms
HOUR = 3_600_000


def event(p=0.5, ts=T0, fn="100", tail="N1", basis="consistent", date="2024-09-02"):
    return {
        "flight_date": date,
        "carrier": "AA",
        "flight_number": fn,
        "origin": "SFO",
        "dest": "LAX",
        "crs_dep_time": "0800",
        "tail_number": tail,
        "scored_at_ts_utc": ts,
        "delay_probability": p,
        "risk_band": c.risk_band(p),
        "alert": p >= c.ALERT_THRESHOLD,
        "model_run_id": "20260730_145241",
        "rotation_state_basis": basis,
        "turnaround_band": "60_120",
    }


def live_data():
    events = [
        event(p=0.7, ts=T0, fn="1"),
        event(p=0.2, ts=T0 + HOUR, fn="2"),
        event(p=0.6, ts=T0 - 12 * HOUR, fn="3", basis="warmup", date="2024-09-01"),
    ]
    return DashboardData(
        source="live_topic",
        events=events,
        alerts=[e for e in events if e["alert"]],
        eval_report={"headline": {"precision": 0.5, "recall": 0.2, "threshold": 0.5},
                     "base_rate": 0.138, "pr_auc": 0.34, "ece": 0.033},
    )


def test_summary_counts_and_span_in_live_mode():
    s = summarize(live_data())
    assert s["source"] == "live_topic"
    assert s["n_scored"] == 3
    assert s["n_alerts"] == 2
    assert s["date_span"] == ["2024-09-01", "2024-09-02"]
    assert s["warmup_rows"] == 1
    assert s["model_run_id"] == "20260730_145241"
    assert s["headline"]["precision"] == 0.5


def test_meta_declares_what_each_mode_can_show():
    m = meta(live_data())
    assert m["topic"] == "flight.delay_risk.v1"
    assert m["views"]["cascade"] == "enabled"
    assert m["note"] is None

    m = meta(DashboardData(source="reference_output", alerts=[event(p=0.8)]))
    assert m["topic"] is None
    assert "disabled" in m["views"]["cascade"]
    assert m["note"] is not None


def test_reference_fallback_loads_the_committed_pair():
    data = load_reference()
    assert data.source == "reference_output"
    assert data.events == []
    assert len(data.alerts) == 7061  # the committed regression reference
    assert data.eval_report["headline"]["threshold"] == c.ALERT_THRESHOLD
    s = summarize(data)
    assert s["n_scored"] is None
    assert s["n_alerts"] == 7061
    assert s["date_span"][0] <= s["date_span"][1]


def test_app_registers_the_stage_one_routes():
    app = create_app(live_data())
    paths = {r.path for r in app.routes}
    assert {"/", "/api/meta", "/api/summary"} <= paths
