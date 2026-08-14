"""Web dashboard over the risk topic: flight risk, cascade exposure, evaluation.

The browser counterpart of streaming/ui.py, serving the same two views plus the
outcome-join evaluation report as one local page. Zero new dependencies:
FastAPI and uvicorn already ship in the serve extra, and every ranking reuses
the terminal UI's functions rather than restating them.

Data path, reported honestly on /api/meta:

  live_topic        flight.delay_risk.v1 consumed once at startup with the
                    evaluator's proven batch reader, plus the live evaluation
                    report (evaluation/streaming_eval.json) when present.
  reference_output  the committed data/reference_output/ pair when the broker
                    is unreachable or the topic is empty. The reference alert
                    artifact is a thin projection (alerts only, no tail), so
                    in this mode the flights view shows alerts and the cascade
                    view is disabled: chains need the FULL event set
                    (streaming/ui.rank_cascade's rule), and pretending
                    otherwise would splice itineraries that never happened.

    uv run --extra kafka --extra ml --extra serve python -m streaming.dashboard
    uv run --extra kafka --extra ml --extra serve python -m streaming.dashboard --port 8601

Then open http://127.0.0.1:8600. Determinism: the data is loaded once at
startup from a deterministic replay, so every response is a pure function of
the loaded snapshot; refreshing re-reads nothing.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path

from streaming import constants as c

REPO = Path(__file__).resolve().parents[1]
OUT_TOPIC = "flight.delay_risk.v1"
LIVE_EVAL = REPO / "evaluation/streaming_eval.json"
REFERENCE_DIR = REPO / "data/reference_output"
DEFAULT_PORT = 8600


# ---- data snapshot ---------------------------------------------------------


@dataclass
class DashboardData:
    """Everything the endpoints serve, loaded once at startup.

    source is "live_topic" or "reference_output". events is the full scored
    set in live mode and EMPTY in reference mode: the committed alert artifact
    carries alerts only, and serving it as if it were the full set would make
    every downstream ranking silently wrong. Reference-mode alerts live in
    alerts instead.
    """

    source: str
    events: list[dict] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)
    eval_report: dict | None = None


def _read_alerts(path: Path) -> list[dict]:
    with path.open() as f:
        return [json.loads(line) for line in f if line.strip()]


def _read_json(path: Path) -> dict | None:
    return json.loads(path.read_text()) if path.exists() else None


def load_reference() -> DashboardData:
    """The committed reference pair: the rubric's fallback output."""
    return DashboardData(
        source="reference_output",
        alerts=_read_alerts(REFERENCE_DIR / "alerts.jsonl"),
        eval_report=_read_json(REFERENCE_DIR / "streaming_eval.json"),
    )


def load_data() -> DashboardData:
    """Live topic first, committed reference output when the broker is down.

    The evaluator's reader raises SystemExit("consumer stalled ...") when the
    broker never answers; an empty topic (stack up, week not replayed) is the
    same situation for a viewer, so both take the fallback.
    """
    try:
        from streaming.evaluator import _consume_all

        events = _consume_all(OUT_TOPIC)
    except (SystemExit, Exception):  # noqa: B014 - any broker failure means fallback
        events = []
    if not events:
        return load_reference()
    return DashboardData(
        source="live_topic",
        events=events,
        alerts=[e for e in events if e.get("alert")],
        eval_report=_read_json(LIVE_EVAL) or _read_json(REFERENCE_DIR / "streaming_eval.json"),
    )


# ---- pure summaries (tested without Kafka or an app) -----------------------


def summarize(data: DashboardData) -> dict:
    """The header tiles: one dict, computable in either mode."""
    rows = data.events or data.alerts
    dates = sorted({str(e["flight_date"]) for e in rows})
    warmup = sum(1 for e in data.events if str(e.get("rotation_state_basis")) == "warmup")
    headline = (data.eval_report or {}).get("headline")
    return {
        "source": data.source,
        "n_scored": len(data.events) or None,
        "n_alerts": len(data.alerts),
        "alert_threshold": c.ALERT_THRESHOLD,
        "date_span": [dates[0], dates[-1]] if dates else None,
        "warmup_rows": warmup or None,
        "model_run_id": next((e.get("model_run_id") for e in data.events), None),
        "headline": headline,
        "base_rate": (data.eval_report or {}).get("base_rate"),
        "pr_auc": (data.eval_report or {}).get("pr_auc"),
        "ece": (data.eval_report or {}).get("ece"),
    }


def meta(data: DashboardData) -> dict:
    """What the viewer is looking at and what that mode can show."""
    live = data.source == "live_topic"
    return {
        "source": data.source,
        "topic": OUT_TOPIC if live else None,
        "views": {
            "flights": "all scored departures" if live else "alerts only (thin projection)",
            "cascade": "enabled" if live else "disabled: chains need the full event set",
            "evaluation": "enabled" if data.eval_report else "no evaluation report found",
        },
        "note": None if live else (
            "Broker unreachable or topic empty; serving the committed "
            "data/reference_output/ pair. Run `make demo` then restart for the full views."
        ),
    }


# ---- app -------------------------------------------------------------------


def create_app(data: DashboardData):
    from fastapi import FastAPI
    from fastapi.responses import PlainTextResponse

    app = FastAPI(title="Gate-Time Delay Risk", docs_url=None, redoc_url=None)

    @app.get("/api/meta")
    def api_meta() -> dict:
        return meta(data)

    @app.get("/api/summary")
    def api_summary() -> dict:
        return summarize(data)

    @app.get("/", response_class=PlainTextResponse)
    def index() -> str:
        # replaced by the HTML page in the next stage
        return "Gate-Time Delay Risk dashboard: see /api/meta and /api/summary"

    return app


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    import uvicorn

    data = load_data()
    m = meta(data)
    print(f"data source: {m['source']}" + (f"\n{m['note']}" if m["note"] else ""))
    uvicorn.run(create_app(data), host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
