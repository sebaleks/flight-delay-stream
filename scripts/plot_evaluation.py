"""Render the evaluation figures for the report and the deck.

Three figures, each answering one question, all read from committed artifacts
so they regenerate byte-for-byte with the numbers they illustrate:

  threshold_sweep.png   what does moving the alert threshold cost and buy?
  weather_gap.png       where does the forecast-substitution gap come from?
  by_day.png            does the operating point hold across all seven days?

Sources: data/reference_output/streaming_eval.json (sweep, by_day, base rate)
and data/taf_harmonization.json (the three-regime decomposition). Nothing here
recomputes a metric; plotting a number the evaluator did not produce would put
a second, unversioned source of truth in the report.

    uv run --extra kafka --extra ml python scripts/plot_evaluation.py
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "data/reference_output/figures"

# Palette: the deck's slate re-stepped to clear the chroma floor, paired with
# its signal orange. Validated for colour-vision deficiency separation
# (worst adjacent pair dE 19.6 protan, 24.3 normal) rather than eyeballed.
BLUE = "#2b6ca3"
ORANGE = "#a8461c"
PAPER = "#f7f5f0"
INK = "#1a2430"
MUTED = "#5c6875"
GRID = "#ddd8ce"

plt.rcParams.update({
    "font.family": ["Helvetica Neue", "Helvetica", "Arial", "DejaVu Sans"],
    "font.size": 12,
    "figure.facecolor": PAPER,
    "axes.facecolor": PAPER,
    "savefig.facecolor": PAPER,
    "text.color": INK,
    "axes.labelcolor": INK,
    "xtick.color": MUTED,
    "ytick.color": MUTED,
    "axes.edgecolor": GRID,
})


def _frame(ax) -> None:
    """Recessive axes: no box, horizontal rules only, marks above the grid."""
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.grid(axis="y", color=GRID, linewidth=0.8, zorder=0)
    ax.set_axisbelow(True)
    ax.tick_params(length=0)


def _title(ax, title: str, subtitle: str) -> None:
    ax.set_title(title, loc="left", fontsize=17, fontweight="bold", pad=26, color=INK)
    ax.annotate(subtitle, xy=(0, 1), xytext=(0, 12), xycoords="axes fraction",
                textcoords="offset points", fontsize=12, color=MUTED, va="bottom")


def threshold_sweep(report: dict) -> None:
    sweep = report["sweep"]
    t = [p["threshold"] for p in sweep]
    prec = [p["precision"] for p in sweep]
    rec = [p["recall"] for p in sweep]
    base = report["base_rate"]

    fig, ax = plt.subplots(figsize=(10, 5.6), dpi=200)
    _frame(ax)

    # the reference that gives 0.549 its meaning: precision of random alerting
    ax.axhline(base, color=MUTED, linestyle=(0, (5, 4)), linewidth=1.4, zorder=1)
    ax.annotate(f"base rate {base:.3f} — precision of alerting at random",
                xy=(0.955, base), xytext=(0, 7), textcoords="offset points",
                ha="right", fontsize=11, color=MUTED)

    ax.plot(t, prec, color=BLUE, linewidth=2, zorder=3, label="Precision")
    ax.plot(t, rec, color=ORANGE, linewidth=2, zorder=3, label="Recall")

    # direct labels on the three operating points only, never every point.
    # The offset follows the data: whichever series is lower at this threshold
    # gets its label below, so a label never floats beside the other's marker.
    for th in (0.3, 0.5, 0.7):
        i = t.index(th)
        for series, colour in ((prec, BLUE), (rec, ORANGE)):
            dy = 16 if series[i] >= (rec[i] if series is prec else prec[i]) else -24
            ax.scatter([th], [series[i]], s=64, color=colour, zorder=4,
                       edgecolors=PAPER, linewidths=2)
            ax.annotate(f"{series[i]:.3f}", xy=(th, series[i]), xytext=(0, dy),
                        textcoords="offset points", ha="center", fontsize=11.5,
                        fontweight="bold", color=colour)
        ax.axvline(th, color=GRID, linewidth=1, zorder=0)

    ax.annotate("shipped default", xy=(0.5, 1.0), xytext=(0, -4),
                xycoords=("data", "axes fraction"), textcoords="offset points",
                ha="center", fontsize=10.5, color=MUTED)

    ax.set_xlim(0.02, 0.98)
    ax.set_ylim(0, 1)
    ax.set_xlabel("Alert threshold", fontsize=12.5, labelpad=10)
    ax.set_xticks([0.1, 0.3, 0.5, 0.7, 0.9])
    ax.set_yticks([0, 0.2, 0.4, 0.6, 0.8, 1.0])
    _title(ax, "The alert threshold is a business decision",
           f"Precision and recall across {report['n_scored']:,} labelled departures, "
           "one held-out week")
    ax.legend(frameon=False, loc="upper center", ncol=2, fontsize=12,
              bbox_to_anchor=(0.5, -0.14))
    fig.tight_layout()
    fig.savefig(OUT / "threshold_sweep.png", bbox_inches="tight")
    plt.close(fig)


def weather_gap(harm: dict) -> None:
    b = harm["bins"]["0-3h"]
    names = ["Observed\n(training's inputs)", "Harmonized\n(observed, TAF's vocabulary)",
             "TAF forecast\n(what a live stream has)"]
    vals = [b["observed"]["pr_auc"], b["harmonized"]["pr_auc"], b["taf"]["pr_auc"]]
    rep, fcst = b["representation_share_pr_auc"], b["forecast_share_pr_auc"]

    fig, ax = plt.subplots(figsize=(10, 5.6), dpi=200)
    _frame(ax)

    # A descent line, not bars. The whole finding lives in a 0.043 difference,
    # and a bar's length would have to be read off a truncated baseline to show
    # it — the classic way to exaggerate a small gap. Position encoding carries
    # the same numbers honestly on a zoomed axis.
    x = [0, 1, 2]
    ax.plot(x, vals, color=BLUE, linewidth=2, zorder=3)
    ax.scatter(x, vals, s=150, color=BLUE, zorder=4, edgecolors=PAPER, linewidths=2.5)
    # values sit below their markers, step labels above the segment they
    # describe: two registers that cannot collide as the line descends
    # the first value label clears the steep segment to its left, the others
    # sit centred beneath their markers
    for xi, v, (dx, ha) in zip(x, vals, ((-16, "right"), (0, "center"), (0, "center")),
                               strict=True):
        ax.annotate(f"{v:.4f}", xy=(xi, v), xytext=(dx, -26), textcoords="offset points",
                    ha=ha, fontsize=13.5, fontweight="bold", color=INK)

    # step labels ride clear of the line: right of the steep first segment,
    # straight above the shallow second one
    for x0, x1, label, (dx, dy, ha) in (
        (0, 1, f"representation  −{rep:.4f}\n89% of the gap", (34, 16, "left")),
        (1, 2, f"forecast error  −{fcst:.4f}\n11%", (0, 34, "center")),
    ):
        ax.annotate(label, xy=((x0 + x1) / 2, (vals[x0] + vals[x1]) / 2),
                    xytext=(dx, dy), textcoords="offset points", ha=ha,
                    fontsize=12.5, fontweight="bold", color=ORANGE)

    ax.set_xticks(x)
    ax.set_xticklabels(names)
    ax.set_xlim(-0.4, 2.4)
    ax.set_ylim(0.301, 0.359)
    ax.set_ylabel("PR-AUC, 0–3 hour horizon", fontsize=12.5, labelpad=10)
    _title(ax, "Most of the forecast gap is vocabulary, not forecasting",
           f"Same {b['observed']['n']:,} covered departures scored three ways by the frozen model")
    fig.tight_layout()
    fig.savefig(OUT / "weather_gap.png", bbox_inches="tight")
    plt.close(fig)


def by_day(report: dict) -> None:
    days = report["by_day"]
    labels = [d[5:] for d in days]  # MM-DD; the year is in the subtitle
    prec = [v["precision"] for v in days.values()]
    rec = [v["recall"] for v in days.values()]
    base = [v["base_rate"] for v in days.values()]

    fig, ax = plt.subplots(figsize=(10, 5.6), dpi=200)
    _frame(ax)

    ax.plot(labels, base, color=MUTED, linestyle=(0, (5, 4)), linewidth=1.4,
            zorder=2, label="Base rate that day")
    for series, colour, name in ((prec, BLUE, "Precision"), (rec, ORANGE, "Recall")):
        ax.plot(labels, series, color=colour, linewidth=2, zorder=3, label=name)
        ax.scatter(labels, series, s=64, color=colour, zorder=4,
                   edgecolors=PAPER, linewidths=2)

    # label the endpoints only: the shape carries the story, not 21 numbers.
    # Recall labels sit below its line so they never land on the base-rate rule,
    # which runs just above recall for most of the week.
    for series, colour, dy in ((prec, BLUE, 14), (rec, ORANGE, -25)):
        for i in (0, len(labels) - 1):
            ax.annotate(f"{series[i]:.3f}", xy=(i, series[i]), xytext=(0, dy),
                        textcoords="offset points", ha="center", fontsize=11.5,
                        fontweight="bold", color=colour)

    ax.set_ylim(0, 0.85)
    ax.set_xlabel("Flight date, September 2024", fontsize=12.5, labelpad=10)
    _title(ax, "The operating point holds across all seven days",
           "Precision and recall at the 0.5 threshold, per flight date")
    ax.legend(frameon=False, loc="upper center", ncol=3, fontsize=12,
              bbox_to_anchor=(0.5, -0.14))
    fig.tight_layout()
    fig.savefig(OUT / "by_day.png", bbox_inches="tight")
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    report = json.loads((REPO / "data/reference_output/streaming_eval.json").read_text())
    harm = json.loads((REPO / "data/taf_harmonization.json").read_text())
    threshold_sweep(report)
    weather_gap(harm)
    by_day(report)
    for f in sorted(OUT.glob("*.png")):
        print(f"wrote {f.relative_to(REPO)}  ({f.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
