# Reference output

The two artifacts a reviewer would otherwise have to run the demo to see. Both
are committed so the repository shows its output without requiring Docker, and
both are byte-reproducible: `make demo` regenerates them exactly, so a diff
against these files is a regression test rather than a formality.

Generated at commit `d3c96a9` from the committed replay week (2024-09-02 to
2024-09-08 plus the 2024-09-01 warm-up day) with the frozen 2024-06-30 model
run `20260730_145241`.

## `alerts.jsonl`

The alert artifact, one JSON object per line, 7,061 alerts at the 0.5
threshold out of 151,878 scored departures. Fields:

| field | meaning |
|---|---|
| `flight_date`, `carrier`, `flight_number`, `origin`, `dest`, `crs_dep_time` | flight identity, all schedule-knowable |
| `delay_probability` | calibrated probability of `ArrDel15`, from the frozen classifier |
| `risk_band` | the probability decile the alert falls in |
| `threshold` | the alerting threshold in force (0.5) |
| `issued_at` | event time of the scoring decision, the scheduled gate time T |
| `mode` | `replay` for the seeded producer path |

`issued_at` is event time, never wall clock. That is what makes two runs on
different days produce identical files.

## `streaming_eval.json`

The outcome-join evaluation over the same run. Headline precision 0.548908 and
recall 0.177616 at threshold 0.5, over 5,950 evaluated alerts and 133,627
labeled departures, base rate 0.137607, PR-AUC 0.340218, ECE 0.033393, with
sensitivity rows at thresholds 0.3 and 0.7.

The `join.counters` block is the reason the numbers can be trusted as
week-level: every one of the 151,878 risk events is accounted for in exactly
one counter. 133,627 scored, 17,780 excluded as warm-up, 248 cancelled, 222
diverted without a label, 1 orphan outcome, 1 past the 48-hour TTL. Nothing is
silently dropped, and the counters are conservation-tested in
`streaming/test_evaluator.py`.

## `figures/`

Rendered from the two files above by `scripts/plot_evaluation.py`, which
recomputes nothing: plotting a number the evaluator did not produce would put
a second, unversioned source of truth in the report.

`threshold_sweep.png` answers what moving the alert threshold costs and buys.
Precision and recall across all 19 sweep points, the three sampled operating
points labelled, and the base rate drawn as a reference, because 0.549
precision only means something against the 0.138 a random alerter would get.

`weather_gap.png` decomposes the forecast-substitution gap in the 0-3 hour
horizon. It is drawn as a descent line rather than bars on purpose: the whole
finding is a 0.043 difference, and a bar's length read off a truncated
baseline is the standard way to exaggerate a small gap. Position encoding
shows the same numbers honestly on a zoomed axis.

`by_day.png` shows the operating point holding across all seven scored days,
precision between 0.492 and 0.689, with each day's own base rate beneath. It
is the check that the week aggregate is not hiding one bad day.

The palette is the deck's slate re-stepped to clear the chroma floor, paired
with its signal orange, validated for colour-vision-deficiency separation
(worst adjacent pair dE 19.6 protan) rather than chosen by eye.

## Reproducing

```bash
make demo
diff data/reference_output/alerts.jsonl alerts.jsonl
diff data/reference_output/streaming_eval.json evaluation/streaming_eval.json
```

Both diffs must be empty. The live runtime paths (`alerts.jsonl`,
`evaluation/`) stay git-ignored; only these reference copies are committed.
