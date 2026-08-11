# Flight-Delay Dashboard — Looker Studio spec

> **Note:** the dashboard is **built as a Streamlit app in [`dashboard/`](../../dashboard/)**
> (runs in-repo, reads these same five `dash_*` views live via BigQuery). This
> file is retained as the click-to-build recipe for a fully-managed Looker Studio
> surface — an equivalent alternative to the Streamlit implementation. The
> layout, grains, and calculated-field rules below apply to both.


Five purpose-built gold views power the dashboard (project `de-flight-project`,
dataset `flight_delays_gold`). Every view is a THIN skin over a materialized
gold mart (label/name columns only, no aggregation) — no dashboard source
aggregates fact_flights at query time, so Looker's live connector re-running a
source per interaction scans at most ~7.6k pre-aggregated rows, never 20.6M.

| View | Grain | Powers |
|---|---|---|
| `dash_airport_reliability` | 1 row / origin airport (374) | Airport reliability ranking, cancellation rates |
| `dash_carrier_reliability` | 1 row / carrier (17) | Carrier reliability ranking |
| `dash_delays_by_time` | (year, month, day-of-week, dep hour) ~6k rows | Hour-of-day, day-of-week, season charts |
| `dash_monthly_trend` | 1 row / calendar month (36) | Monthly trend + year-over-year, scorecards |
| `dash_route_drilldown` | 1 row / directed route (~7.6k) | Route drill-down page |

**The one rule that keeps numbers correct:** `dash_delays_by_time` and
`dash_monthly_trend` carry *additive* counts. Any rate shown at a rolled-up
grain must be a **calculated field over SUMs** — never the average of a
pre-computed rate column.

## Step-by-step connection

1. In Looker Studio: **Create → Report → Add data → BigQuery** connector.
2. Authorize, then pick **Project** `de-flight-project` → **Dataset**
   `flight_delays_gold` → table `dash_airport_reliability` → **Add**.
3. Repeat **Add data** for the other four `dash_*` views (five data sources in
   one report).
4. In each data source schema, set types Looker guessed wrong:
   `month_start` → Date (YYYYMMDD); `dep_hour`, `day_of_week`, `season_order`
   → Number; everything `*_rate` → Number, display as Percent.
5. Add these **calculated fields** (data source level):
   - on `dash_delays_by_time` and `dash_monthly_trend`:
     - `Delay rate` = `SUM(n_arr_del15) / SUM(n_with_arr_outcome)` (Percent)
     - `Cancellation rate (agg)` = `SUM(n_cancelled) / SUM(n_flights)` (Percent)
     - `Avg arrival delay (min)` = `SUM(sum_arr_delay_minutes) / SUM(n_with_arr_outcome)`
     - `Avg departure delay (min)` = `SUM(sum_dep_delay_minutes) / SUM(n_with_dep_outcome)`
       (its own denominator — 60,576 flights departed and were then
       cancelled/diverted, so neither `n_flights` nor `n_with_arr_outcome`
       is the departure-delay population)
   - on `dash_airport_reliability` and `dash_route_drilldown`: none needed —
     rates are display-safe at their native grain.
6. Set the report theme, then build pages per the layout below.

## Layout (3 pages)

### Page 1 — "Who is reliable?" (lead)

```
┌───────── scorecard row (dash_monthly_trend) ─────────────────────┐
│ Total flights | Delay rate | Cancellation rate | Avg delay (min) │
├───────────────────────────────┬──────────────────────────────────┤
│ Least reliable AIRPORTS       │ Least reliable CARRIERS          │
│ horiz. bar, top 15            │ horiz. bar, all 17               │
├───────────────────────────────┴──────────────────────────────────┤
│ Delay rate by HOUR OF DAY — column chart, 0–23                   │
└──────────────────────────────────────────────────────────────────┘
```

- **Scorecards**: `dash_monthly_trend` → metrics: `SUM(n_flights)`,
  `Delay rate`, `Cancellation rate (agg)`, `Avg arrival delay (min)`.
- **Airport bar**: `dash_airport_reliability` → dimension `airport_name`,
  metric `arr_del15_rate`, sort desc, rows 15, **chart filter
  `n_flight_legs >= 10000`** (so 40-flight airfields don't top the ranking);
  optional second metric `cancellation_rate`. Tooltip: `n_flight_legs`, `city`.
- **Carrier bar**: `dash_carrier_reliability` → dimension `carrier_key`,
  metrics `arr_del15_rate` + `cancellation_rate`, sort desc.
- **Hour chart**: `dash_delays_by_time` → dimension `dep_hour`, metric
  `Delay rate` (calculated), sort by `dep_hour` ascending. The evening
  delay build-up is the story this chart tells.

### Page 2 — "When do delays happen?"

- **Day of week**: `dash_delays_by_time` → dimension `day_name` (sort by
  `day_of_week`), metric `Delay rate`.
- **Season**: same source → dimension `season` (sort by `season_order`),
  metrics `Delay rate` and `Cancellation rate (agg)` — winter cancellations
  vs summer delays is the contrast to surface.
- **Monthly trend + YoY**: `dash_monthly_trend` → time-series chart, date
  dimension `month_start`, metric `Delay rate`. For YoY either (a) enable
  **Comparison date range: previous year** on the time series, or (b) make a
  line chart with dimension `month_name` (sort by `month`), breakdown
  dimension `year` — three overlaid year lines.
- Optional heatmap (pivot table with heatmap style): rows `day_name`,
  columns `dep_hour`, metric `Delay rate`.
- Page-level **filter controls**: `year`, `season`.

### Page 3 — "Route drill-down"

- **Controls**: drop-downs on `origin_airport_key` and `dest_airport_key`
  (searchable), slider on `n_flight_legs`.
- **Table**: `dash_route_drilldown` → dimensions `route`,
  `origin_city`, `dest_city`; metrics `n_flight_legs`, `arr_del15_rate`,
  `avg_arr_delay_minutes`, `p90_arr_delay_minutes`, `cancellation_rate`;
  default sort `n_flight_legs` desc; conditional formatting on
  `arr_del15_rate`.
- **Scatter** (optional): `n_flight_legs` (x, log) vs `arr_del15_rate` (y) —
  busy-and-late routes sit top-right.
- Reach this page via a chart action: clicking an airport on Page 1 →
  cross-filter linked to the route table (enable cross-filtering on the
  airport bar; Looker applies `airport_name` — add `origin_airport_key` to
  the bar's dimensions drill hierarchy so the filter carries).

## Notes

- `hist_*` columns on the reliability/route views are **training-window**
  (pre-2024-07) rates from the shared ML model — if shown, label them
  "historical (through Jun 2024)"; don't mix them into full-period charts.
- `day_of_week` is BTS convention: 1 = Monday … 7 = Sunday.
- All five sources are views backed by materialized marts (the time and
  monthly grains are the tables mart_delays_by_schedule and
  mart_delays_monthly): dashboards always reflect the latest dbt build, and a
  full page load scans well under 1 MB even with Looker re-querying per
  interaction.
