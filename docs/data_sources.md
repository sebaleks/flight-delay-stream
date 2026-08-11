# Data Sources

Everything here was verified on 2026-08-11 unless labeled otherwise. Items that could not be fully verified are labeled as such; nothing unverified is stated as fact.

## Replay week (primary demo data)

Source: the existing BigQuery mart (`flight_delays_gold.ml_flight_features`), one week from the 2024-H2 held-out window. The week is chosen by the day-typicality method (`ml/day_typicality.py`, a z-band test that a day is unexceptional), so the demo week is defensibly not cherry-picked. Provenance to disclose wherever the week appears: held out from training by the single dbt cutoff (`train_test_cutoff_date: "2024-07-01"`, `dbt/dbt_project.yml:48`), never trained on, previously scored only in aggregate during 681.

Exports land at: `data/replay/departures_week.parquet` (schedule fields), `data/replay/outcomes_week.parquet` (truth), `data/weather/isd_week.parquet` (that week's hourly ISD observations from the existing silver tables; no new NOAA fetch), `data/golden/rotation_reference_week.parquet` (the mart's rotation feature columns for the week, keyed by flight identity; the in-stream rotation parity target), `data/golden/golden_vectors.parquet` (scoring parity vectors captured via `ml/parity.py` while the BigQuery path is still live), and `data/lookups/*.parquet` (the three serving lookup tables plus route distances and the airports seed). A one-day warm-up precedes the scored window so per-tail rotation state hydrates before evaluation begins.

## BTS On-Time Performance (narrow ingest for drift; also the replay week's original source)

- URL pattern, verified live by HEAD request: `https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_<YYYY>_<M>.zip`. May 2026 returned HTTP 200 at 31.7 MB.
- Latest available month as of 2026-08-11: May 2026 (posted 2026-06-30). Observed lag is variable: April 2026 took about 10 weeks, May about 4.3 weeks, June still 404 at 6 weeks. Plan on 1 to 2.5 months, not a fixed date.
- Header diff, executed (not just planned): the December 2024 and May 2026 in-zip CSV headers are byte-identical, 1,696 bytes, 109 columns each, and all 33 columns in `ingestion/bts.py` `REQUIRED_COLUMNS` are present in the 2026-05 header. The DOT rule phasing January 2025 to June 2026 did not change the on-time file schema through May 2026.
- The DOT rule in that phasing window is the wheelchair-accommodations rule (ACAA, 14 CFR Part 382 territory), consumer protection, not the Part 234 on-time file. One caveat, labeled unverified: BTS Technical Reporting Directive #40 (effective 2026-01-01) updates reporting-carrier and reportable-airport lists and gives codeshare OTP reporting instructions; its PDF returns 403 to non-browser clients, so this rests on the BTS search abstract. Codeshare reporting-carrier changes would matter to `hist_carrier_*` joins, so keep the header and carrier-code check in the ingest runbook.
- Month arithmetic, corrected from the brief: 17 unused months exist beyond the 681 test set (2025-01 through 2026-05); 23 beyond the training cutoff, 6 of which are the test set.
- The trimmed fetcher keeps `ingestion/bts.py`'s verified URL template, `REQUIRED_COLUMNS`, and the zip-member month-identity check, and lands to a local file. The GCS half of the module is deleted.

## Observed weather for the replay and drift windows

The replay week's weather comes out of the existing warehouse (silver ISD hourly), preserving the exact training semantics: the last observation at or before scheduled departure within a 3-hour ceiling. For the 2026 drift window there is no warehouse coverage; the accepted design scores both drift windows with the 12 weather features NULLed (`has_origin_weather=false` is an in-distribution, trained path), so no new observed-weather source is on the critical path. If a future pass wants real 2026 observations, the tradeoff is: a trimmed NOAA ISD station-year fetch preserves source parity with training; the IEM ASOS/METAR archive is operationally simpler but risks semantic drift on gust reporting, visibility censoring, precipitation, and weather-code flags.

## IEM archived TAFs (the forecast-substitution study)

All verified live:

- Bulk endpoint: `https://mesonet.agron.iastate.edu/cgi-bin/request/taf.py` with parameters `station` (single, comma-separated, or repeated), `sts`/`ets` (ISO bounds on TAF issuance time), `fmt` (`csv` or `excel`), plus `tz` and `last`. Self-documents at `?help=`. Free, keyless. The `/api/1/nws/taf` path exists but serves single issuances; use the cgi-bin route for bulk pulls.
- Archive depth: back to 1996-01-01 (stated on the download page and live-verified for multiple years), so the 2024-H2 replay week is covered with certainty.
- Output: decoded CSV, one row per forecast group (FM/TEMPO), columns including station, valid (issuance), fx_valid, fx_valid_end, sknt, drct, gust, visibility, presentwx, skyc, skyl, is_amendment.
- Known quirks: visibility greater than 6 miles is encoded as 6.01; amendments carry `is_amendment` and supersede scheduled issuances; scheduled TAFs come 4 times daily (00/06/12/18Z) with a 30-hour horizon confirmed live at majors (standard TAFs 24 hours; the 4-a-day cadence is standard practice, not re-verified against a primary document).
- Volume, measured: 134 to 348 KB per station-month decoded CSV across sampled airports, so roughly 55 to 95 MB for 374 airports for a full month, and about 15 to 25 MB for the one-week-plus-lookback pull this project needs (`data/weather/taf_week.csv`).
- Coverage plan: major terminals all issue TAFs; the tail of the 374 origins will not. Uncovered departures take the NULL weather path and are counted separately in the study.
- The substitution is partial by nature: TAF carries no temperature, dewpoint, or precipitation amount, so only 8 of the 12 weather features are mappable (wind speed, gust and gust-reported, visibility, and the fog/rain/snow/thunder flags). The other four go NULL, and that is part of the measured cost, stated in the results rather than imputed away.
- Standing guard: `forecast.issued_at <= prediction_time` for every substituted row, keyed by (station, issued_at, valid_at).

## $0 live weather endpoints (Tier 1 live mode reference; not on the critical path)

- `api.weather.gov`: free, keyless, verified live. Flow: `GET /points/{lat},{lon}` returns the gridpoint forecast URLs (`/gridpoints/{wfo}/{x},{y}/forecast` and `/forecast/hourly`). Etiquette: send an identifying User-Agent with contact info (documented requirement; enforcement observed lenient). Rate limits unpublished, described as generous (label: unverified beyond the docs' wording).
- `aviationweather.gov`: free, keyless, verified live. `GET /api/data/taf?ids=KSFO&format=json` returns the current decoded TAF. Docs state roughly 100 requests/minute with guidance to keep sustained rates near 1/minute per thread (label: from the docs page summary, not primary-source verified).

## Local streaming stack

- Images: `confluentinc/cp-kafka:8.3.1` and `confluentinc/cp-schema-registry:8.3.1`, both publishing arm64 (native on Apple Silicon). Confluent Platform 8.x is KRaft-only; ZooKeeper no longer exists.
- Confluent Cloud (screenshot deployment only): new organizations get free credit that expires 30 days after org creation (docs-verified); the widely cited $400 amount comes from marketing pages, not docs (label: unverified amount). Consequence: create the cloud org near demo time, and keep the grade independent of it. Basic clusters bill $0 at zero consumption after trial.
