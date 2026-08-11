-- fact_flights pruning benchmark: the representative dashboard drill-down,
-- as TWO runnable blocks — identical except for the table — so both variants
-- execute as written with zero manual editing.
--
-- Setup — baseline recreate DDL (plain CTAS: no PARTITION BY, no CLUSTER BY):
--   CREATE OR REPLACE TABLE `de-flight-project.flight_delays_gold.fact_flights_benchmark_baseline`
--   AS SELECT * FROM `de-flight-project.flight_delays_gold.fact_flights`;
--
-- Measure from EXECUTED job statistics (dry-run bytes are an UPPER_BOUND
-- estimate for clustered tables, not exact). Run each block as its OWN job
-- with the cache disabled. Feed the SQL on stdin via a heredoc with a QUOTED
-- delimiter — the blocks contain single-quoted literals ('2024-06-01', 'ORD')
-- that break if the SQL itself is shell-quoted:
--
--   bq query --nouse_legacy_sql --nouse_cache --job_id=<id> <<'SQL'
--   <paste exactly one block here>
--   SQL
--
--   bq show --format=prettyjson -j <id>   # statistics.query.totalBytesProcessed / totalBytesBilled
--
-- Job ids are UNIQUE per project: use a fresh <id> for every run (the README's
-- 3 runs/variant needs six, e.g. opt_1..opt_3, base_1..base_3). Reusing an id
-- fails with "Already Exists" — and `bq show -j` would then return the FIRST
-- job's statistics, silently recording the wrong run.
--
-- Do NOT pipe this whole file to bq: two statements become one SCRIPT job
-- whose parent statistics blend BOTH variants. One block per invocation.
--
-- Teardown — after measuring, remove the baseline copy (it is not dbt-managed):
--   DROP TABLE `de-flight-project.flight_delays_gold.fact_flights_benchmark_baseline`;


-- ============================================================================
-- BLOCK 1 — OPTIMIZED: fact_flights (month partitions; clustered by
-- origin/dest/carrier — this query prunes on the first clustering column)
-- ============================================================================
SELECT
  carrier_key,
  COUNT(*) AS n_flights,
  COUNTIF(arr_del15) / NULLIF(COUNTIF(arr_del15 IS NOT NULL), 0) AS arr_del15_rate,
  AVG(arr_delay_minutes) AS avg_arr_delay_minutes,
  COUNTIF(cancelled) AS n_cancelled
FROM `de-flight-project.flight_delays_gold.fact_flights`
WHERE date_key BETWEEN '2024-06-01' AND '2024-06-30'
  AND origin_airport_key = 'ORD'
GROUP BY carrier_key
ORDER BY n_flights DESC;


-- ============================================================================
-- BLOCK 2 — BASELINE: fact_flights_benchmark_baseline (unpartitioned,
-- unclustered copy from the recreate DDL above)
-- ============================================================================
SELECT
  carrier_key,
  COUNT(*) AS n_flights,
  COUNTIF(arr_del15) / NULLIF(COUNTIF(arr_del15 IS NOT NULL), 0) AS arr_del15_rate,
  AVG(arr_delay_minutes) AS avg_arr_delay_minutes,
  COUNTIF(cancelled) AS n_cancelled
FROM `de-flight-project.flight_delays_gold.fact_flights_benchmark_baseline`
WHERE date_key BETWEEN '2024-06-01' AND '2024-06-30'
  AND origin_airport_key = 'ORD'
GROUP BY carrier_key
ORDER BY n_flights DESC;
