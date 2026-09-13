-- Report: late_arriving_data
-- Business question: Which source systems deliver postings long after their accounting date, and how
--                    much money arrives more than N days late in the batches for the run date?
-- Grain: one row per source system for the run date's batches
-- Owner: data-platform@example.com
-- Params: :run_date, :late_days
-- Sources: curated.fact_transactions, dq.batch_run_log
WITH batches AS (
    SELECT DISTINCT batch_id, run_date
    FROM dq.batch_run_log
    WHERE stage = 'ingest' AND status = 'success' AND run_date = CAST(:run_date AS date)
),
lagged AS (
    SELECT f.source_system,
           f.transaction_id,
           f.amount_usd,
           b.run_date - f.posted_date AS lag_days
    FROM curated.fact_transactions f
    JOIN batches b ON b.batch_id = f.batch_id
)
SELECT source_system,
       COUNT(*)                                          AS rows_in_batch,
       COUNT(*) FILTER (WHERE lag_days > :late_days)     AS late_rows,
       ROUND(COUNT(*) FILTER (WHERE lag_days > :late_days)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS late_pct,
       ROUND(SUM(ABS(amount_usd)) FILTER (WHERE lag_days > :late_days), 2) AS late_abs_usd,
       MAX(lag_days)                                     AS max_lag_days,
       ROUND(AVG(lag_days) FILTER (WHERE lag_days > :late_days), 1) AS avg_late_lag_days,
       PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lag_days) AS p95_lag_days
FROM lagged
GROUP BY source_system
ORDER BY late_rows DESC, source_system;
