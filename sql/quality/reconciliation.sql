-- Check: reconciliation
-- Purpose: prove that every raw row for the batch is accounted for as a curated row, a quarantined row
--          or a removed duplicate, and that amounts tie out (raw sum = curated sum + quarantined sum).
-- Grain: one row per batch
-- Params: :batch_id
WITH raw_side AS (
    SELECT COUNT(*) AS raw_rows,
           COALESCE(SUM(CASE WHEN amount_local ~ '^-?[0-9]+(\.[0-9]+)?$' THEN amount_local::numeric END), 0) AS raw_sum_local
    FROM raw.transactions WHERE _batch_id = :batch_id
),
staged AS (
    SELECT COUNT(*) AS staged_rows,
           COUNT(*) FILTER (WHERE quarantined) AS quarantined_rows,
           COALESCE(SUM(amount_local) FILTER (WHERE quarantined), 0) AS quarantined_sum_local
    FROM staging.transactions WHERE batch_id = :batch_id
),
dupes AS (
    SELECT COALESCE((details->>'duplicates_removed')::bigint, 0) AS duplicates_removed
    FROM dq.batch_run_log WHERE batch_id = :batch_id AND stage = 'transform' AND status = 'success'
    ORDER BY started_at DESC LIMIT 1
),
curated_side AS (
    SELECT COUNT(*) AS curated_rows, COALESCE(SUM(amount_local), 0) AS curated_sum_local
    FROM curated.fact_transactions WHERE batch_id = :batch_id
),
dup_sum AS (
    SELECT COALESCE(SUM(amount_local::numeric), 0) AS duplicate_sum_local
    FROM (
        SELECT amount_local, ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _ingested_at) AS rn
        FROM raw.transactions
        WHERE _batch_id = :batch_id AND transaction_id IS NOT NULL
          AND amount_local ~ '^-?[0-9]+(\.[0-9]+)?$'
    ) d WHERE rn > 1
)
SELECT :batch_id AS batch_id,
       r.raw_rows, s.staged_rows, s.quarantined_rows, d.duplicates_removed, c.curated_rows,
       r.raw_rows - d.duplicates_removed - s.quarantined_rows - c.curated_rows AS row_gap,
       ROUND(r.raw_sum_local, 2) AS raw_sum_local,
       ROUND(c.curated_sum_local, 2) AS curated_sum_local,
       ROUND(s.quarantined_sum_local, 2) AS quarantined_sum_local,
       ROUND(ds.duplicate_sum_local, 2) AS duplicate_sum_local,
       ROUND(r.raw_sum_local - c.curated_sum_local - s.quarantined_sum_local - ds.duplicate_sum_local, 2) AS amount_gap,
       (r.raw_rows - d.duplicates_removed - s.quarantined_rows - c.curated_rows = 0
        AND ABS(r.raw_sum_local - c.curated_sum_local - s.quarantined_sum_local - ds.duplicate_sum_local) < 0.01) AS reconciled
FROM raw_side r, staged s, dupes d, curated_side c, dup_sum ds;
