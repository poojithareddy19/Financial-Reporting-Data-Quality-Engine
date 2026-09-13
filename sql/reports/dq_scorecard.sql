-- Report: dq_scorecard
-- Business question: Over the last 30 pipeline runs, how often does each rule pass or fail, how many
--                    rows fail, and how does that roll up by dimension and severity?
-- Grain: one row per rule (dimension and severity carried for roll-ups), plus per-run pass rates
-- Owner: data-platform@example.com
-- Params: :run_date
-- Sources: dq.rule_results, dq.rule_registry
WITH recent_runs AS (
    SELECT DISTINCT run_date
    FROM dq.rule_results
    WHERE run_date <= CAST(:run_date AS date)
    ORDER BY run_date DESC
    LIMIT 30
),
scoped AS (
    SELECT r.*
    FROM dq.rule_results r
    JOIN recent_runs rr ON rr.run_date = r.run_date
),
per_rule AS (
    SELECT rule_id, dimension, severity,
           COUNT(*)                          AS runs,
           COUNT(*) FILTER (WHERE status = 'pass')  AS passes,
           COUNT(*) FILTER (WHERE status = 'fail')  AS fails,
           COUNT(*) FILTER (WHERE status = 'error') AS errors,
           SUM(rows_checked)                 AS rows_checked,
           SUM(rows_failed)                  AS rows_failed,
           MAX(run_date) FILTER (WHERE status = 'fail') AS last_failed_on
    FROM scoped
    GROUP BY rule_id, dimension, severity
)
SELECT p.rule_id,
       g.description,
       g.owner,
       p.dimension,
       p.severity,
       p.runs, p.passes, p.fails, p.errors,
       ROUND(p.passes::numeric / NULLIF(p.runs, 0) * 100, 2)              AS run_pass_rate_pct,
       p.rows_checked, p.rows_failed,
       ROUND((1 - p.rows_failed::numeric / NULLIF(p.rows_checked, 0)) * 100, 4) AS row_pass_rate_pct,
       p.last_failed_on,
       SUM(p.fails) OVER (PARTITION BY p.dimension) AS dimension_fails,
       SUM(p.fails) OVER (PARTITION BY p.severity)  AS severity_fails,
       (SELECT MIN(run_date) FROM recent_runs)       AS window_start,
       (SELECT MAX(run_date) FROM recent_runs)       AS window_end
FROM per_rule p
JOIN LATERAL (
    SELECT description, owner FROM dq.rule_registry g
    WHERE g.rule_id = p.rule_id ORDER BY is_active DESC, loaded_at DESC LIMIT 1
) g ON TRUE
ORDER BY p.severity, p.fails DESC, p.rule_id;
