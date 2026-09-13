-- Report: month_over_month_variance
-- Business question: Which GL accounts moved the most month over month and year over year, and by how
--                    much, for the 13 months ending in the run date's month?
-- Grain: one row per account per month
-- Owner: fp-and-a@example.com
-- Params: :run_date
-- Sources: curated.fact_transactions, curated.dim_account
-- Notes: LAG over a complete month spine so a missing month yields NULL rather than a wrong comparison.
WITH months AS (
    SELECT DISTINCT month_start
    FROM curated.dim_date
    WHERE calendar_date BETWEEN (date_trunc('month', CAST(:run_date AS date)) - INTERVAL '24 months')::date
                            AND CAST(:run_date AS date)
),
monthly AS (
    SELECT f.account_id, date_trunc('month', f.posted_date)::date AS month_start, SUM(f.amount_usd) AS net_usd
    FROM curated.fact_transactions f
    WHERE f.posted_date BETWEEN (date_trunc('month', CAST(:run_date AS date)) - INTERVAL '24 months')::date
                            AND CAST(:run_date AS date)
    GROUP BY f.account_id, date_trunc('month', f.posted_date)
),
spine AS (
    SELECT a.account_id, a.account_code, a.account_name, a.account_type, m.month_start,
           COALESCE(x.net_usd, 0) AS net_usd
    FROM curated.dim_account a
    CROSS JOIN months m
    LEFT JOIN monthly x ON x.account_id = a.account_id AND x.month_start = m.month_start
),
lagged AS (
    SELECT *,
           LAG(net_usd, 1)  OVER (PARTITION BY account_id ORDER BY month_start) AS prior_month_usd,
           LAG(net_usd, 12) OVER (PARTITION BY account_id ORDER BY month_start) AS prior_year_usd
    FROM spine
),
variance AS (
    SELECT *,
           net_usd - prior_month_usd AS mom_change_usd,
           net_usd - prior_year_usd  AS yoy_change_usd,
           CASE WHEN prior_month_usd <> 0 THEN (net_usd - prior_month_usd) / ABS(prior_month_usd) * 100 END AS mom_change_pct,
           CASE WHEN prior_year_usd  <> 0 THEN (net_usd - prior_year_usd)  / ABS(prior_year_usd)  * 100 END AS yoy_change_pct
    FROM lagged
)
SELECT account_id, account_code, account_name, account_type, month_start,
       ROUND(net_usd, 2)         AS net_usd,
       ROUND(prior_month_usd, 2) AS prior_month_usd,
       ROUND(mom_change_usd, 2)  AS mom_change_usd,
       ROUND(mom_change_pct, 2)  AS mom_change_pct,
       ROUND(prior_year_usd, 2)  AS prior_year_usd,
       ROUND(yoy_change_usd, 2)  AS yoy_change_usd,
       ROUND(yoy_change_pct, 2)  AS yoy_change_pct,
       RANK() OVER (PARTITION BY month_start ORDER BY ABS(COALESCE(mom_change_usd, 0)) DESC) AS mom_mover_rank,
       RANK() OVER (PARTITION BY month_start ORDER BY ABS(COALESCE(yoy_change_usd, 0)) DESC) AS yoy_mover_rank
FROM variance
WHERE month_start >= (date_trunc('month', CAST(:run_date AS date)) - INTERVAL '12 months')::date
ORDER BY month_start DESC, mom_mover_rank, account_code;
