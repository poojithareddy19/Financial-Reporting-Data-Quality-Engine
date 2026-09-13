-- Report: customer_concentration
-- Business question: How concentrated is trailing-12-month revenue? Top 10 customers, their share,
--                    cumulative share and the Herfindahl-Hirschman index across all customers.
-- Grain: one row per top-10 customer (HHI and totals repeated on each row)
-- Owner: sales-ops@example.com
-- Params: :run_date
-- Sources: curated.fact_transactions, curated.dim_account, curated.dim_customer
-- Notes: RANK() lives in its own CTE on purpose. Combining it with a differently framed window
--        (the cumulative SUM) in one SELECT and then filtering "rank <= 10" lets PostgreSQL 16 turn
--        the predicate into a window run condition that is silently dropped (verified on 16.15,
--        see docs/architecture.md#postgresql-run-condition-pitfall).
WITH revenue AS (
    SELECT f.customer_id, -SUM(f.amount_usd) AS revenue_usd
    FROM curated.fact_transactions f
    JOIN curated.dim_account a ON a.account_id = f.account_id
    WHERE a.account_type = 'revenue'
      AND f.customer_id IS NOT NULL
      AND f.posted_date > CAST(:run_date AS date) - INTERVAL '12 months'
      AND f.posted_date <= CAST(:run_date AS date)
    GROUP BY f.customer_id
    HAVING -SUM(f.amount_usd) > 0
),
totals AS (
    SELECT SUM(revenue_usd) AS total_revenue_usd, COUNT(*) AS customer_count FROM revenue
),
hhi AS (
    SELECT SUM(POWER(r.revenue_usd / t.total_revenue_usd, 2)) * 10000 AS hhi
    FROM revenue r CROSS JOIN totals t
),
ranked AS (
    SELECT customer_id, revenue_usd,
           RANK() OVER (ORDER BY revenue_usd DESC, customer_id) AS revenue_rank
    FROM revenue
),
shares AS (
    SELECT r.customer_id,
           r.revenue_usd,
           r.revenue_rank,
           r.revenue_usd / t.total_revenue_usd AS share,
           SUM(r.revenue_usd) OVER (ORDER BY r.revenue_rank ROWS UNBOUNDED PRECEDING) / t.total_revenue_usd AS cumulative_share,
           h.hhi,
           t.customer_count,
           t.total_revenue_usd
    FROM ranked r
    CROSS JOIN totals t
    CROSS JOIN hhi h
)
SELECT s.revenue_rank,
       s.customer_id,
       c.customer_name,
       c.segment,
       ROUND(s.revenue_usd, 2)              AS revenue_usd,
       ROUND(s.share * 100, 2)              AS share_pct,
       ROUND(s.cumulative_share * 100, 2)   AS cumulative_share_pct,
       ROUND(s.hhi, 1)                      AS hhi,
       s.customer_count,
       ROUND(s.total_revenue_usd, 2)        AS total_revenue_usd
FROM shares s
LEFT JOIN curated.dim_customer c ON c.customer_id = s.customer_id AND c.is_current
WHERE s.revenue_rank <= 10
ORDER BY s.revenue_rank;
