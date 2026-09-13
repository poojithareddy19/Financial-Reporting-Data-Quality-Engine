-- Report: daily_revenue_by_entity
-- Business question: How is daily revenue, COGS and gross margin trending per legal entity, smoothed
--                    over 7 and 28 days, for the 90 days ending on the run date?
-- Grain: one row per entity per posting day
-- Owner: finance-ops@example.com
-- Params: :run_date
-- Sources: curated.fact_transactions, curated.dim_account, curated.dim_entity
-- Notes: Uses ix_fact_entity_date (entity_id, posted_date) INCLUDE (account_id, amount_usd).
WITH scoped AS (
    SELECT f.entity_id, f.posted_date, a.account_type, f.amount_usd
    FROM curated.fact_transactions f
    JOIN curated.dim_account a ON a.account_id = f.account_id
    WHERE f.posted_date BETWEEN CAST(:run_date AS date) - 89 AND CAST(:run_date AS date)
      AND a.account_type IN ('revenue', 'cogs')
),
daily AS (
    SELECT entity_id,
           posted_date,
           -SUM(amount_usd) FILTER (WHERE account_type = 'revenue') AS revenue_usd,
            SUM(amount_usd) FILTER (WHERE account_type = 'cogs')    AS cogs_usd
    FROM scoped
    GROUP BY entity_id, posted_date
),
windowed AS (
    SELECT d.entity_id,
           e.entity_code,
           d.posted_date,
           COALESCE(d.revenue_usd, 0) AS revenue_usd,
           COALESCE(d.cogs_usd, 0)    AS cogs_usd,
           COALESCE(d.revenue_usd, 0) - COALESCE(d.cogs_usd, 0) AS gross_margin_usd,
           AVG(COALESCE(d.revenue_usd, 0)) OVER (PARTITION BY d.entity_id ORDER BY d.posted_date
                                                  ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)  AS revenue_ma_7d,
           AVG(COALESCE(d.revenue_usd, 0)) OVER (PARTITION BY d.entity_id ORDER BY d.posted_date
                                                  ROWS BETWEEN 27 PRECEDING AND CURRENT ROW) AS revenue_ma_28d
    FROM daily d
    JOIN curated.dim_entity e ON e.entity_id = d.entity_id
)
SELECT entity_id,
       entity_code,
       posted_date,
       ROUND(revenue_usd, 2)      AS revenue_usd,
       ROUND(cogs_usd, 2)         AS cogs_usd,
       ROUND(gross_margin_usd, 2) AS gross_margin_usd,
       CASE WHEN revenue_usd <> 0 THEN ROUND(gross_margin_usd / revenue_usd * 100, 2) END AS gross_margin_pct,
       ROUND(revenue_ma_7d, 2)    AS revenue_ma_7d,
       ROUND(revenue_ma_28d, 2)   AS revenue_ma_28d,
       CASE WHEN revenue_ma_28d > 0 THEN ROUND(revenue_usd / revenue_ma_28d, 3) END AS ratio_to_ma_28d
FROM windowed
ORDER BY entity_id, posted_date;
