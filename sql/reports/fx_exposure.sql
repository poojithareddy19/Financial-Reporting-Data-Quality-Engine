-- Report: fx_exposure
-- Business question: What is the unrealised FX impact on foreign-currency balance sheet positions
--                    (cash, receivables, inventory, other assets and liabilities) if revalued at the
--                    latest rate versus the rates at which they were booked?
-- Grain: one row per entity and non-USD currency
-- Owner: treasury@example.com
-- Params: :run_date
-- Sources: curated.fact_transactions, curated.dim_account, curated.dim_fx_rate
WITH latest_rate AS (
    SELECT DISTINCT ON (currency) currency, rate_date, rate_to_usd
    FROM curated.dim_fx_rate
    WHERE rate_date <= CAST(:run_date AS date)
    ORDER BY currency, rate_date DESC
),
positions AS (
    SELECT f.entity_id, f.currency,
           SUM(f.amount_local) AS local_balance,
           SUM(f.amount_usd)   AS booked_usd,
           COUNT(*)            AS postings
    FROM curated.fact_transactions f
    JOIN curated.dim_account a ON a.account_id = f.account_id
    WHERE a.account_type IN ('asset', 'liability')
      AND f.currency <> 'USD'
      AND f.posted_date > CAST(:run_date AS date) - INTERVAL '12 months'
      AND f.posted_date <= CAST(:run_date AS date)
    GROUP BY f.entity_id, f.currency
)
SELECT p.entity_id,
       p.currency,
       p.postings,
       ROUND(p.local_balance, 2)                       AS local_balance,
       ROUND(p.booked_usd, 2)                          AS booked_usd,
       CASE WHEN p.local_balance <> 0 THEN ROUND(p.booked_usd / p.local_balance, 6) END AS avg_booked_rate,
       lr.rate_to_usd                                  AS latest_rate,
       lr.rate_date                                    AS latest_rate_date,
       ROUND(p.local_balance * lr.rate_to_usd, 2)      AS revalued_usd,
       ROUND(p.local_balance * lr.rate_to_usd - p.booked_usd, 2) AS unrealized_fx_usd,
       CASE WHEN p.booked_usd <> 0
            THEN ROUND((p.local_balance * lr.rate_to_usd - p.booked_usd) / ABS(p.booked_usd) * 100, 2) END AS unrealized_fx_pct
FROM positions p
JOIN latest_rate lr ON lr.currency = p.currency
ORDER BY ABS(p.local_balance * lr.rate_to_usd - p.booked_usd) DESC, p.entity_id, p.currency;
