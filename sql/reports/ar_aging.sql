-- Report: ar_aging
-- Business question: How much is each customer's open receivable balance as of the run date, and how
--                    old is it (0-30, 31-60, 61-90, 90+ days)?
-- Grain: one row per customer with an open balance
-- Owner: finance-ops@example.com
-- Params: :run_date
-- Sources: curated.fact_transactions, curated.dim_account, curated.dim_customer
-- Notes: Invoices are matched to payments on reference_id. Uses ix_fact_reference and ix_fact_account_date.
WITH ar_legs AS (
    SELECT f.reference_id, f.customer_id, f.posted_date, f.amount_usd
    FROM curated.fact_transactions f
    JOIN curated.dim_account a ON a.account_id = f.account_id
    WHERE a.account_code = '1100'
      AND f.posted_date <= CAST(:run_date AS date)
      AND f.reference_id IS NOT NULL
),
invoices AS (
    SELECT reference_id,
           MAX(customer_id)                                             AS customer_id,
           MIN(posted_date) FILTER (WHERE amount_usd > 0)               AS invoice_date,
           SUM(amount_usd)                                              AS open_usd
    FROM ar_legs
    GROUP BY reference_id
    HAVING SUM(amount_usd) > 0.005 AND MIN(posted_date) FILTER (WHERE amount_usd > 0) IS NOT NULL
),
aged AS (
    SELECT customer_id,
           open_usd,
           CAST(:run_date AS date) - invoice_date AS age_days,
           CASE WHEN CAST(:run_date AS date) - invoice_date <= 30 THEN 'b0_30'
                WHEN CAST(:run_date AS date) - invoice_date <= 60 THEN 'b31_60'
                WHEN CAST(:run_date AS date) - invoice_date <= 90 THEN 'b61_90'
                ELSE 'b90_plus' END AS bucket
    FROM invoices
)
SELECT ag.customer_id,
       c.customer_name,
       c.customer_email,
       c.segment,
       ROUND(SUM(open_usd) FILTER (WHERE bucket = 'b0_30'), 2)    AS bucket_0_30,
       ROUND(SUM(open_usd) FILTER (WHERE bucket = 'b31_60'), 2)   AS bucket_31_60,
       ROUND(SUM(open_usd) FILTER (WHERE bucket = 'b61_90'), 2)   AS bucket_61_90,
       ROUND(SUM(open_usd) FILTER (WHERE bucket = 'b90_plus'), 2) AS bucket_90_plus,
       ROUND(SUM(open_usd), 2)                                    AS total_open_usd,
       ROUND(COALESCE(SUM(open_usd) FILTER (WHERE bucket = 'b90_plus'), 0) / SUM(open_usd) * 100, 2) AS pct_over_90,
       COUNT(*)                                                   AS open_invoices,
       MAX(age_days)                                              AS oldest_invoice_days
FROM aged ag
LEFT JOIN curated.dim_customer c ON c.customer_id = ag.customer_id AND c.is_current
GROUP BY ag.customer_id, c.customer_name, c.customer_email, c.segment
ORDER BY total_open_usd DESC, ag.customer_id;
