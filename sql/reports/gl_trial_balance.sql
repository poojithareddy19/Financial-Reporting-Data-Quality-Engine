-- Report: gl_trial_balance
-- Business question: Do debits equal credits for every entity and month, and which accounts carry
--                    unbalanced postings (a leg whose double-entry pair is missing or mismatched)?
-- Grain: one row per entity, month and account, plus entity-month balance flags
-- Owner: finance-ops@example.com
-- Params: :run_date
-- Sources: curated.fact_transactions, curated.dim_account, curated.dim_entity
-- Notes: A posting is "unbalanced" when the legs sharing its event_id do not net to zero. Because
--        quarantined legs never reach curated, this is where DQ failures surface in the books.
--        Optimised: event nets are pre-aggregated with GROUP BY + HAVING and left-joined back, instead
--        of two window functions over every leg. Same output, 31% faster, no disk sort
--        (docs/architecture.md#query-optimisation, docs/explain/gl_trial_balance_*.txt).
WITH scoped AS (
    SELECT f.event_id, f.entity_id, f.account_id, f.amount_usd,
           date_trunc('month', f.posted_date)::date AS month_start
    FROM curated.fact_transactions f
    WHERE f.posted_date <= CAST(:run_date AS date)
),
event_net AS (
    SELECT event_id, SUM(amount_usd) AS event_net_usd, COUNT(*) AS event_legs
    FROM scoped GROUP BY event_id
    HAVING ABS(SUM(amount_usd)) > 0.005 OR COUNT(*) % 2 = 1
),
by_account AS (
    SELECT s.entity_id, s.month_start, s.account_id,
           SUM(s.amount_usd)  FILTER (WHERE s.amount_usd > 0) AS debits_usd,
           -SUM(s.amount_usd) FILTER (WHERE s.amount_usd < 0) AS credits_usd,
           SUM(s.amount_usd)                                  AS net_usd,
           SUM(s.amount_usd)  FILTER (WHERE e.event_id IS NOT NULL) AS unbalanced_usd,
           COUNT(*)           FILTER (WHERE e.event_id IS NOT NULL) AS unbalanced_legs
    FROM scoped s
    LEFT JOIN event_net e ON e.event_id = s.event_id
    GROUP BY s.entity_id, s.month_start, s.account_id
),
entity_month AS (
    SELECT entity_id, month_start,
           SUM(COALESCE(debits_usd, 0))  AS entity_debits_usd,
           SUM(COALESCE(credits_usd, 0)) AS entity_credits_usd
    FROM by_account
    GROUP BY entity_id, month_start
)
SELECT b.entity_id, e.entity_code, b.month_start, a.account_code, a.account_name, a.account_type, a.normal_balance,
       ROUND(COALESCE(b.debits_usd, 0), 2)  AS debits_usd,
       ROUND(COALESCE(b.credits_usd, 0), 2) AS credits_usd,
       ROUND(b.net_usd, 2)                  AS net_usd,
       ROUND(COALESCE(b.unbalanced_usd, 0), 2) AS unbalanced_usd,
       b.unbalanced_legs,
       (COALESCE(b.unbalanced_legs, 0) > 0)  AS account_flag,
       ROUND(em.entity_debits_usd, 2)       AS entity_debits_usd,
       ROUND(em.entity_credits_usd, 2)      AS entity_credits_usd,
       ROUND(em.entity_debits_usd - em.entity_credits_usd, 2) AS entity_imbalance_usd,
       (ABS(em.entity_debits_usd - em.entity_credits_usd) < 0.01) AS entity_month_balanced
FROM by_account b
JOIN curated.dim_account a ON a.account_id = b.account_id
JOIN curated.dim_entity  e ON e.entity_id = b.entity_id
JOIN entity_month em ON em.entity_id = b.entity_id AND em.month_start = b.month_start
ORDER BY b.entity_id, b.month_start, a.account_code;
