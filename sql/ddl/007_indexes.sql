-- 007: reporting indexes on the fact table. See docs/architecture.md#query-optimisation for the
-- EXPLAIN ANALYZE before/after that motivated each of these.
CREATE INDEX IF NOT EXISTS ix_fact_entity_date        ON curated.fact_transactions (entity_id, posted_date) INCLUDE (account_id, amount_usd);
CREATE INDEX IF NOT EXISTS ix_fact_account_date       ON curated.fact_transactions (account_id, posted_date);
CREATE INDEX IF NOT EXISTS ix_fact_customer_date      ON curated.fact_transactions (customer_id, posted_date) WHERE customer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_fact_reference          ON curated.fact_transactions (reference_id) WHERE reference_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_fact_batch              ON curated.fact_transactions (batch_id);
CREATE INDEX IF NOT EXISTS ix_fact_currency_date      ON curated.fact_transactions (currency, posted_date);
