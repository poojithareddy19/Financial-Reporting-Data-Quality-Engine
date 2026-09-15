-- 009: enforce the SCD2 invariant that a customer has exactly one current version.
-- 004 created this as a plain index, so a batch that produced two current rows corrupted the
-- dimension silently and every join on is_current double-counted that customer. Making it unique
-- turns that into an error at write time. If this migration fails, the dimension already holds
-- duplicates from before the fix: find them with
--   SELECT customer_id FROM curated.dim_customer WHERE is_current GROUP BY 1 HAVING count(*) > 1;
DROP INDEX IF EXISTS curated.ix_dim_customer_current;

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_customer_current
    ON curated.dim_customer (customer_id) WHERE is_current;
