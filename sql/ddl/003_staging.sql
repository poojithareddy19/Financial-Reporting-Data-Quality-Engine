-- 003: staging layer. Typed, deduplicated, FX-normalised to USD. One batch at a time.
CREATE TABLE IF NOT EXISTS staging.transactions (
    staging_row_id  BIGSERIAL PRIMARY KEY,
    transaction_id  TEXT,
    event_id        TEXT,
    event_type      TEXT,
    reference_id    TEXT,
    posted_date     DATE,
    created_at      TIMESTAMPTZ,
    entity_id       TEXT,
    account_id      TEXT,
    customer_id     TEXT,
    currency        TEXT,
    amount_local    NUMERIC(18, 2),
    amount_usd      NUMERIC(18, 2),
    fx_rate         NUMERIC(18, 8),
    description     TEXT,
    source_system   TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL,
    batch_id        TEXT NOT NULL,
    dq_score        INTEGER,
    quarantined     BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS ix_staging_transactions_batch ON staging.transactions (batch_id);
CREATE INDEX IF NOT EXISTS ix_staging_transactions_txn ON staging.transactions (batch_id, transaction_id);

CREATE TABLE IF NOT EXISTS staging.fx_rates (
    currency        TEXT NOT NULL,
    rate_date       DATE NOT NULL,
    rate_to_usd     NUMERIC(18, 8) NOT NULL,
    source          TEXT,
    batch_id        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id     TEXT NOT NULL,
    customer_name   TEXT,
    customer_email  TEXT,
    segment         TEXT,
    region          TEXT,
    country         TEXT,
    customer_since  DATE,
    effective_date  DATE NOT NULL,
    batch_id        TEXT NOT NULL
);
