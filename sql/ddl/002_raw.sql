-- 002: raw layer. Landed as-is (all TEXT) with ingestion metadata.
CREATE TABLE IF NOT EXISTS raw.transactions (
    transaction_id  TEXT,
    event_id        TEXT,
    event_type      TEXT,
    reference_id    TEXT,
    posted_date     TEXT,
    created_at      TEXT,
    entity_id       TEXT,
    account_id      TEXT,
    customer_id     TEXT,
    currency        TEXT,
    amount_local    TEXT,
    description     TEXT,
    source_system   TEXT,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file    TEXT NOT NULL,
    _batch_id       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_raw_transactions_batch ON raw.transactions (_batch_id);

CREATE TABLE IF NOT EXISTS raw.fx_rates (
    currency        TEXT,
    rate_date       TEXT,
    rate_to_usd     TEXT,
    source          TEXT,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file    TEXT NOT NULL,
    _batch_id       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_raw_fx_rates_batch ON raw.fx_rates (_batch_id);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id     TEXT,
    customer_name   TEXT,
    customer_email  TEXT,
    segment         TEXT,
    region          TEXT,
    country         TEXT,
    customer_since  TEXT,
    effective_date  TEXT,
    _ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    _source_file    TEXT NOT NULL,
    _batch_id       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_raw_customers_batch ON raw.customers (_batch_id);
