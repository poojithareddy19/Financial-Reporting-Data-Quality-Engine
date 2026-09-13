-- 004: curated star schema
CREATE TABLE IF NOT EXISTS curated.dim_account (
    account_id      TEXT PRIMARY KEY,
    account_code    TEXT NOT NULL UNIQUE,
    account_name    TEXT NOT NULL,
    account_type    TEXT NOT NULL CHECK (account_type IN ('asset','liability','equity','revenue','cogs','expense')),
    normal_balance  TEXT NOT NULL CHECK (normal_balance IN ('debit','credit'))
);

CREATE TABLE IF NOT EXISTS curated.dim_entity (
    entity_id           TEXT PRIMARY KEY,
    entity_code         TEXT NOT NULL UNIQUE,
    entity_name         TEXT NOT NULL,
    country             TEXT NOT NULL,
    functional_currency TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS curated.dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     TEXT NOT NULL,
    customer_name   TEXT NOT NULL,
    customer_email  TEXT,
    segment         TEXT,
    region          TEXT,
    country         TEXT,
    customer_since  DATE,
    valid_from      DATE NOT NULL,
    valid_to        DATE NOT NULL DEFAULT DATE '9999-12-31',
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_dim_customer_version UNIQUE (customer_id, valid_from)
);
CREATE INDEX IF NOT EXISTS ix_dim_customer_current ON curated.dim_customer (customer_id) WHERE is_current;

CREATE TABLE IF NOT EXISTS curated.dim_date (
    date_key        INTEGER PRIMARY KEY,
    calendar_date   DATE NOT NULL UNIQUE,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    month_start     DATE NOT NULL,
    month_end       DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS curated.dim_fx_rate (
    currency        TEXT NOT NULL,
    rate_date       DATE NOT NULL,
    rate_to_usd     NUMERIC(18, 8) NOT NULL,
    source          TEXT,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (currency, rate_date)
);

CREATE TABLE IF NOT EXISTS curated.fact_transactions (
    transaction_id  TEXT PRIMARY KEY,
    event_id        TEXT,
    event_type      TEXT,
    reference_id    TEXT,
    posted_date     DATE NOT NULL,
    created_at      TIMESTAMPTZ,
    entity_id       TEXT NOT NULL REFERENCES curated.dim_entity (entity_id),
    account_id      TEXT NOT NULL REFERENCES curated.dim_account (account_id),
    customer_id     TEXT,
    currency        TEXT NOT NULL,
    amount_local    NUMERIC(18, 2) NOT NULL,
    amount_usd      NUMERIC(18, 2) NOT NULL,
    fx_rate         NUMERIC(18, 8) NOT NULL,
    description     TEXT,
    source_system   TEXT,
    dq_score        INTEGER NOT NULL CHECK (dq_score BETWEEN 0 AND 100),
    batch_id        TEXT NOT NULL,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
