-- 008: schema contracts for the raw feeds
-- Two tables with deliberately different lifetimes. The registry is the current promise for each feed
-- and is synced from contracts/*.avsc on every run. contract_events is the append-only record of what
-- each file actually delivered, written on its own transaction so a breaking verdict survives the
-- rollback it causes.
CREATE TABLE IF NOT EXISTS governance.contract_registry (
    contract_id      BIGSERIAL PRIMARY KEY,
    feed             TEXT NOT NULL,
    contract_name    TEXT NOT NULL,
    version          TEXT NOT NULL,           -- short sha256 over the structural canonical form
    owner            TEXT NOT NULL,           -- team accountable for the feed, named in every incident
    producing_system TEXT NOT NULL,
    delivery_window  TEXT NOT NULL,
    fields           TEXT[] NOT NULL,
    nullable_fields  TEXT[] NOT NULL DEFAULT '{}',
    definition       JSONB NOT NULL,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_contract_registry_feed_version UNIQUE (feed, version)
);

CREATE TABLE IF NOT EXISTS governance.contract_events (
    event_id          BIGSERIAL PRIMARY KEY,
    batch_id          TEXT,                   -- null for a breaking verdict: no batch was ever opened
    run_date          DATE NOT NULL,
    feed              TEXT NOT NULL,
    contract_name     TEXT NOT NULL,
    version           TEXT NOT NULL,
    owner             TEXT NOT NULL,
    verdict           TEXT NOT NULL,          -- compatible | additive | breaking
    missing_fields    TEXT[] NOT NULL DEFAULT '{}',
    duplicated_fields TEXT[] NOT NULL DEFAULT '{}',
    unknown_fields    TEXT[] NOT NULL DEFAULT '{}',
    source_file       TEXT,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_contract_events_verdict CHECK (verdict IN ('compatible', 'additive', 'breaking'))
);

CREATE INDEX IF NOT EXISTS ix_contract_events_feed ON governance.contract_events (feed, run_date, observed_at);
CREATE INDEX IF NOT EXISTS ix_contract_events_verdict ON governance.contract_events (verdict, observed_at);
