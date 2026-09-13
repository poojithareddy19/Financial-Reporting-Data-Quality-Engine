-- 005: data quality schema
CREATE TABLE IF NOT EXISTS dq.rule_registry (
    rule_id         TEXT NOT NULL,
    rule_version    TEXT NOT NULL,           -- sha256 of the rule definition, first 12 chars
    rule_type       TEXT NOT NULL,
    description     TEXT NOT NULL,
    dimension       TEXT NOT NULL CHECK (dimension IN ('completeness','uniqueness','validity','consistency','timeliness','accuracy')),
    severity        TEXT NOT NULL CHECK (severity IN ('blocking','warning','info')),
    owner           TEXT NOT NULL,
    policy_section  TEXT NOT NULL,
    params          JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    loaded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_id, rule_version)
);

CREATE TABLE IF NOT EXISTS dq.rule_results (
    result_id       BIGSERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    run_date        DATE NOT NULL,
    rule_id         TEXT NOT NULL,
    rule_version    TEXT NOT NULL,
    dimension       TEXT NOT NULL,
    severity        TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pass','fail','error')),
    rows_checked    BIGINT NOT NULL,
    rows_failed     BIGINT NOT NULL,
    failure_rate    NUMERIC(9, 6) NOT NULL,
    sample_keys     JSONB NOT NULL DEFAULT '[]'::jsonb,
    duration_ms     INTEGER NOT NULL,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rule_results_run ON dq.rule_results (run_date, rule_id);
CREATE INDEX IF NOT EXISTS ix_rule_results_batch ON dq.rule_results (batch_id);

CREATE TABLE IF NOT EXISTS dq.anomaly_flags (
    flag_id         BIGSERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    run_date        DATE NOT NULL,
    method          TEXT NOT NULL,
    entity_id       TEXT,
    subject         TEXT NOT NULL,           -- e.g. a date, a transaction_id, a currency
    score           NUMERIC(18, 6) NOT NULL,
    threshold       NUMERIC(18, 6) NOT NULL,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    flagged_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_anomaly_flags_run ON dq.anomaly_flags (run_date, method);

CREATE TABLE IF NOT EXISTS dq.batch_run_log (
    log_id          BIGSERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    run_date        DATE NOT NULL,
    stage           TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('started','success','skipped','failed','aborted')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    source_file     TEXT,
    checksum        TEXT,
    rows_in         BIGINT,
    rows_out        BIGINT,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS ix_batch_run_log_batch ON dq.batch_run_log (batch_id, stage);
CREATE INDEX IF NOT EXISTS ix_batch_run_log_checksum ON dq.batch_run_log (checksum) WHERE stage = 'ingest';

CREATE TABLE IF NOT EXISTS dq.quarantine (
    quarantine_id   BIGSERIAL PRIMARY KEY,
    batch_id        TEXT NOT NULL,
    run_date        DATE NOT NULL,
    transaction_id  TEXT,
    rule_ids        TEXT[] NOT NULL,
    severity        TEXT NOT NULL,
    row_data        JSONB NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    released_at     TIMESTAMPTZ,
    released_by     TEXT
);
CREATE INDEX IF NOT EXISTS ix_quarantine_batch ON dq.quarantine (batch_id);
CREATE INDEX IF NOT EXISTS ix_quarantine_open ON dq.quarantine (run_date) WHERE released_at IS NULL;
