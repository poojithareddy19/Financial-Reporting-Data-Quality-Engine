-- 006: governance schema
CREATE TABLE IF NOT EXISTS governance.audit_log (
    audit_id        BIGSERIAL PRIMARY KEY,
    batch_id        TEXT,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,           -- insert | upsert | delete | truncate | release
    target_schema   TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    row_count       BIGINT NOT NULL,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_audit_log_target ON governance.audit_log (target_schema, target_table, occurred_at);

CREATE TABLE IF NOT EXISTS governance.lineage (
    lineage_id      BIGSERIAL PRIMARY KEY,
    report_name     TEXT NOT NULL,
    run_date        DATE NOT NULL,
    batch_id        TEXT NOT NULL,
    source_tables   TEXT[] NOT NULL,
    source_batches  TEXT[] NOT NULL,
    rule_versions   JSONB NOT NULL,
    output_path     TEXT NOT NULL,
    row_count       BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lineage_report ON governance.lineage (report_name, run_date);

CREATE TABLE IF NOT EXISTS governance.pii_access_log (
    access_id       BIGSERIAL PRIMARY KEY,
    actor           TEXT NOT NULL,
    role            TEXT NOT NULL,
    report_name     TEXT NOT NULL,
    columns         TEXT[] NOT NULL,
    masked          BOOLEAN NOT NULL,
    accessed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS governance.retention_log (
    retention_id    BIGSERIAL PRIMARY KEY,
    retention_class TEXT NOT NULL,
    target_table    TEXT NOT NULL,
    cutoff_date     DATE NOT NULL,
    rows_deleted    BIGINT NOT NULL,
    dry_run         BOOLEAN NOT NULL,
    actor           TEXT NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
