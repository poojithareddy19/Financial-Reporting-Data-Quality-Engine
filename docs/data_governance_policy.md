# Data Governance Policy: Financial Reporting Warehouse

Version 1.0. Owner: data-platform@example.com. Review cadence: quarterly.

## 1. Scope

Applies to every table in the `raw`, `staging`, `curated`, `dq` and `governance` schemas, the files in the raw,
staged and curated buckets, and every report produced from them.

## 2. Ownership

| Domain | Business owner | Technical owner | Tables / reports |
|---|---|---|---|
| General ledger | finance-ops@example.com | data-platform@example.com | fact_transactions, dim_account, dim_entity, gl_trial_balance, daily_revenue_by_entity, ar_aging |
| Customers | sales-ops@example.com | data-platform@example.com | dim_customer, customer_concentration |
| Treasury | treasury@example.com | data-platform@example.com | dim_fx_rate, fx_exposure |
| FP&A | fp-and-a@example.com | data-platform@example.com | month_over_month_variance |
| Data quality | data-platform@example.com | data-platform@example.com | dq.*, dq_scorecard, late_arriving_data |

Every curated column has a documented owner, type, description, PII flag and retention class in
`config/data_dictionary.yaml`. A column that is not in the dictionary fails CI
(`tests/unit/test_data_dictionary.py`).

## 3. Quality dimensions and SLAs

| Dimension | Definition | SLA (per daily batch) |
|---|---|---|
| 3.1 Completeness | Required fields are present | 99.5% of rows have transaction_id, posted_date, amount_local; 98% of sale/payment rows have customer_id |
| 3.2 Uniqueness | One row per transaction_id after deduplication | 100% |
| 3.3 Validity | Values conform to governed formats and code lists | 99.5% (currency, id pattern, date range) |
| 3.4 Consistency | Foreign keys resolve; FX resolves | 99.5% referential integrity to dim_account and dim_entity; 100% of loaded rows priced in USD |
| 3.5 Timeliness | Data lands within 3 days of the accounting date | max posted_date within 3 days of run date; late rows reported by source |
| 3.6 Accuracy | Amounts are plausible and correctly signed | 99.9% of revenue postings on sale events are credits |

The batch-level SLA is a **DQ pass rate of at least 95%** (share of rows not quarantined). Breaching it raises the
CloudWatch alarm `fin-dq-dq-pass-rate-below-95`. Blocking failures above **5% of the batch** abort the load.

## 4. Severity definitions

| Severity | Effect on the row | Effect on the batch | Example |
|---|---|---|---|
| blocking | Quarantined, never loaded | Aborts load if rate > 5% | null transaction_id, unknown account |
| warning | Loaded with dq_score reduced by 20 per failure | Reported, no abort | missing customer on a sale, sign anomaly |
| info | Loaded with dq_score reduced by 5 | Reported | blank description |

`dq_score` is 0-100 per row (`quality/scoring.py`) and stored on `fact_transactions` so analysts can filter.

## 5. Escalation

1. Pipeline aborted or a stage failed: SNS failure topic pages the data-platform on-call; respond within 1 business hour;
   follow `docs/runbook.md`.
2. DQ pass rate below 95% but load completed: data-platform reviews `dq_scorecard` the same day and notifies the
   affected business owner.
3. Anomaly flagged (revenue spike, FX jump, Benford, Isolation Forest): included in the daily summary; the business
   owner acknowledges within 2 business days.
4. PII access without the `finance_admin` role: impossible by construction (masking); any unmasked access is logged in
   `governance.pii_access_log` and reviewed monthly by finance-ops.

## 6. PII handling

`customer_name` and `customer_email` are PII. They are masked in every report output (`j***@example.com`,
`Ac***9f1e3b`) unless the caller runs with `role: finance_admin`. Every report that contains PII columns writes a
`governance.pii_access_log` row with actor, role, columns and whether masking applied. Raw and staging schemas are
restricted to the pipeline role in AWS (IAM) and are never exposed to the dashboard.

## 7. Retention

| Class | Tables | Retention | Enforcement |
|---|---|---|---|
| raw | raw.* | 400 days | `fin-dq retention apply --execute` (dry-run by default locally); S3 lifecycle rule |
| staged | staging.* | 90 days | same; S3 lifecycle rule |
| financial-record (curated) | curated.* | 7 years (2555 days), never auto-deleted | manual, with finance sign-off |
| operational (logs) | dq.rule_results, dq.anomaly_flags, dq.batch_run_log, governance.pii_access_log | 730 days | `retention apply` |

Every deletion writes `governance.retention_log` and, for dq/curated targets, `governance.audit_log`.

## 8. Audit and lineage

Every write to `raw`, `staging`, `curated` or `dq` records who, what, when and row counts in `governance.audit_log`.
Every report output records its source tables, batch ids and the exact rule versions active at run time in
`governance.lineage` (`fin-dq lineage show --report ar_aging --run-date ...`).

## 9. Change control for rules

Rules live in `config/dq_rules.yaml` and are versioned by a content hash in `dq.rule_registry`.

1. Open a pull request that edits `dq_rules.yaml` (id, type, description, dimension, severity, owner, policy section
   reference, params). New rule types need a class in `quality/rules.py` plus positive and negative tests.
2. CI runs the rule against the seeded fixture; the PR shows the resulting quarantine counts.
3. The **rule owner** listed in the file must approve. Severity changes to or from `blocking` also need
   finance-ops approval.
4. On merge the next run loads the new version into `dq.rule_registry`; prior versions are kept (`is_active = false`)
   so historical results remain interpretable.

Emergency disablement: set `severity: info` in a hotfix PR rather than deleting the rule, so results keep flowing.
