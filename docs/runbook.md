# Runbook

All commands assume local mode (`--local`, the default). In AWS, run the same CLI from a bastion or the Lambda
container image with `--aws`, or start the Step Functions state machine with `{"run_date": "YYYY-MM-DD"}`.

## Pipeline aborted (blocking failure rate above threshold)

Symptoms: SNS email "[fin-dq] CRITICAL: ... Pipeline aborted", Step Functions execution failed at `ValidateTask`,
`dq.batch_run_log` row with `stage = 'validate', status = 'aborted'`.

1. Identify the batch and reasons:
   ```sql
   SELECT batch_id, details->'scorecard'->>'quarantine_reasons', details->'scorecard'->>'blocking_failure_rate'
   FROM dq.batch_run_log WHERE stage = 'validate' AND status = 'aborted' ORDER BY started_at DESC LIMIT 5;
   ```
2. Inspect the failing samples for each rule:
   ```bash
   fin-dq dq explain --rule-id DQ-010 --run-date 2025-03-05
   ```
3. Decide:
   - **Source defect** (e.g. a new GL account not yet in the chart of accounts): fix the reference data or the source
     file. A corrected file has a new checksum, so `fin-dq run-daily --run-date ...` ingests it as a new batch.
   - **Rule too strict**: follow the change-control process in `docs/data_governance_policy.md#9-change-control-for-rules`.
     Do not lower `pipeline.batch_failure_threshold_pct` in production without finance-ops approval.
   - **Known, accepted issue for this batch only**: re-run with a temporary threshold override for one date:
     `FIN_DQ__PIPELINE__BATCH_FAILURE_THRESHOLD_PCT=20 fin-dq run-daily --run-date ...`. The blocking rows are still
     quarantined; only the abort is suppressed. Record the decision in the incident ticket.
4. After a successful re-run confirm `reconciled = true` in the summary's technical appendix.

## Contract violation

Symptoms: `SchemaDriftError` from the ingest stage, structured log line `contract_violation`, a `breaking`
row in `governance.contract_events`. **Nothing was landed** and curated is untouched, so there is no
cleanup to do and no urgency to re-run before the producer has answered.

1. See the field and the accountable owner without touching the database:
   ```bash
   fin-dq contracts check --run-date 2025-03-05
   ```
   Exit code 2 means breaking. `missing` names fields the file no longer carries, `unknown` names fields
   it carries that the contract does not know about. A rename shows up in both lists at once.
2. Confirm against the durable record:
   ```sql
   SELECT feed, verdict, missing_fields, unknown_fields, owner, observed_at
   FROM governance.contract_events WHERE verdict = 'breaking' ORDER BY observed_at DESC LIMIT 10;
   ```
3. Raise a producer incident against the `owner` on the event row. **Never hand-edit the source file**:
   that hides a producer-side change as a data-quality blip and the same break returns tomorrow.
4. Once the producer confirms the change is intentional, update `contracts/<feed>.avsc` under the
   change-control process in `docs/data_governance_policy.md#9-change-control-for-rules`. The contract
   version is a fingerprint of the field structure, so the new version registers itself on the next run
   and the old one stays in `governance.contract_registry` for audit.
5. Re-run the date normally: `fin-dq run-daily --run-date 2025-03-05`.

An `additive` verdict is not an incident. The file carried a field the contract does not list, the batch
loaded normally, and the field is recorded in `governance.contract_events` for follow-up at your pace:

```sql
SELECT feed, unknown_fields, run_date FROM governance.contract_events
WHERE verdict = 'additive' ORDER BY observed_at DESC;
```

## A stage failed (exception, not a DQ abort)

1. Read the structured log line `stage_failed` (CloudWatch Logs `/aws/lambda/fin-dq-<stage>` or the terminal).
2. Common causes: Postgres unreachable (check `docker compose ps` / RDS status and the Secrets Manager secret),
   missing source file (`SourceFileMissingError`: the date has no drop yet, re-run later), S3 permissions.
3. Every stage is idempotent, so re-run only the failed stage and the ones after it:
   ```bash
   fin-dq load   --run-date 2025-03-05
   fin-dq report --run-date 2025-03-05
   fin-dq notify --run-date 2025-03-05
   ```
   `--batch-id` selects a specific batch when a date has more than one.

## Release quarantined rows

Quarantined rows are never loaded automatically. Releasing marks them as reviewed; it does not load them, because a
blocking failure means the row cannot be represented correctly in curated.

1. Export for the owner: use the dashboard's Quarantine explorer (filters by rule, entity, status, CSV download) or
   ```sql
   SELECT transaction_id, rule_ids, row_data FROM dq.quarantine WHERE batch_id = '...' AND released_at IS NULL;
   ```
2. If the source corrects the rows, they arrive in a later file and load normally. Then close the quarantine:
   ```bash
   fin-dq dq release --batch-id 2025-03-05-a7110730            # all rows of the batch
   fin-dq dq release --batch-id 2025-03-05-a7110730 --rule-id DQ-010
   ```
   The release is written to `governance.audit_log` with the actor from settings (`actor:` or `FIN_DQ__ACTOR`).
3. If the rows must be loaded as-is (business decision), fix the reference data that made them fail (e.g. add the
   account to `dim_account`) and re-run `fin-dq run-daily --run-date ... --force`. Never edit `curated.*` by hand.

## Backfill a date range

```bash
fin-dq run-daily --start 2025-01-01 --end 2025-01-31 --no-notify
```

- Dates without a source file are reported as `skipped`, others run in order (later dates depend on dim_fx_rate and
  dim_customer changes from earlier dates).
- Re-running a range that already loaded is safe: ingest is skipped on checksum and load upserts.
- For a large backfill in AWS, start one Step Functions execution per date with `{"run_date": "..."}`; the state
  machine has a 2 hour timeout per run.
- Reports and summaries are regenerated for every date in the range; only the last date's summary is normally sent.

## Rule registry drift

`dq.rule_registry` is synced from `config/dq_rules.yaml` at the start of every validate. If a rule disappears from the
file it is marked `is_active = false`, never deleted. To see what changed:

```sql
SELECT rule_id, rule_version, is_active, loaded_at FROM dq.rule_registry ORDER BY rule_id, loaded_at;
```

## Retention

```bash
fin-dq retention apply                      # dry run: prints what would be deleted
fin-dq retention apply --execute            # deletes and writes governance.retention_log
```

Local mode always dry-runs unless `--execute` is given. Curated data is never deleted by this command.

## Useful queries

```sql
-- stage timings for the last run
SELECT stage, status, rows_in, rows_out, finished_at - started_at AS took
FROM dq.batch_run_log WHERE batch_id = (SELECT batch_id FROM dq.batch_run_log ORDER BY log_id DESC LIMIT 1) ORDER BY log_id;

-- pass rate trend
SELECT run_date, (details->'scorecard'->>'dq_pass_rate')::numeric FROM dq.batch_run_log
WHERE stage = 'validate' AND status = 'success' ORDER BY run_date DESC LIMIT 30;

-- who looked at unmasked PII
SELECT * FROM governance.pii_access_log WHERE NOT masked ORDER BY accessed_at DESC;
```
