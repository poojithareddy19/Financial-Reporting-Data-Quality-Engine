"""Stage 3: the data quality engine. Loads the rule registry, runs every rule, quarantines rows failing
blocking rules, scores the rest, runs anomaly detection and aborts the batch above the failure threshold."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import Connection, Engine, text

from fin_dq_engine.batchlog import clear_stage_rows, log_stage
from fin_dq_engine.config import Settings
from fin_dq_engine.db import execute, insert_rows, jsonb, query_df, scalar, transaction
from fin_dq_engine.governance.audit import record_write
from fin_dq_engine.logging_utils import get_logger
from fin_dq_engine.quality.anomaly import AnomalyFlag, run_anomaly_detection
from fin_dq_engine.quality.rules import (
    RuleContext,
    RuleDefinition,
    RuleOutcome,
    build_rule,
    load_rule_definitions,
)
from fin_dq_engine.quality.scoring import Scorecard, build_scorecard, compute_dq_score

log = get_logger(__name__)


class BatchAbortedError(RuntimeError):
    """Raised when the blocking failure rate exceeds the configured threshold."""

    def __init__(self, scorecard: Scorecard, threshold_pct: float) -> None:
        self.scorecard = scorecard
        self.threshold_pct = threshold_pct
        super().__init__(
            f"Batch {scorecard.batch_id} aborted: blocking failure rate "
            f"{scorecard.blocking_failure_rate * 100:.2f}% exceeds threshold {threshold_pct}%"
        )


@dataclass
class ValidationResult:
    """Outcome of :func:`validate_batch`."""

    batch_id: str
    outcomes: list[RuleOutcome]
    scorecard: Scorecard
    anomalies: list[AnomalyFlag]
    aborted: bool = False


def sync_rule_registry(conn: Connection, definitions: list[RuleDefinition]) -> int:
    """Upsert every rule version into dq.rule_registry and deactivate superseded versions."""
    for d in definitions:
        execute(
            conn,
            """
            INSERT INTO dq.rule_registry (rule_id, rule_version, rule_type, description, dimension, severity,
                                          owner, policy_section, params, is_active)
            VALUES (:rule_id, :rule_version, :rule_type, :description, :dimension, :severity, :owner,
                    :policy_section, :params, TRUE)
            ON CONFLICT (rule_id, rule_version) DO UPDATE SET is_active = TRUE, loaded_at = now()
        """,
            {
                "rule_id": d.id,
                "rule_version": d.version,
                "rule_type": d.type,
                "description": d.description,
                "dimension": d.dimension,
                "severity": d.severity,
                "owner": d.owner,
                "policy_section": d.policy_section,
                "params": jsonb(d.params),
            },
        )
        execute(
            conn,
            "UPDATE dq.rule_registry SET is_active = FALSE WHERE rule_id = :r AND rule_version <> :v",
            {"r": d.id, "v": d.version},
        )
    active_ids = [d.id for d in definitions]
    if active_ids:
        placeholders = ", ".join(f":i{n}" for n in range(len(active_ids)))
        execute(
            conn,
            f"UPDATE dq.rule_registry SET is_active = FALSE WHERE rule_id NOT IN ({placeholders})",
            {f"i{n}": v for n, v in enumerate(active_ids)},
        )
    return len(definitions)


def _persist_outcomes(conn: Connection, ctx: RuleContext, outcomes: list[RuleOutcome]) -> None:
    insert_rows(
        conn,
        "dq.rule_results",
        [
            {
                "batch_id": ctx.batch_id,
                "run_date": ctx.run_date,
                "rule_id": o.definition.id,
                "rule_version": o.definition.version,
                "dimension": o.definition.dimension,
                "severity": o.definition.severity,
                "status": o.status,
                "rows_checked": o.rows_checked,
                "rows_failed": o.rows_failed,
                "failure_rate": round(o.failure_rate, 6),
                "sample_keys": jsonb(o.sample_keys),
                "duration_ms": o.duration_ms,
            }
            for o in outcomes
        ],
    )


def _apply_scores(
    conn: Connection, ctx: RuleContext, outcomes: list[RuleOutcome], settings: Settings, actor: str
) -> tuple[set[int], dict[int, int]]:
    """Quarantine rows failing blocking rules; set dq_score on the remainder."""
    failed_by_row: dict[int, list[RuleOutcome]] = {}
    for o in outcomes:
        for key in o.failing_keys:
            failed_by_row.setdefault(key, []).append(o)
    quarantined = {k for k, outs in failed_by_row.items() if any(o.definition.severity == "blocking" for o in outs)}
    scores: dict[int, int] = {}
    for k, outs in failed_by_row.items():
        if k not in quarantined:
            scores[k] = compute_dq_score(
                [o.definition.severity for o in outs],
                settings.pipeline.warning_penalty,
                settings.pipeline.info_penalty,
            )

    execute(
        conn,
        "UPDATE staging.transactions SET dq_score = 100, quarantined = FALSE WHERE batch_id = :b",
        {"b": ctx.batch_id},
    )
    if scores:
        conn.execute(
            text("UPDATE staging.transactions SET dq_score = :score WHERE staging_row_id = :rid"),
            [{"score": s, "rid": rid} for rid, s in scores.items()],
        )
    if quarantined:
        rows = query_df(
            conn,
            "SELECT * FROM staging.transactions WHERE batch_id = :b AND staging_row_id = ANY(:ids)",
            {"b": ctx.batch_id, "ids": list(quarantined)},
        )
        q_rows = []
        for rec in rows.to_dict(orient="records"):
            rid = int(rec["staging_row_id"])
            rule_ids = sorted(o.definition.id for o in failed_by_row[rid])
            payload = {
                k: (None if _isna(v) else v)
                for k, v in rec.items()
                if k not in ("staging_row_id", "dq_score", "quarantined")
            }
            q_rows.append(
                {
                    "batch_id": ctx.batch_id,
                    "run_date": ctx.run_date,
                    "transaction_id": rec["transaction_id"] if not _isna(rec["transaction_id"]) else None,
                    "rule_ids": rule_ids,
                    "severity": "blocking",
                    "row_data": jsonb(payload),
                }
            )
        insert_rows(conn, "dq.quarantine", q_rows)
        execute(
            conn,
            "UPDATE staging.transactions SET quarantined = TRUE, dq_score = 0 "
            "WHERE batch_id = :b AND staging_row_id = ANY(:ids)",
            {"b": ctx.batch_id, "ids": list(quarantined)},
        )
        record_write(
            conn,
            actor=actor,
            action="insert",
            target="dq.quarantine",
            row_count=len(q_rows),
            batch_id=ctx.batch_id,
        )
    return quarantined, scores


def _isna(v: object) -> bool:
    try:
        return bool(pd.isna(v))  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return False


def validate_batch(
    settings: Settings,
    engine: Engine,
    batch_id: str,
    run_date: date,
    rules_path: Path | None = None,
) -> ValidationResult:
    """Run the DQ engine for a batch. Raises :class:`BatchAbortedError` above the failure threshold.

    Re-runnable: prior rule_results, quarantine and anomaly rows for the batch are replaced.
    """
    started_at = datetime.now(UTC)
    rules_path = rules_path or settings.paths.config_dir / "dq_rules.yaml"
    definitions = load_rule_definitions(rules_path)
    ctx = RuleContext(batch_id=batch_id, run_date=run_date)
    with transaction(engine) as conn:
        sync_rule_registry(conn, definitions)
        for table in ("dq.rule_results", "dq.quarantine", "dq.anomaly_flags"):
            clear_stage_rows(conn, batch_id, table)
        rows_checked = int(
            scalar(
                conn,
                "SELECT count(*) FROM staging.transactions WHERE batch_id = :b",
                {"b": batch_id},
            )
            or 0
        )
        outcomes = [build_rule(d).evaluate(conn, ctx) for d in definitions]
        _persist_outcomes(conn, ctx, outcomes)
        quarantined, scores = _apply_scores(conn, ctx, outcomes, settings, settings.actor)
        # Rows that failed nothing keep 100; include them in the mean.
        all_scores = dict.fromkeys(range(rows_checked - len(quarantined) - len(scores)), 100) | scores
        scorecard = build_scorecard(batch_id, rows_checked, outcomes, quarantined, all_scores)
        record_write(
            conn,
            actor=settings.actor,
            action="insert",
            target="dq.rule_results",
            row_count=len(outcomes),
            batch_id=batch_id,
        )

        threshold = settings.pipeline.batch_failure_threshold_pct
        if scorecard.blocking_failure_rate * 100 > threshold:
            log_stage(
                conn,
                batch_id=batch_id,
                run_date=run_date,
                stage="validate",
                status="aborted",
                started_at=started_at,
                rows_in=rows_checked,
                rows_out=rows_checked - len(quarantined),
                details={"scorecard": scorecard.__dict__, "threshold_pct": threshold},
            )
            log.error("batch_aborted", batch_id=batch_id, failure_rate=scorecard.blocking_failure_rate)
            err = BatchAbortedError(scorecard, threshold)
        else:
            err = None
            anomalies = run_anomaly_detection(conn, settings.anomaly, batch_id, run_date)
            log_stage(
                conn,
                batch_id=batch_id,
                run_date=run_date,
                stage="validate",
                status="success",
                started_at=started_at,
                rows_in=rows_checked,
                rows_out=rows_checked - len(quarantined),
                details={"scorecard": scorecard.__dict__, "anomalies": len(anomalies)},
            )
    if err:
        raise err
    log.info(
        "validate_finished",
        batch_id=batch_id,
        quarantined=len(quarantined),
        dq_pass_rate=round(scorecard.dq_pass_rate, 4),
        anomalies=len(anomalies),
        rules_failed=scorecard.rules_failed,
    )
    return ValidationResult(batch_id, outcomes, scorecard, anomalies)


def explain_rule(engine: Engine, rule_id: str, run_date: date, limit: int = 20) -> pd.DataFrame:
    """Return failing sample rows for a rule on a run date (backs ``dq explain``)."""
    with engine.connect() as conn:
        res = query_df(
            conn,
            """
            SELECT r.batch_id, r.rule_id, r.status, r.rows_checked, r.rows_failed, r.sample_keys, g.description, g.severity
            FROM dq.rule_results r
            JOIN dq.rule_registry g ON g.rule_id = r.rule_id AND g.rule_version = r.rule_version
            WHERE r.rule_id = :r AND r.run_date = :d ORDER BY r.executed_at DESC LIMIT 1""",
            {"r": rule_id, "d": run_date},
        )
        if res.empty:
            return res
        keys = [k for k in res.iloc[0]["sample_keys"] if k != "<null>"][:limit]
        if not keys:
            return res
        sample = query_df(
            conn,
            """
            SELECT transaction_id, posted_date, entity_id, account_id, customer_id, currency, amount_local,
                   fx_rate, dq_score, quarantined
            FROM staging.transactions WHERE batch_id = :b AND transaction_id = ANY(:keys)""",
            {"b": res.iloc[0]["batch_id"], "keys": keys},
        )
        sample.attrs["rule"] = res.iloc[0].to_dict()
        return sample
