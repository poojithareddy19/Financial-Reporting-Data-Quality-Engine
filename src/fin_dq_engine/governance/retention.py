"""Retention: delete raw partitions and operational logs older than their class limit. Dry-run by default locally."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy import Engine

from fin_dq_engine.config import Settings
from fin_dq_engine.db import execute, scalar, transaction
from fin_dq_engine.governance.audit import record_write
from fin_dq_engine.logging_utils import get_logger

log = get_logger(__name__)

# retention class -> (table, date expression used for the cutoff)
RETENTION_TARGETS: dict[str, list[tuple[str, str]]] = {
    "raw": [
        ("raw.transactions", "_ingested_at::date"),
        ("raw.fx_rates", "_ingested_at::date"),
        ("raw.customers", "_ingested_at::date"),
    ],
    "staged": [("staging.transactions", "ingested_at::date")],
    "logs": [
        ("dq.rule_results", "run_date"),
        ("dq.anomaly_flags", "run_date"),
        ("dq.batch_run_log", "run_date"),
        ("governance.pii_access_log", "accessed_at::date"),
    ],
}


@dataclass(frozen=True)
class RetentionAction:
    """One table evaluated by retention."""

    retention_class: str
    table: str
    cutoff: date
    rows: int
    dry_run: bool


def apply_retention(
    settings: Settings, engine: Engine, *, as_of: date | None = None, dry_run: bool | None = None
) -> list[RetentionAction]:
    """Delete (or count, when dry-run) rows older than each class limit and log every action."""
    as_of = as_of or date.today()
    dry_run = settings.is_local if dry_run is None else dry_run
    actions: list[RetentionAction] = []
    with transaction(engine) as conn:
        for cls, targets in RETENTION_TARGETS.items():
            days = settings.governance.retention_days.get(cls)
            if days is None:
                continue
            cutoff = as_of - timedelta(days=days)
            for table, date_expr in targets:
                count = int(
                    scalar(conn, f"SELECT count(*) FROM {table} WHERE {date_expr} < :cutoff", {"cutoff": cutoff}) or 0
                )
                if not dry_run and count:
                    execute(conn, f"DELETE FROM {table} WHERE {date_expr} < :cutoff", {"cutoff": cutoff})
                    schema, _ = table.split(".", 1)
                    if schema in ("curated", "dq"):
                        record_write(
                            conn,
                            actor=settings.actor,
                            action="delete",
                            target=table,
                            row_count=count,
                            details={"retention_class": cls, "cutoff": cutoff},
                        )
                execute(
                    conn,
                    """
                    INSERT INTO governance.retention_log (retention_class, target_table, cutoff_date, rows_deleted, dry_run, actor)
                    VALUES (:cls, :table, :cutoff, :rows, :dry, :actor)""",
                    {
                        "cls": cls,
                        "table": table,
                        "cutoff": cutoff,
                        "rows": count,
                        "dry": dry_run,
                        "actor": settings.actor,
                    },
                )
                actions.append(RetentionAction(cls, table, cutoff, count, dry_run))
    log.info("retention_applied", dry_run=dry_run, actions=len(actions))
    return actions


def release_quarantine(settings: Settings, engine: Engine, batch_id: str, rule_id: str | None = None) -> int:
    """Mark quarantined rows as released (audited). The runbook covers when this is appropriate."""
    with transaction(engine) as conn:
        clause = " AND :rule = ANY(rule_ids)" if rule_id else ""
        n = execute(
            conn,
            f"UPDATE dq.quarantine SET released_at = now(), released_by = :actor "
            f"WHERE batch_id = :b AND released_at IS NULL{clause}",
            {"actor": settings.actor, "b": batch_id, "rule": rule_id},
        )
        record_write(
            conn,
            actor=settings.actor,
            action="release",
            target="dq.quarantine",
            row_count=n,
            batch_id=batch_id,
            details={"rule_id": rule_id},
        )
    return n
