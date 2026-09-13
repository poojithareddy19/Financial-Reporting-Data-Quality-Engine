"""Helpers for dq.batch_run_log, the stage-level run ledger."""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

from sqlalchemy import Connection

from fin_dq_engine.db import execute, scalar


def log_stage(
    conn: Connection,
    *,
    batch_id: str,
    run_date: date,
    stage: str,
    status: str,
    rows_in: int | None = None,
    rows_out: int | None = None,
    source_file: str | None = None,
    checksum: str | None = None,
    details: dict[str, Any] | None = None,
    started_at: datetime | None = None,
) -> int:
    """Insert one ledger row and return its id. ``started_at`` lets stages record real durations."""
    return int(
        scalar(
            conn,
            """
            INSERT INTO dq.batch_run_log
                (batch_id, run_date, stage, status, started_at, finished_at, source_file, checksum, rows_in, rows_out, details)
            VALUES
                (:batch_id, :run_date, :stage, :status, COALESCE(:started_at, clock_timestamp()), clock_timestamp(),
                 :source_file, :checksum,
                 :rows_in, :rows_out, CAST(:details AS jsonb))
            RETURNING log_id
            """,
            {
                "batch_id": batch_id,
                "run_date": run_date,
                "stage": stage,
                "status": status,
                "source_file": source_file,
                "checksum": checksum,
                "rows_in": rows_in,
                "rows_out": rows_out,
                "details": json.dumps(details or {}, default=str),
                "started_at": started_at,
            },
        )
    )


def find_ingested_batch(conn: Connection, checksum: str) -> str | None:
    """Return the batch_id of a successful ingest with this checksum, if any."""
    val = scalar(
        conn,
        "SELECT batch_id FROM dq.batch_run_log WHERE stage = 'ingest' AND status = 'success' "
        "AND checksum = :c ORDER BY started_at DESC LIMIT 1",
        {"c": checksum},
    )
    return str(val) if val else None


def latest_batch_for_date(conn: Connection, run_date: date) -> str | None:
    """Most recent successfully ingested batch for a run date."""
    val = scalar(
        conn,
        "SELECT batch_id FROM dq.batch_run_log WHERE stage = 'ingest' AND status = 'success' "
        "AND run_date = :d ORDER BY started_at DESC LIMIT 1",
        {"d": run_date},
    )
    return str(val) if val else None


def clear_stage_rows(conn: Connection, batch_id: str, table: str, column: str = "batch_id") -> int:
    """Delete rows for a batch so a stage can be re-run idempotently."""
    return execute(conn, f"DELETE FROM {table} WHERE {column} = :b", {"b": batch_id})
