"""Audit logging for every write to the curated and dq schemas."""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Connection

from fin_dq_engine.db import execute


def record_write(
    conn: Connection,
    *,
    actor: str,
    action: str,
    target: str,
    row_count: int,
    batch_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> None:
    """Write one governance.audit_log row. ``target`` is ``schema.table``."""
    schema, table = target.split(".", 1)
    execute(
        conn,
        """
        INSERT INTO governance.audit_log (batch_id, actor, action, target_schema, target_table, row_count, details)
        VALUES (:batch_id, :actor, :action, :schema, :table, :row_count, CAST(:details AS jsonb))
        """,
        {
            "batch_id": batch_id,
            "actor": actor,
            "action": action,
            "schema": schema,
            "table": table,
            "row_count": row_count,
            "details": json.dumps(details or {}, default=str),
        },
    )
