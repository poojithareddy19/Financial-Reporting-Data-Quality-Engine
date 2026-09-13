"""Lineage records for report outputs."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import Connection

from fin_dq_engine.db import execute, jsonb, query_df


def active_rule_versions(conn: Connection) -> dict[str, str]:
    """Current rule_id -> version mapping from the registry."""
    df = query_df(conn, "SELECT rule_id, rule_version FROM dq.rule_registry WHERE is_active ORDER BY rule_id")
    return dict(zip(df["rule_id"], df["rule_version"], strict=True))


def record_lineage(
    conn: Connection,
    *,
    report_name: str,
    run_date: object,
    batch_id: str,
    source_tables: list[str],
    source_batches: list[str],
    rule_versions: dict[str, str],
    output_path: str,
    row_count: int,
) -> None:
    """Insert one governance.lineage row."""
    execute(
        conn,
        """
        INSERT INTO governance.lineage (report_name, run_date, batch_id, source_tables, source_batches, rule_versions,
                                        output_path, row_count)
        VALUES (:report, :run_date, :batch_id, :tables, :batches, :versions, :path, :rows)""",
        {
            "report": report_name,
            "run_date": run_date,
            "batch_id": batch_id,
            "tables": source_tables,
            "batches": source_batches,
            "versions": jsonb(rule_versions),
            "path": output_path,
            "rows": row_count,
        },
    )


def show_lineage(conn: Connection, report_name: str, run_date: object) -> pd.DataFrame:
    """Lineage rows for a report and run date (backs ``lineage show``)."""
    return query_df(
        conn,
        """
        SELECT report_name, run_date, batch_id, source_tables, source_batches, rule_versions, output_path, row_count, created_at
        FROM governance.lineage WHERE report_name = :r AND run_date = :d ORDER BY created_at DESC""",
        {"r": report_name, "d": run_date},
    )
