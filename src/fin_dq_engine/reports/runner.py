"""Stage 5a: execute sql/reports/*.sql, mask PII, persist CSV + Parquet, record lineage."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Connection, Engine

from fin_dq_engine.config import Settings
from fin_dq_engine.db import query_df, transaction
from fin_dq_engine.governance.lineage import active_rule_versions, record_lineage
from fin_dq_engine.governance.pii import log_pii_access, mask_frame
from fin_dq_engine.logging_utils import get_logger
from fin_dq_engine.storage import Storage, get_storage, write_parquet

log = get_logger(__name__)

REPORT_ORDER = [
    "daily_revenue_by_entity",
    "ar_aging",
    "gl_trial_balance",
    "month_over_month_variance",
    "customer_concentration",
    "fx_exposure",
    "dq_scorecard",
    "late_arriving_data",
]


@dataclass
class ReportMeta:
    """Header metadata parsed from the SQL file."""

    name: str
    business_question: str = ""
    grain: str = ""
    owner: str = ""
    params: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)


@dataclass
class ReportResult:
    """One executed report."""

    meta: ReportMeta
    frame: pd.DataFrame
    csv_uri: str
    parquet_uri: str
    masked: bool


def parse_header(sql: str, name: str) -> ReportMeta:
    """Parse ``-- Key: value`` header lines (continuation lines are joined)."""
    meta = ReportMeta(name=name)
    fields: dict[str, str] = {}
    current: str | None = None
    for line in sql.splitlines():
        if not line.startswith("--"):
            break
        body = line[2:].strip()
        m = re.match(r"^([A-Za-z ]+):\s*(.*)$", body)
        if m and m.group(1).strip().lower() in {
            "report",
            "check",
            "business question",
            "purpose",
            "grain",
            "owner",
            "params",
            "sources",
            "notes",
        }:
            current = m.group(1).strip().lower()
            fields[current] = m.group(2).strip()
        elif current:
            fields[current] = f"{fields[current]} {body}"
    meta.business_question = fields.get("business question", fields.get("purpose", ""))
    meta.grain = fields.get("grain", "")
    meta.owner = fields.get("owner", "")
    meta.params = re.findall(r":(\w+)", fields.get("params", ""))
    meta.sources = [s.strip() for s in fields.get("sources", "").split(",") if s.strip()]
    return meta


def bind_params(sql: str, available: dict[str, Any]) -> dict[str, Any]:
    """Only bind the parameters the SQL actually references."""
    used = set(re.findall(r"(?<!:):(\w+)", sql))
    return {k: v for k, v in available.items() if k in used}


def run_report_sql(conn: Connection, path: Path, params: dict[str, Any]) -> tuple[ReportMeta, pd.DataFrame]:
    """Execute one report file and return its metadata and frame."""
    sql = path.read_text(encoding="utf-8")
    meta = parse_header(sql, path.stem)
    return meta, query_df(conn, sql, bind_params(sql, params))


def run_reports(
    settings: Settings,
    engine: Engine,
    batch_id: str,
    run_date: date,
    *,
    storage: Storage | None = None,
    names: list[str] | None = None,
    role: str | None = None,
) -> list[ReportResult]:
    """Run every report (or the named subset) for a run date, write outputs and lineage."""
    storage = storage or get_storage(settings)
    role = role or settings.role
    reports_dir = settings.paths.sql_dir / "reports"
    names = names or [p.stem for p in sorted(reports_dir.glob("*.sql"))]
    ordered = [n for n in REPORT_ORDER if n in names] + [n for n in names if n not in REPORT_ORDER]
    params = {
        "run_date": run_date,
        "batch_id": batch_id,
        "late_days": settings.pipeline.late_arrival_days,
    }
    results: list[ReportResult] = []
    with transaction(engine) as conn:
        versions = active_rule_versions(conn)
        for name in ordered:
            meta, df = run_report_sql(conn, reports_dir / f"{name}.sql", params)
            df, pii_cols, masked = mask_frame(df, settings.governance.pii_columns, role)
            log_pii_access(
                conn,
                actor=settings.actor,
                role=role,
                report_name=name,
                columns=pii_cols,
                masked=masked,
            )
            prefix = f"reports/{run_date.isoformat()}/{name}"
            csv_uri = storage.write_bytes("curated", f"{prefix}.csv", df.to_csv(index=False).encode())
            parquet_uri = write_parquet(storage, "curated", f"{prefix}.parquet", _parquet_safe(df))
            record_lineage(
                conn,
                report_name=name,
                run_date=run_date,
                batch_id=batch_id,
                source_tables=meta.sources or ["curated.fact_transactions"],
                source_batches=[batch_id],
                rule_versions=versions,
                output_path=csv_uri,
                row_count=len(df),
            )
            results.append(ReportResult(meta, df, csv_uri, parquet_uri, masked))
            log.info("report_written", report=name, rows=len(df), masked=masked)
    return results


def run_reconciliation(conn: Connection, sql_dir: Path, batch_id: str) -> dict[str, Any]:
    """Run sql/quality/reconciliation.sql for a batch and return the single result row."""
    sql = (sql_dir / "quality" / "reconciliation.sql").read_text(encoding="utf-8")
    df = query_df(conn, sql, {"batch_id": batch_id})
    return {str(k): (v.item() if hasattr(v, "item") else v) for k, v in df.iloc[0].to_dict().items()}


def _parquet_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce object columns holding Decimals/dates to types pyarrow handles uniformly."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            sample = out[col].dropna()
            if not sample.empty and type(sample.iloc[0]).__name__ == "Decimal":
                out[col] = pd.to_numeric(out[col], errors="coerce")
            elif not sample.empty and isinstance(sample.iloc[0], date):
                out[col] = pd.to_datetime(out[col], errors="coerce")
            elif not sample.empty and isinstance(sample.iloc[0], list | dict):
                out[col] = out[col].astype(str)
    return out
