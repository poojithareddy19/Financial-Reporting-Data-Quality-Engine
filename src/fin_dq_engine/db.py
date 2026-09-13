"""Database access: SQLAlchemy Core engine, SQL file execution and versioned DDL migrations."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import Connection, Engine, create_engine, text
from sqlalchemy.exc import DBAPIError

from fin_dq_engine.config import Settings
from fin_dq_engine.logging_utils import get_logger

log = get_logger(__name__)


def get_engine(settings: Settings) -> Engine:
    """Create a SQLAlchemy engine for the warehouse."""
    return create_engine(settings.database.url, echo=settings.database.echo, future=True)


@contextmanager
def transaction(engine: Engine) -> Iterator[Connection]:
    """Open a connection with an explicit transaction that commits on success, rolls back on error."""
    with engine.begin() as conn:
        yield conn


def read_sql_file(path: Path) -> str:
    """Read a SQL file from disk."""
    return path.read_text(encoding="utf-8")


def _split_statements(sql: str) -> list[str]:
    """Split a DDL file into statements on semicolons that end a line (no $$ bodies used here).

    Full-line comments are stripped so a statement preceded by a header comment still executes.
    """
    stripped = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    parts = re.split(r";\s*(?:\n|$)", stripped)
    return [p.strip() for p in parts if p.strip()]


def execute_sql_file(conn: Connection, path: Path, params: Mapping[str, Any] | None = None) -> None:
    """Execute every statement in a SQL file within the given connection."""
    for stmt in _split_statements(read_sql_file(path)):
        conn.execute(text(stmt), dict(params or {}))


def apply_migrations(engine: Engine, ddl_dir: Path) -> list[str]:
    """Apply sql/ddl/NNN_*.sql files in order, recording each in meta.schema_version.

    Returns the list of versions applied in this call. Files are idempotent (IF NOT EXISTS), so a
    changed checksum is applied again and the recorded checksum updated.
    """
    files = sorted(p for p in ddl_dir.glob("*.sql") if re.match(r"^\d{3}_", p.name))
    applied: list[str] = []
    with transaction(engine) as conn:
        # 001 creates meta.schema_version; run it first unconditionally.
        execute_sql_file(conn, files[0])
        existing = {
            row.version: row.checksum for row in conn.execute(text("SELECT version, checksum FROM meta.schema_version"))
        }
        for path in files:
            version = path.stem
            checksum = hashlib.sha256(path.read_bytes()).hexdigest()
            if existing.get(version) == checksum:
                continue
            execute_sql_file(conn, path)
            conn.execute(
                text(
                    "INSERT INTO meta.schema_version (version, checksum) VALUES (:v, :c) "
                    "ON CONFLICT (version) DO UPDATE SET checksum = EXCLUDED.checksum, applied_at = now()"
                ),
                {"v": version, "c": checksum},
            )
            applied.append(version)
    log.info("migrations_applied", versions=applied)
    return applied


def query_df(conn: Connection, sql: str, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
    """Run a parameterised query and return a DataFrame."""
    return pd.read_sql_query(text(sql), conn, params=dict(params or {}))


def scalar(conn: Connection, sql: str, params: Mapping[str, Any] | None = None) -> Any:
    """Run a query and return the first column of the first row."""
    return conn.execute(text(sql), dict(params or {})).scalar()


def execute(conn: Connection, sql: str, params: Mapping[str, Any] | None = None) -> int:
    """Execute a statement and return the affected row count (-1 if unknown)."""
    result = conn.execute(text(sql), dict(params or {}))
    return result.rowcount if result.rowcount is not None else -1


def insert_rows(conn: Connection, table: str, rows: Sequence[Mapping[str, Any]]) -> int:
    """Bulk insert dict rows into ``schema.table`` with bound parameters."""
    if not rows:
        return 0
    cols = list(rows[0].keys())
    col_sql = ", ".join(cols)
    val_sql = ", ".join(f":{c}" for c in cols)
    conn.execute(text(f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql})"), list(rows))
    return len(rows)


def copy_df(conn: Connection, table: str, df: pd.DataFrame) -> int:
    """Fast-path bulk load of a DataFrame using psycopg COPY; falls back to executemany."""
    if df.empty:
        return 0
    cols = list(df.columns)
    try:
        raw: Any = conn.connection.driver_connection  # psycopg3 connection
        with raw.cursor() as cur, cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as copy:
            for row in df.itertuples(index=False, name=None):
                copy.write_row([None if _is_null(v) else v for v in row])
    except (AttributeError, DBAPIError):  # pragma: no cover - exercised only on non-psycopg drivers
        insert_rows(conn, table, records(df))
    return len(df)


def records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """DataFrame rows as str-keyed dicts (typed for bound-parameter helpers)."""
    return [{str(k): v for k, v in r.items()} for r in df.to_dict(orient="records")]


def _is_null(value: Any) -> bool:
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def jsonb(value: Any) -> Any:
    """Wrap a Python object so psycopg binds it as jsonb."""
    from psycopg.types.json import Jsonb

    return Jsonb(value, dumps=lambda v: json.dumps(v, default=str))
