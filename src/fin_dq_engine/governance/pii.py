"""PII masking for report outputs and access logging."""

from __future__ import annotations

import hashlib

import pandas as pd
from sqlalchemy import Connection

from fin_dq_engine.db import execute

UNMASKED_ROLES = {"finance_admin"}


def mask_value(value: object, column: str) -> object:
    """Mask one value: emails keep the first character and domain, names keep two characters plus a hash."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    s = str(value)
    digest = hashlib.sha256(s.encode()).hexdigest()[:6]
    if "email" in column and "@" in s:
        local, domain = s.split("@", 1)
        return f"{local[:1]}***@{domain}"
    return f"{s[:2]}***{digest}"


def mask_frame(df: pd.DataFrame, pii_columns: list[str], role: str) -> tuple[pd.DataFrame, list[str], bool]:
    """Return (frame, pii columns present, masked?). Unmasked only for roles in UNMASKED_ROLES."""
    present = [c for c in pii_columns if c in df.columns]
    if not present:
        return df, [], False
    if role in UNMASKED_ROLES:
        return df, present, False
    out = df.copy()
    for col in present:
        out[col] = out[col].map(lambda v, c=col: mask_value(v, c))
    return out, present, True


def log_pii_access(
    conn: Connection, *, actor: str, role: str, report_name: str, columns: list[str], masked: bool
) -> None:
    """Record a PII access. Called for every report containing PII columns; unmasked access is the audit target."""
    if not columns:
        return
    execute(
        conn,
        """
        INSERT INTO governance.pii_access_log (actor, role, report_name, columns, masked)
        VALUES (:actor, :role, :report, :columns, :masked)""",
        {"actor": actor, "role": role, "report": report_name, "columns": columns, "masked": masked},
    )
