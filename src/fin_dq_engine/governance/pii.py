"""PII masking for report outputs and access logging."""

from __future__ import annotations

import hashlib
import hmac
import secrets

import pandas as pd
from sqlalchemy import Connection

from fin_dq_engine.db import execute
from fin_dq_engine.logging_utils import get_logger

log = get_logger(__name__)

UNMASKED_ROLES = {"finance_admin"}

# Fallback when no key is configured. Deliberately per-process: masked values then differ between
# runs, which is safer than handing out a stable pseudonym, and visible enough to get noticed.
_PROCESS_KEY = secrets.token_bytes(32)


def resolve_key(configured: str) -> bytes:
    """Key for the masking HMAC, falling back to a per-process random key with a warning."""
    if configured:
        return configured.encode("utf-8")
    log.warning("pii_hash_key_not_set", detail="masked values will not be stable across runs")
    return _PROCESS_KEY


def mask_value(value: object, column: str, key: bytes | None = None) -> object:
    """Mask one value: emails keep the first character and domain, names keep two characters plus a digest.

    The digest is an HMAC, not a plain hash. An unkeyed ``sha256`` of a customer name is recoverable by
    anyone holding a candidate list: hash each candidate and match. The key removes that, and makes the
    pseudonym unlinkable to anyone who does not hold it.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    s = str(value)
    digest = hmac.new(key or _PROCESS_KEY, s.encode("utf-8"), hashlib.sha256).hexdigest()[:6]
    if "email" in column and "@" in s:
        local, domain = s.split("@", 1)
        return f"{local[:1]}***@{domain}"
    return f"{s[:2]}***{digest}"


def mask_frame(
    df: pd.DataFrame, pii_columns: list[str], role: str, key: bytes | None = None
) -> tuple[pd.DataFrame, list[str], bool]:
    """Return (frame, pii columns present, masked?). Unmasked only for roles in UNMASKED_ROLES."""
    present = [c for c in pii_columns if c in df.columns]
    if not present:
        return df, [], False
    if role in UNMASKED_ROLES:
        return df, present, False
    out = df.copy()
    for col in present:
        out[col] = out[col].map(lambda v, c=col: mask_value(v, c, key))
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
