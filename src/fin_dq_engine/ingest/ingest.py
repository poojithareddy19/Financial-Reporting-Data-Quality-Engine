"""Stage 1: land raw files for a run date into the raw schema.

Idempotency: a checksum over all source files for the date identifies the batch. If a successful
ingest with the same checksum exists, the stage is skipped and the existing batch_id returned.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy import Engine

from fin_dq_engine.batchlog import clear_stage_rows, find_ingested_batch, log_stage
from fin_dq_engine.config import Settings
from fin_dq_engine.db import copy_df, transaction
from fin_dq_engine.governance.audit import record_write
from fin_dq_engine.logging_utils import get_logger
from fin_dq_engine.storage import Storage, get_storage, read_csv

log = get_logger(__name__)

TRANSACTION_COLUMNS = [
    "transaction_id",
    "event_id",
    "event_type",
    "reference_id",
    "posted_date",
    "created_at",
    "entity_id",
    "account_id",
    "customer_id",
    "currency",
    "amount_local",
    "description",
    "source_system",
]
FX_COLUMNS = ["currency", "rate_date", "rate_to_usd", "source"]
CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "customer_email",
    "segment",
    "region",
    "country",
    "customer_since",
    "effective_date",
]


@dataclass(frozen=True)
class IngestResult:
    """Outcome of :func:`ingest_date`."""

    batch_id: str
    run_date: date
    skipped: bool
    rows_transactions: int
    rows_fx: int
    rows_customers: int
    checksum: str
    source_file: str


class SourceFileMissingError(FileNotFoundError):
    """Raised when the transactions file for the run date does not exist."""


def source_keys(run_date: date) -> dict[str, str]:
    """Raw-layer keys for a run date."""
    d = run_date.isoformat()
    return {
        "transactions": f"transactions/{d}.csv",
        "fx_rates": f"fx_rates/{d}.csv",
        "customers": f"customers/{d}.csv",
    }


def compute_checksum(blobs: list[bytes]) -> str:
    """sha256 over the concatenation of all source blobs (empty blob for absent optional files)."""
    h = hashlib.sha256()
    for b in blobs:
        h.update(hashlib.sha256(b).digest())
    return h.hexdigest()


def make_batch_id(run_date: date, checksum: str) -> str:
    """Deterministic batch id: ``<run_date>-<checksum prefix>``."""
    return f"{run_date.isoformat()}-{checksum[:8]}"


def _with_meta(df: pd.DataFrame, columns: list[str], source_file: str, batch_id: str, now: datetime) -> pd.DataFrame:
    out = df.reindex(columns=columns)
    out = out.replace({"": None})
    out["_ingested_at"] = now
    out["_source_file"] = source_file
    out["_batch_id"] = batch_id
    return out


def ingest_date(
    settings: Settings,
    engine: Engine,
    run_date: date,
    *,
    force: bool = False,
    storage: Storage | None = None,
) -> IngestResult:
    """Land the files for ``run_date`` into ``raw.*`` and write a batch_run_log row.

    Args:
        settings: Loaded settings.
        engine: Warehouse engine.
        run_date: Ingestion date whose files to pull.
        force: Re-ingest even if a batch with the same checksum already succeeded.
        storage: Optional storage override (tests inject moto-backed S3).
    """
    started_at = datetime.now(UTC)
    storage = storage or get_storage(settings)
    keys = source_keys(run_date)
    if not storage.exists("raw", keys["transactions"]):
        raise SourceFileMissingError(f"No transactions file for {run_date}: {storage.uri('raw', keys['transactions'])}")

    blobs = {
        name: (storage.read_bytes("raw", key) if storage.exists("raw", key) else b"") for name, key in keys.items()
    }
    checksum = compute_checksum([blobs["transactions"], blobs["fx_rates"], blobs["customers"]])
    batch_id = make_batch_id(run_date, checksum)
    source_file = storage.uri("raw", keys["transactions"])

    with transaction(engine) as conn:
        existing = find_ingested_batch(conn, checksum)
        if existing and not force:
            log.info(
                "ingest_skipped",
                batch_id=existing,
                run_date=str(run_date),
                reason="checksum already processed",
            )
            log_stage(
                conn,
                batch_id=existing,
                run_date=run_date,
                stage="ingest",
                status="skipped",
                started_at=started_at,
                checksum=checksum,
                source_file=source_file,
            )
            return IngestResult(existing, run_date, True, 0, 0, 0, checksum, source_file)

        now = datetime.now(UTC)
        for table in ("raw.transactions", "raw.fx_rates", "raw.customers"):
            clear_stage_rows(conn, batch_id, table, "_batch_id")

        tx = read_csv(storage, "raw", keys["transactions"])
        n_tx = copy_df(
            conn,
            "raw.transactions",
            _with_meta(tx, TRANSACTION_COLUMNS, source_file, batch_id, now),
        )
        n_fx = n_cust = 0
        if blobs["fx_rates"]:
            fx = read_csv(storage, "raw", keys["fx_rates"])
            n_fx = copy_df(
                conn,
                "raw.fx_rates",
                _with_meta(fx, FX_COLUMNS, storage.uri("raw", keys["fx_rates"]), batch_id, now),
            )
        if blobs["customers"]:
            cust = read_csv(storage, "raw", keys["customers"])
            n_cust = copy_df(
                conn,
                "raw.customers",
                _with_meta(cust, CUSTOMER_COLUMNS, storage.uri("raw", keys["customers"]), batch_id, now),
            )

        record_write(
            conn,
            actor=settings.actor,
            action="insert",
            target="raw.transactions",
            row_count=n_tx,
            batch_id=batch_id,
        )
        log_stage(
            conn,
            batch_id=batch_id,
            run_date=run_date,
            stage="ingest",
            status="success",
            started_at=started_at,
            rows_in=n_tx,
            rows_out=n_tx,
            checksum=checksum,
            source_file=source_file,
            details={"fx_rows": n_fx, "customer_rows": n_cust, "forced": force},
        )
    log.info("ingest_finished", batch_id=batch_id, rows=n_tx, fx_rows=n_fx, customer_rows=n_cust)
    return IngestResult(batch_id, run_date, False, n_tx, n_fx, n_cust, checksum, source_file)
