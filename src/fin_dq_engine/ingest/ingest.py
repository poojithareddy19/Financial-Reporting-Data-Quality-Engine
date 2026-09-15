"""Stage 1: land raw files for a run date into the raw schema.

Idempotency: a checksum over all source files for the date identifies the batch. If a successful
ingest with the same checksum exists, the stage is skipped and the existing batch_id returned.

Contract gate: every source header is checked against ``contracts/<feed>.avsc`` before the landing
transaction opens. A breaking verdict aborts with nothing landed, and the evidence is committed on a
transaction of its own so it survives the abort.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy import Connection, Engine

from fin_dq_engine.batchlog import clear_stage_rows, find_ingested_batch, log_stage
from fin_dq_engine.config import Settings
from fin_dq_engine.contracts import (
    Contract,
    SchemaDriftError,
    Verdict,
    header_from_bytes,
    load_contracts,
    validate_feeds,
)
from fin_dq_engine.db import copy_df, execute, transaction
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


def _sync_registry(conn: Connection, contract: Contract) -> None:
    """Upsert the current promise for one feed into governance.contract_registry."""
    execute(
        conn,
        """
        INSERT INTO governance.contract_registry
            (feed, contract_name, version, owner, producing_system, delivery_window,
             fields, nullable_fields, definition)
        VALUES (:feed, :contract_name, :version, :owner, :producing_system, :delivery_window,
                :fields, :nullable_fields, CAST(:definition AS jsonb))
        ON CONFLICT (feed, version) DO UPDATE SET
            owner = EXCLUDED.owner,
            producing_system = EXCLUDED.producing_system,
            delivery_window = EXCLUDED.delivery_window,
            definition = EXCLUDED.definition,
            last_seen_at = clock_timestamp()
        """,
        {
            "feed": contract.feed,
            "contract_name": contract.name,
            "version": contract.version,
            "owner": contract.owner,
            "producing_system": contract.producing_system,
            "delivery_window": contract.delivery_window,
            "fields": list(contract.fields),
            "nullable_fields": sorted(contract.nullable_fields),
            "definition": json.dumps(contract.definition, default=str),
        },
    )


def _record_contract_event(conn: Connection, verdict: Verdict, *, run_date: date, source_file: str | None) -> None:
    """Append one governance.contract_events row. ``batch_id`` stays null for a breaking verdict
    because no batch was ever opened."""
    execute(
        conn,
        """
        INSERT INTO governance.contract_events
            (run_date, feed, contract_name, version, owner, verdict,
             missing_fields, duplicated_fields, unknown_fields, source_file)
        VALUES (:run_date, :feed, :contract_name, :version, :owner, :verdict,
                :missing, :duplicated, :unknown, :source_file)
        """,
        {
            "run_date": run_date,
            "feed": verdict.feed,
            "contract_name": verdict.contract,
            "version": verdict.version,
            "owner": verdict.owner,
            "verdict": verdict.verdict,
            "missing": list(verdict.missing),
            "duplicated": list(verdict.duplicated),
            "unknown": list(verdict.unknown),
            "source_file": source_file,
        },
    )


def validate_contracts(
    settings: Settings,
    engine: Engine,
    run_date: date,
    *,
    blobs: Mapping[str, bytes],
    uris: Mapping[str, str],
) -> dict[str, Verdict]:
    """Check every present feed against its contract before the landing transaction opens.

    Three separate committed transactions, and the split is the whole point. An audit record about a
    failure must never share a transaction with the work that failed: writing the breach inside the
    landing transaction means the exception that correctly refuses the batch also rolls back the only
    evidence of why, leaving an empty table after a run that did exactly the right thing.

    Runs on skipped re-runs too, so a repeated date still re-affirms that the contract held.

    Args:
        settings: Loaded settings; supplies ``paths.contracts_dir``.
        engine: Warehouse engine, used for short independently committed transactions.
        run_date: Date being landed, recorded on every event row.
        blobs: Raw bytes per feed. An absent optional feed has an empty blob and is not validated.
        uris: Source URI per feed, recorded on the event row.

    Returns:
        The verdict for every feed that was present, keyed by feed name.

    Raises:
        SchemaDriftError: At least one feed broke its contract. Nothing has been landed.
        ContractNotFoundError: A feed arrived with no contract to validate it against.
    """
    contracts = load_contracts(settings.paths.contracts_dir)
    with transaction(engine) as conn:  # 1. registry sync, durable regardless of the verdicts below
        for contract in contracts.values():
            _sync_registry(conn, contract)

    headers = {feed: header_from_bytes(blob) for feed, blob in blobs.items() if blob}
    verdicts = validate_feeds(contracts, headers)
    breaking = [v for v in verdicts.values() if v.is_breaking]

    for verdict in breaking:  # 2. breaking verdict, committed before the exception is raised
        with transaction(engine) as conn:
            _record_contract_event(conn, verdict, run_date=run_date, source_file=uris.get(verdict.feed))
        log.error("contract_violation", **verdict.as_log())

    for verdict in verdicts.values():  # 3. compatible or additive verdict
        if verdict.is_breaking:
            continue
        with transaction(engine) as conn:
            _record_contract_event(conn, verdict, run_date=run_date, source_file=uris.get(verdict.feed))
        if verdict.verdict == "additive":
            log.warning("contract_additive", **verdict.as_log())

    if breaking:
        raise SchemaDriftError(breaking)
    return verdicts


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
    uris = {name: storage.uri("raw", key) for name, key in keys.items()}
    source_file = uris["transactions"]

    # The contract gate runs outside the landing transaction on purpose; see validate_contracts.
    validate_contracts(settings, engine, run_date, blobs=blobs, uris=uris)

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
                _with_meta(fx, FX_COLUMNS, uris["fx_rates"], batch_id, now),
            )
        if blobs["customers"]:
            cust = read_csv(storage, "raw", keys["customers"])
            n_cust = copy_df(
                conn,
                "raw.customers",
                _with_meta(cust, CUSTOMER_COLUMNS, uris["customers"], batch_id, now),
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
