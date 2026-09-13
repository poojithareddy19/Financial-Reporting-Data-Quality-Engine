"""Stage 2: raw -> staging. Typed, deduplicated, FX-normalised; Parquet written to the staged layer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

import pandas as pd
from sqlalchemy import Engine

from fin_dq_engine.batchlog import clear_stage_rows, log_stage
from fin_dq_engine.config import Settings
from fin_dq_engine.db import copy_df, execute, query_df, transaction
from fin_dq_engine.governance.audit import record_write
from fin_dq_engine.logging_utils import get_logger
from fin_dq_engine.storage import Storage, get_storage, write_parquet
from fin_dq_engine.transform.clean import cast_types, deduplicate, normalize_fx

log = get_logger(__name__)

STAGING_COLUMNS = [
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
    "amount_usd",
    "fx_rate",
    "description",
    "source_system",
    "ingested_at",
    "batch_id",
]


@dataclass(frozen=True)
class TransformResult:
    """Outcome of :func:`transform_batch`."""

    batch_id: str
    rows_in: int
    rows_out: int
    duplicates_removed: int
    fx_rows: int
    customer_rows: int
    parquet_uri: str


def _upsert_fx_rates(conn: object, batch_id: str) -> int:
    """Type raw FX rows into staging and upsert into curated.dim_fx_rate."""
    from sqlalchemy import Connection

    assert isinstance(conn, Connection)
    clear_stage_rows(conn, batch_id, "staging.fx_rates")
    n = execute(
        conn,
        """
        INSERT INTO staging.fx_rates (currency, rate_date, rate_to_usd, source, batch_id)
        SELECT upper(btrim(currency)), CAST(rate_date AS date), CAST(rate_to_usd AS numeric), source, _batch_id
        FROM raw.fx_rates WHERE _batch_id = :b AND rate_to_usd ~ '^[0-9.]+$'
    """,
        {"b": batch_id},
    )
    execute(
        conn,
        """
        INSERT INTO curated.dim_fx_rate (currency, rate_date, rate_to_usd, source)
        SELECT currency, rate_date, rate_to_usd, source FROM staging.fx_rates WHERE batch_id = :b
        ON CONFLICT (currency, rate_date) DO UPDATE SET rate_to_usd = EXCLUDED.rate_to_usd,
            source = EXCLUDED.source, loaded_at = now()
    """,
        {"b": batch_id},
    )
    return n


def _stage_customers(conn: object, batch_id: str) -> int:
    from sqlalchemy import Connection

    assert isinstance(conn, Connection)
    clear_stage_rows(conn, batch_id, "staging.customers")
    return execute(
        conn,
        """
        INSERT INTO staging.customers (customer_id, customer_name, customer_email, segment, region, country,
                                       customer_since, effective_date, batch_id)
        SELECT customer_id, customer_name, customer_email, segment, region, country,
               CAST(NULLIF(customer_since, '') AS date), CAST(effective_date AS date), _batch_id
        FROM raw.customers WHERE _batch_id = :b AND customer_id IS NOT NULL AND effective_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
    """,
        {"b": batch_id},
    )


def transform_batch(
    settings: Settings,
    engine: Engine,
    batch_id: str,
    run_date: date,
    *,
    storage: Storage | None = None,
) -> TransformResult:
    """Transform one ingested batch into staging (re-runnable: staging rows for the batch are replaced)."""
    started_at = datetime.now(UTC)
    storage = storage or get_storage(settings)
    with transaction(engine) as conn:
        fx_rows = _upsert_fx_rates(conn, batch_id)
        customer_rows = _stage_customers(conn, batch_id)

        raw = query_df(conn, "SELECT * FROM raw.transactions WHERE _batch_id = :b", {"b": batch_id})
        rows_in = len(raw)
        typed = cast_types(
            raw.drop(columns=["_source_file", "_batch_id"]).rename(columns={"_ingested_at": "ingested_at"})
        )
        dedup = deduplicate(typed)
        rates = query_df(conn, "SELECT currency, rate_date, rate_to_usd FROM curated.dim_fx_rate")
        staged = normalize_fx(dedup.frame, rates, settings.pipeline.reporting_currency)
        staged["batch_id"] = batch_id
        staged = staged.reindex(columns=STAGING_COLUMNS)

        clear_stage_rows(conn, batch_id, "staging.transactions")
        rows_out = copy_df(conn, "staging.transactions", staged)
        record_write(
            conn,
            actor=settings.actor,
            action="insert",
            target="staging.transactions",
            row_count=rows_out,
            batch_id=batch_id,
            details={"duplicates_removed": dedup.duplicates_removed},
        )

        parquet_df = staged.copy()
        parquet_df["posted_date"] = pd.to_datetime(parquet_df["posted_date"], errors="coerce")
        uri = write_parquet(
            storage,
            "staged",
            f"transactions/run_date={run_date.isoformat()}/{batch_id}.parquet",
            parquet_df,
        )
        log_stage(
            conn,
            batch_id=batch_id,
            run_date=run_date,
            stage="transform",
            status="success",
            started_at=started_at,
            rows_in=rows_in,
            rows_out=rows_out,
            details={
                "duplicates_removed": dedup.duplicates_removed,
                "fx_rows": fx_rows,
                "customer_rows": customer_rows,
                "parquet": uri,
            },
        )
    log.info(
        "transform_finished",
        batch_id=batch_id,
        rows_in=rows_in,
        rows_out=rows_out,
        duplicates_removed=dedup.duplicates_removed,
    )
    return TransformResult(batch_id, rows_in, rows_out, dedup.duplicates_removed, fx_rows, customer_rows, uri)
