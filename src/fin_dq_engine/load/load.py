"""Stage 4: staging -> curated. Idempotent upserts for the fact table, SCD Type 2 merge for dim_customer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import Connection, Engine

from fin_dq_engine.batchlog import log_stage
from fin_dq_engine.config import Settings
from fin_dq_engine.db import execute, scalar, transaction
from fin_dq_engine.governance.audit import record_write
from fin_dq_engine.logging_utils import get_logger

log = get_logger(__name__)


@dataclass(frozen=True)
class LoadResult:
    """Row counts recorded around the load."""

    batch_id: str
    fact_rows_before: int
    fact_rows_after: int
    rows_upserted: int
    customers_closed: int
    customers_inserted: int


def merge_customers_scd2(conn: Connection, batch_id: str) -> tuple[int, int]:
    """Apply SCD2 changes from staging.customers.

    For each staged customer whose attributes differ from the current version (or who is new),
    close the current version at ``effective_date - 1`` and insert a new current version.
    Re-running the same batch is a no-op because the new version already matches.
    """
    closed = execute(
        conn,
        """
        WITH changed AS (
            SELECT s.customer_id, s.effective_date
            FROM staging.customers s
            JOIN curated.dim_customer d ON d.customer_id = s.customer_id AND d.is_current
            WHERE s.batch_id = :b
              AND d.valid_from < s.effective_date
              AND (d.customer_name, d.customer_email, d.segment, d.region, d.country) IS DISTINCT FROM
                  (s.customer_name, s.customer_email, s.segment, s.region, s.country)
        )
        UPDATE curated.dim_customer d
        SET valid_to = c.effective_date - 1, is_current = FALSE
        FROM changed c
        WHERE d.customer_id = c.customer_id AND d.is_current
    """,
        {"b": batch_id},
    )
    inserted = execute(
        conn,
        """
        INSERT INTO curated.dim_customer (customer_id, customer_name, customer_email, segment, region, country,
                                          customer_since, valid_from, valid_to, is_current)
        SELECT s.customer_id, s.customer_name, s.customer_email, s.segment, s.region, s.country,
               s.customer_since, s.effective_date, DATE '9999-12-31', TRUE
        FROM staging.customers s
        WHERE s.batch_id = :b
          AND NOT EXISTS (SELECT 1 FROM curated.dim_customer d WHERE d.customer_id = s.customer_id AND d.is_current)
        ON CONFLICT (customer_id, valid_from) DO UPDATE SET is_current = TRUE, valid_to = DATE '9999-12-31',
            customer_name = EXCLUDED.customer_name, customer_email = EXCLUDED.customer_email,
            segment = EXCLUDED.segment, region = EXCLUDED.region, country = EXCLUDED.country
    """,
        {"b": batch_id},
    )
    return closed, inserted


def upsert_facts(conn: Connection, batch_id: str) -> int:
    """Upsert non-quarantined staged rows into curated.fact_transactions."""
    return execute(
        conn,
        """
        INSERT INTO curated.fact_transactions
            (transaction_id, event_id, event_type, reference_id, posted_date, created_at, entity_id, account_id,
             customer_id, currency, amount_local, amount_usd, fx_rate, description, source_system, dq_score, batch_id)
        SELECT transaction_id, event_id, event_type, reference_id, posted_date, created_at, entity_id, account_id,
               customer_id, currency, amount_local, amount_usd, fx_rate, description, source_system,
               COALESCE(dq_score, 100), batch_id
        FROM staging.transactions
        WHERE batch_id = :b AND NOT quarantined
        ON CONFLICT (transaction_id) DO UPDATE SET
            event_id = EXCLUDED.event_id, event_type = EXCLUDED.event_type, reference_id = EXCLUDED.reference_id,
            posted_date = EXCLUDED.posted_date, created_at = EXCLUDED.created_at, entity_id = EXCLUDED.entity_id,
            account_id = EXCLUDED.account_id, customer_id = EXCLUDED.customer_id, currency = EXCLUDED.currency,
            amount_local = EXCLUDED.amount_local, amount_usd = EXCLUDED.amount_usd, fx_rate = EXCLUDED.fx_rate,
            description = EXCLUDED.description, source_system = EXCLUDED.source_system,
            dq_score = EXCLUDED.dq_score, batch_id = EXCLUDED.batch_id, loaded_at = now()
    """,
        {"b": batch_id},
    )


def load_batch(settings: Settings, engine: Engine, batch_id: str, run_date: date) -> LoadResult:
    """Load one validated batch into curated inside a single transaction."""
    started_at = datetime.now(UTC)
    with transaction(engine) as conn:
        before = int(scalar(conn, "SELECT count(*) FROM curated.fact_transactions") or 0)
        closed, inserted = merge_customers_scd2(conn, batch_id)
        upserted = upsert_facts(conn, batch_id)
        after = int(scalar(conn, "SELECT count(*) FROM curated.fact_transactions") or 0)
        record_write(
            conn,
            actor=settings.actor,
            action="upsert",
            target="curated.fact_transactions",
            row_count=upserted,
            batch_id=batch_id,
            details={"before": before, "after": after},
        )
        if closed or inserted:
            record_write(
                conn,
                actor=settings.actor,
                action="upsert",
                target="curated.dim_customer",
                row_count=inserted,
                batch_id=batch_id,
                details={"closed": closed},
            )
        log_stage(
            conn,
            batch_id=batch_id,
            run_date=run_date,
            stage="load",
            status="success",
            started_at=started_at,
            rows_in=upserted,
            rows_out=after - before,
            details={
                "fact_rows_before": before,
                "fact_rows_after": after,
                "customers_closed": closed,
                "customers_inserted": inserted,
            },
        )
    log.info("load_finished", batch_id=batch_id, upserted=upserted, before=before, after=after)
    return LoadResult(batch_id, before, after, upserted, closed, inserted)
