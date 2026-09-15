"""Stage 4: staging -> curated. Idempotent upserts for the fact table, SCD Type 2 merge for dim_customer."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy import Connection, Engine, text

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


_AFFECTED = "SELECT DISTINCT customer_id FROM staging.customers WHERE batch_id = :b"

_ATTRS = "d.customer_name, d.customer_email, d.segment, d.region, d.country"


def _sks(conn: Connection, batch_id: str, *, only_current: bool) -> set[int]:
    """customer_sk values for every customer this batch touches."""
    clause = " AND d.is_current" if only_current else ""
    rows = conn.execute(
        text(f"SELECT d.customer_sk FROM curated.dim_customer d WHERE d.customer_id IN ({_AFFECTED}){clause}"),
        {"b": batch_id},
    )
    return {int(r.customer_sk) for r in rows}


def merge_customers_scd2(conn: Connection, batch_id: str) -> tuple[int, int]:
    """Apply SCD2 changes from staging.customers by rebuilding the affected version chains.

    The earlier implementation closed the current version pairwise. When one batch carried two changes
    for the same customer, ``UPDATE ... FROM`` matched an arbitrary row from the changed set, so the
    batch left two rows with ``is_current = TRUE`` and overlapping validity. Any join on ``is_current``
    then fanned out and double-counted that customer's revenue, silently.

    Three set-based steps instead, each independent of how many changes a customer has in the batch:

    1. Upsert every distinct ``(customer_id, effective_date)`` the batch carries.
    2. Drop any version whose attributes match the version immediately before it, which is what keeps
       an unchanged customer from accumulating a version per delivery.
    3. Recompute ``valid_to`` and ``is_current`` across each affected customer's whole chain, so
       exactly one version is current and the ranges are contiguous.

    Re-running the same batch is a no-op: step 1 is keyed on ``(customer_id, valid_from)`` and steps 2
    and 3 are idempotent.

    Returns:
        ``(closed, inserted)``: versions that stopped being current, and versions newly created.
    """
    before_all = _sks(conn, batch_id, only_current=False)
    before_current = _sks(conn, batch_id, only_current=True)

    conn.execute(
        text("""
        INSERT INTO curated.dim_customer (customer_id, customer_name, customer_email, segment, region,
                                          country, customer_since, valid_from, valid_to, is_current)
        SELECT DISTINCT ON (s.customer_id, s.effective_date)
               s.customer_id, s.customer_name, s.customer_email, s.segment, s.region, s.country,
               s.customer_since, s.effective_date, DATE '9999-12-31', FALSE
        FROM staging.customers s
        WHERE s.batch_id = :b
        ORDER BY s.customer_id, s.effective_date
        ON CONFLICT (customer_id, valid_from) DO UPDATE SET
            customer_name = EXCLUDED.customer_name, customer_email = EXCLUDED.customer_email,
            segment = EXCLUDED.segment, region = EXCLUDED.region, country = EXCLUDED.country,
            customer_since = EXCLUDED.customer_since, is_current = FALSE
        """),
        {"b": batch_id},
    )

    conn.execute(
        text(f"""
        WITH ordered AS (
            SELECT d.customer_sk,
                   ROW_NUMBER() OVER w AS rn,
                   ROW({_ATTRS}) AS attrs,
                   LAG(ROW({_ATTRS})) OVER w AS prev_attrs
            FROM curated.dim_customer d
            WHERE d.customer_id IN ({_AFFECTED})
            WINDOW w AS (PARTITION BY d.customer_id ORDER BY d.valid_from)
        )
        DELETE FROM curated.dim_customer d
        USING ordered o
        WHERE d.customer_sk = o.customer_sk AND o.rn > 1 AND o.attrs IS NOT DISTINCT FROM o.prev_attrs
        """),
        {"b": batch_id},
    )

    # Two statements, not one. uq_dim_customer_current is a plain unique index, so it is enforced per
    # row as the update walks: a single statement that stood one version up while standing another down
    # would trip on the ordering. Standing every affected version down first makes that impossible.
    conn.execute(
        text(f"""
        WITH chain AS (
            SELECT d.customer_sk,
                   LEAD(d.valid_from) OVER (PARTITION BY d.customer_id ORDER BY d.valid_from) AS next_from
            FROM curated.dim_customer d
            WHERE d.customer_id IN ({_AFFECTED})
        )
        UPDATE curated.dim_customer d
        SET valid_to = COALESCE(c.next_from - 1, DATE '9999-12-31'), is_current = FALSE
        FROM chain c
        WHERE d.customer_sk = c.customer_sk
        """),
        {"b": batch_id},
    )
    conn.execute(
        text(f"""
        WITH tail AS (
            SELECT DISTINCT ON (d.customer_id) d.customer_sk
            FROM curated.dim_customer d
            WHERE d.customer_id IN ({_AFFECTED})
            ORDER BY d.customer_id, d.valid_from DESC
        )
        UPDATE curated.dim_customer d
        SET is_current = TRUE, valid_to = DATE '9999-12-31'
        FROM tail t
        WHERE d.customer_sk = t.customer_sk
        """),
        {"b": batch_id},
    )

    after_all = _sks(conn, batch_id, only_current=False)
    after_current = _sks(conn, batch_id, only_current=True)
    return len(before_current - after_current), len(after_all - before_all)


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
