"""Reference data seeding: entities, accounts, initial customers and the date dimension."""

from __future__ import annotations

from datetime import date

import pandas as pd
from sqlalchemy import Connection, Engine, text

from fin_dq_engine.config import Settings
from fin_dq_engine.db import execute, insert_rows, records, scalar, transaction
from fin_dq_engine.governance.audit import record_write
from fin_dq_engine.logging_utils import get_logger
from fin_dq_engine.storage import Storage, get_storage, read_csv

log = get_logger(__name__)


def build_dim_date(start: date, end: date) -> pd.DataFrame:
    """Calendar rows from start to end inclusive."""
    days = pd.date_range(start, end, freq="D")
    return pd.DataFrame(
        {
            "date_key": days.strftime("%Y%m%d").astype(int),
            "calendar_date": days.date,
            "year": days.year,
            "quarter": days.quarter,
            "month": days.month,
            "day": days.day,
            "day_of_week": days.dayofweek + 1,
            "is_weekend": days.dayofweek >= 5,
            "month_start": days.to_period("M").start_time.date,
            "month_end": days.to_period("M").end_time.date,
        }
    )


def _backfill_fx_history(conn: Connection, storage: Storage) -> int:
    """Load every raw/fx_rates/*.csv into curated.dim_fx_rate so late-arriving postings can be priced."""
    frames = [read_csv(storage, "raw", key) for key in storage.list_keys("raw", "fx_rates")]
    if not frames:
        return 0
    fx = pd.concat(frames, ignore_index=True)
    fx = fx[fx["rate_to_usd"].str.match(r"^[0-9.]+$")]
    rows = [
        {
            "currency": r["currency"].upper(),
            "rate_date": r["rate_date"],
            "rate_to_usd": r["rate_to_usd"],
            "source": r["source"],
        }
        for r in records(fx)
    ]
    for i in range(0, len(rows), 5000):
        conn.execute(
            text("""
            INSERT INTO curated.dim_fx_rate (currency, rate_date, rate_to_usd, source)
            VALUES (:currency, CAST(:rate_date AS date), CAST(:rate_to_usd AS numeric), :source)
            ON CONFLICT (currency, rate_date) DO NOTHING"""),
            rows[i : i + 5000],
        )
    return len(rows)


def seed_reference_data(
    settings: Settings,
    engine: Engine,
    *,
    storage: Storage | None = None,
    date_start: date = date(2020, 1, 1),
    date_end: date = date(2030, 12, 31),
) -> dict[str, int]:
    """Upsert dims from raw/reference/*.csv and populate dim_date. Safe to run repeatedly."""
    storage = storage or get_storage(settings)
    counts: dict[str, int] = {}
    with transaction(engine) as conn:
        entities = read_csv(storage, "raw", "reference/entities.csv")
        for row in records(entities):
            execute(
                conn,
                """
                INSERT INTO curated.dim_entity (entity_id, entity_code, entity_name, country, functional_currency)
                VALUES (:entity_id, :entity_code, :entity_name, :country, :functional_currency)
                ON CONFLICT (entity_id) DO UPDATE SET entity_code = EXCLUDED.entity_code,
                    entity_name = EXCLUDED.entity_name, country = EXCLUDED.country,
                    functional_currency = EXCLUDED.functional_currency""",
                row,
            )
        counts["dim_entity"] = len(entities)

        accounts = read_csv(storage, "raw", "reference/accounts.csv")
        for row in records(accounts):
            execute(
                conn,
                """
                INSERT INTO curated.dim_account (account_id, account_code, account_name, account_type, normal_balance)
                VALUES (:account_id, :account_code, :account_name, :account_type, :normal_balance)
                ON CONFLICT (account_id) DO UPDATE SET account_code = EXCLUDED.account_code,
                    account_name = EXCLUDED.account_name, account_type = EXCLUDED.account_type,
                    normal_balance = EXCLUDED.normal_balance""",
                row,
            )
        counts["dim_account"] = len(accounts)

        customers = read_csv(storage, "raw", "reference/customers.csv")
        for row in records(customers):
            execute(
                conn,
                """
                INSERT INTO curated.dim_customer (customer_id, customer_name, customer_email, segment, region,
                                                  country, customer_since, valid_from)
                VALUES (:customer_id, :customer_name, :customer_email, :segment, :region, :country,
                        CAST(:customer_since AS date), CAST(:effective_date AS date))
                ON CONFLICT (customer_id, valid_from) DO UPDATE SET customer_name = EXCLUDED.customer_name,
                    customer_email = EXCLUDED.customer_email, segment = EXCLUDED.segment,
                    region = EXCLUDED.region, country = EXCLUDED.country""",
                row,
            )
        counts["dim_customer"] = len(customers)

        counts["dim_fx_rate"] = _backfill_fx_history(conn, storage)
        existing = int(scalar(conn, "SELECT count(*) FROM curated.dim_date") or 0)
        if existing == 0:
            dim_date = build_dim_date(date_start, date_end)
            counts["dim_date"] = insert_rows(conn, "curated.dim_date", records(dim_date))
        else:
            counts["dim_date"] = 0
        for table, n in counts.items():
            record_write(conn, actor=settings.actor, action="upsert", target=f"curated.{table}", row_count=n)
    log.info("seed_finished", **counts)
    return counts
