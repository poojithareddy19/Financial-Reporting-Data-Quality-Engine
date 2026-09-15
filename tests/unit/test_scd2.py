"""SCD2 customer dimension invariants.

Guards a silent defect: a batch carrying two changes for one customer used to leave two rows with
``is_current = TRUE`` and overlapping validity, so every join on ``is_current`` fanned out and
double-counted that customer's revenue.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from sqlalchemy import Engine, text

from fin_dq_engine.db import transaction
from fin_dq_engine.load.load import merge_customers_scd2

CUSTOMER = "SCD2-TEST-1"
BATCH = "scd2-test-batch"


def _reset(engine: Engine) -> None:
    with transaction(engine) as conn:
        conn.execute(text("DELETE FROM curated.dim_customer WHERE customer_id = :c"), {"c": CUSTOMER})
        conn.execute(text("DELETE FROM staging.customers WHERE batch_id = :b"), {"b": BATCH})


def _seed_current(engine: Engine) -> None:
    with transaction(engine) as conn:
        conn.execute(
            text("""
            INSERT INTO curated.dim_customer
                (customer_id, customer_name, segment, region, country, valid_from, valid_to, is_current)
            VALUES (:c, 'Original', 'smb', 'EU', 'DE', DATE '2025-01-01', DATE '9999-12-31', TRUE)
            """),
            {"c": CUSTOMER},
        )


def _stage(engine: Engine, versions: list[tuple[str, str]]) -> None:
    with transaction(engine) as conn:
        for effective_date, name in versions:
            conn.execute(
                text("""
                INSERT INTO staging.customers
                    (customer_id, customer_name, segment, region, country, effective_date, batch_id)
                VALUES (:c, :n, 'smb', 'EU', 'DE', CAST(:e AS DATE), :b)
                """),
                {"c": CUSTOMER, "n": name, "e": effective_date, "b": BATCH},
            )


def _chain(engine: Engine) -> list[Any]:
    with engine.connect() as conn:
        return list(
            conn.execute(
                text("""
                SELECT customer_name, valid_from, valid_to, is_current
                FROM curated.dim_customer WHERE customer_id = :c ORDER BY valid_from
                """),
                {"c": CUSTOMER},
            )
        )


@pytest.fixture
def clean_customer(pg_engine: Engine) -> Any:
    _reset(pg_engine)
    yield pg_engine
    _reset(pg_engine)


def test_two_changes_in_one_batch_keep_one_current_version(clean_customer: Engine) -> None:
    """The defect: UPDATE ... FROM matched an arbitrary row, leaving two current versions."""
    engine = clean_customer
    _seed_current(engine)
    _stage(engine, [("2025-02-01", "SecondName"), ("2025-03-01", "ThirdName")])
    with transaction(engine) as conn:
        closed, inserted = merge_customers_scd2(conn, BATCH)
    assert (closed, inserted) == (1, 2)

    rows = _chain(engine)
    assert [r.customer_name for r in rows] == ["Original", "SecondName", "ThirdName"]
    assert sum(1 for r in rows if r.is_current) == 1, "exactly one version may be current"
    assert rows[-1].is_current, "the latest version is the current one"
    # contiguous and non-overlapping: each version ends the day before the next begins
    assert [r.valid_to for r in rows] == [date(2025, 1, 31), date(2025, 2, 28), date(9999, 12, 31)]


def test_rerunning_the_same_batch_is_a_no_op(clean_customer: Engine) -> None:
    engine = clean_customer
    _seed_current(engine)
    _stage(engine, [("2025-02-01", "SecondName"), ("2025-03-01", "ThirdName")])
    with transaction(engine) as conn:
        merge_customers_scd2(conn, BATCH)
    before = _chain(engine)
    with transaction(engine) as conn:
        closed, inserted = merge_customers_scd2(conn, BATCH)
    assert (closed, inserted) == (0, 0)
    assert _chain(engine) == before


def test_unchanged_redelivery_creates_no_version(clean_customer: Engine) -> None:
    """A customer redelivered with identical attributes must not accumulate a version per drop."""
    engine = clean_customer
    _seed_current(engine)
    _stage(engine, [("2025-02-01", "Original")])
    with transaction(engine) as conn:
        closed, inserted = merge_customers_scd2(conn, BATCH)
    assert (closed, inserted) == (0, 0)
    rows = _chain(engine)
    assert len(rows) == 1 and rows[0].is_current


def test_unique_index_enforces_the_invariant(clean_customer: Engine) -> None:
    """Even outside the merge, the database refuses a second current version."""
    engine = clean_customer
    _seed_current(engine)
    with pytest.raises(Exception, match="uq_dim_customer_current"), transaction(engine) as conn:
        conn.execute(
            text("""
            INSERT INTO curated.dim_customer
                (customer_id, customer_name, segment, region, country, valid_from, valid_to, is_current)
            VALUES (:c, 'Sneaky', 'smb', 'EU', 'DE', DATE '2025-06-01', DATE '9999-12-31', TRUE)
            """),
            {"c": CUSTOMER},
        )
