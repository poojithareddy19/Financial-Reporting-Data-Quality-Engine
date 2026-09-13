"""Every rule type against hand-built staging rows: one positive (failing) and one negative (passing) fixture."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from fin_dq_engine.quality.rules import (
    RULE_REGISTRY,
    RuleContext,
    RuleDefinition,
    build_rule,
    load_rule_definitions,
)

REPO = Path(__file__).resolve().parents[2]
RUN = date(2025, 6, 10)
BATCH = "unit-rules-batch"
PRIOR = "unit-rules-prior"

BASE = {
    "event_id": "E",
    "event_type": "sale",
    "reference_id": "INV-1",
    "posted_date": RUN,
    "created_at": None,
    "entity_id": "ENT-U1",
    "account_id": "ACC-U-AR",
    "customer_id": "CUST-U1",
    "currency": "USD",
    "amount_local": 100.0,
    "amount_usd": 100.0,
    "fx_rate": 1.0,
    "description": "ok",
    "source_system": "ERP",
}

ROWS = [
    {**BASE, "transaction_id": "TXN-000000000001"},  # clean debit
    {
        **BASE,
        "transaction_id": "TXN-000000000002",
        "account_id": "ACC-U-REV",
        "amount_local": -100.0,
        "amount_usd": -100.0,
    },
    {**BASE, "transaction_id": None},  # not_null fail
    {
        **BASE,
        "transaction_id": "TXN-000000000004",
        "currency": "XXX",
        "fx_rate": None,
        "amount_usd": None,
    },  # accepted_values + custom_sql
    {**BASE, "transaction_id": "bad-id", "posted_date": date(1999, 1, 1)},  # regex + range
    {
        **BASE,
        "transaction_id": "TXN-000000000006",
        "account_id": "ACC-MISSING",
        "entity_id": "ENT-MISSING",
        "customer_id": "CUST-MISSING",
    },  # 3x referential
    {
        **BASE,
        "transaction_id": "TXN-000000000007",
        "account_id": "ACC-U-REV",
        "amount_local": 50.0,
        "amount_usd": 50.0,
    },  # sign fail
    {
        **BASE,
        "transaction_id": "TXN-000000000008",
        "amount_local": 99_999_999.0,
        "amount_usd": 99_999_999.0,
    },  # range fail
    {**BASE, "transaction_id": "TXN-000000000009", "description": "  "},  # blank description
    {**BASE, "transaction_id": "TXN-000000000010", "customer_id": None},  # DQ-004 custom_sql
    {**BASE, "transaction_id": "TXN-000000000011", "event_type": "cogs", "customer_id": None, "account_id": "ACC-U-AR"},
    {**BASE, "transaction_id": "TXN-000000000001"},  # unique fail (dup of row 1)
]


@pytest.fixture(scope="module")
def rules_db(pg_engine: Engine) -> Engine:
    with pg_engine.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO curated.dim_entity VALUES ('ENT-U1', 'U1', 'Unit Entity', 'US', 'USD') ON CONFLICT DO NOTHING;
        """)
        )
        conn.execute(
            text("""
            INSERT INTO curated.dim_account VALUES ('ACC-U-AR', '9901', 'Unit AR', 'asset', 'debit'),
                                                  ('ACC-U-REV', '9902', 'Unit Revenue', 'revenue', 'credit')
            ON CONFLICT DO NOTHING""")
        )
        conn.execute(
            text("""
            INSERT INTO curated.dim_customer (customer_id, customer_name, valid_from) VALUES ('CUST-U1', 'Unit Co', '2020-01-01')
            ON CONFLICT DO NOTHING""")
        )
        conn.execute(text("DELETE FROM staging.transactions WHERE batch_id IN (:b, :p)"), {"b": BATCH, "p": PRIOR})
        conn.execute(text("DELETE FROM dq.batch_run_log WHERE batch_id IN (:b, :p)"), {"b": BATCH, "p": PRIOR})
        cols = list(ROWS[0].keys())
        conn.execute(
            text(f"""INSERT INTO staging.transactions ({", ".join(cols)}, ingested_at, batch_id)
                              VALUES ({", ".join(":" + c for c in cols)}, now(), :batch_id)"""),
            [{**r, "batch_id": BATCH} for r in ROWS],
        )
        conn.execute(
            text("""INSERT INTO dq.batch_run_log (batch_id, run_date, stage, status, rows_out)
                             VALUES (:p, :d, 'transform', 'success', 100)"""),
            {"p": PRIOR, "d": RUN - timedelta(days=1)},
        )
    return pg_engine


def _defn(rule_type: str, params: dict, severity: str = "warning") -> RuleDefinition:  # type: ignore[type-arg]
    return RuleDefinition(
        id=f"T-{rule_type}",
        type=rule_type,
        description="t",
        dimension="validity",
        severity=severity,  # type: ignore[arg-type]
        owner="t",
        policy_section="t",
        params=params,
    )


def _run(engine: Engine, defn: RuleDefinition) -> tuple[str, int, list[str]]:
    with engine.connect() as conn:
        out = build_rule(defn).evaluate(conn, RuleContext(BATCH, RUN))
    return out.status, out.rows_failed, out.sample_keys


CASES = [
    ("not_null", {"column": "transaction_id"}, 1, ["<null>"]),
    ("not_null", {"column": "entity_id"}, 0, []),
    ("unique", {"column": "transaction_id"}, 2, ["TXN-000000000001", "TXN-000000000001"]),
    ("unique", {"column": "event_id"}, 12, None),  # every row shares event_id E
    ("accepted_values", {"column": "currency", "values": ["USD", "EUR"]}, 1, ["TXN-000000000004"]),
    ("accepted_values", {"column": "source_system", "values": ["ERP"]}, 0, []),
    ("regex_match", {"column": "transaction_id", "pattern": "^TXN-[0-9]{12}$"}, 1, ["bad-id"]),
    ("regex_match", {"column": "source_system", "pattern": "^ERP$"}, 0, []),
    ("range", {"column": "posted_date", "min": "2000-01-01", "max": "{run_date_plus_1}"}, 1, ["bad-id"]),
    ("range", {"column": "amount_local", "min": -1e7, "max": 1e7}, 1, ["TXN-000000000008"]),
    ("range", {"column": "amount_local", "min": -1e9, "max": 1e9}, 0, []),
    (
        "referential_integrity",
        {"column": "account_id", "ref_table": "curated.dim_account", "ref_column": "account_id"},
        1,
        ["TXN-000000000006"],
    ),
    (
        "referential_integrity",
        {"column": "entity_id", "ref_table": "curated.dim_entity", "ref_column": "entity_id"},
        1,
        None,
    ),
    (
        "referential_integrity",
        {
            "column": "customer_id",
            "ref_table": "curated.dim_customer",
            "ref_column": "customer_id",
            "ref_filter": "is_current",
        },
        1,
        ["TXN-000000000006"],
    ),
    (
        "referential_integrity",
        {"column": "currency", "ref_table": "curated.dim_entity", "ref_column": "functional_currency"},
        1,
        ["TXN-000000000004"],
    ),
    (
        "custom_sql",
        {"sql": "SELECT transaction_id FROM staging.transactions WHERE batch_id = :batch_id AND fx_rate IS NULL"},
        1,
        ["TXN-000000000004"],
    ),
    (
        "custom_sql",
        {
            "sql": "SELECT transaction_id FROM staging.transactions WHERE batch_id = :batch_id AND (description IS NULL OR btrim(description) = '')"
        },
        1,
        ["TXN-000000000009"],
    ),
    (
        "custom_sql",
        {
            "sql": "SELECT transaction_id FROM staging.transactions WHERE batch_id = :batch_id AND customer_id IS NULL AND event_type IN ('sale','payment')"
        },
        1,
        ["TXN-000000000010"],
    ),
    ("custom_sql", {"sql": "SELECT transaction_id FROM staging.transactions WHERE 1 = 0"}, 0, []),
    ("sign_by_account_type", {"account_types": ["revenue"], "event_types": ["sale"]}, 1, ["TXN-000000000007"]),
    ("sign_by_account_type", {"account_types": ["liability"]}, 0, []),
]


@pytest.mark.parametrize(("rule_type", "params", "expected_failed", "expected_samples"), CASES)
def test_row_rules(
    rules_db: Engine, rule_type: str, params: dict, expected_failed: int, expected_samples: list[str] | None
) -> None:  # type: ignore[type-arg]
    status, failed, samples = _run(rules_db, _defn(rule_type, params))
    assert failed == expected_failed
    assert status == ("fail" if expected_failed else "pass")
    if expected_samples is not None:
        assert sorted(samples) == sorted(expected_samples)


def test_sample_keys_capped_at_20(rules_db: Engine) -> None:
    with rules_db.begin() as conn:
        conn.execute(
            text("""INSERT INTO staging.transactions (transaction_id, posted_date, amount_local, ingested_at, batch_id)
                             SELECT 'TXN-CAP-' || g, NULL, 1, now(), 'unit-cap' FROM generate_series(1, 30) g""")
        )
    with rules_db.connect() as conn:
        out = build_rule(_defn("not_null", {"column": "posted_date"})).evaluate(conn, RuleContext("unit-cap", RUN))
    assert out.rows_failed == 30 and len(out.sample_keys) == 20 and out.failure_rate == 1.0


def test_freshness(rules_db: Engine) -> None:
    fresh = _run(rules_db, _defn("freshness", {"column": "posted_date", "max_age_days": 3}))
    assert fresh[0] == "pass"
    with rules_db.connect() as conn:
        out = build_rule(_defn("freshness", {"max_age_days": 3})).evaluate(
            conn, RuleContext(BATCH, RUN + timedelta(days=10))
        )
    assert out.status == "fail" and out.details["age_days"] == 10
    with rules_db.connect() as conn:
        out = build_rule(_defn("freshness", {})).evaluate(conn, RuleContext("no-such-batch", RUN))
    assert out.status == "fail" and out.details["newest"] is None


def test_row_count_delta(rules_db: Engine) -> None:
    status, _, _ = _run(rules_db, _defn("row_count_delta", {"tolerance_pct": 50.0}))
    assert status == "fail"  # 12 rows vs prior 100
    status, _, _ = _run(rules_db, _defn("row_count_delta", {"tolerance_pct": 95.0}))
    assert status == "pass"
    with rules_db.connect() as conn:
        out = build_rule(_defn("row_count_delta", {})).evaluate(
            conn, RuleContext(BATCH, RUN - timedelta(days=3650))
        )  # older than any seeded batch
    assert out.status == "pass" and out.details["prior"] is None


def test_error_status_on_broken_rule(rules_db: Engine) -> None:
    status, _, _ = _run(rules_db, _defn("custom_sql", {"sql": "SELECT nonsense FROM nowhere"}))
    assert status == "error"


def test_identifier_whitelist() -> None:
    with pytest.raises(ValueError):
        build_rule(_defn("not_null", {"column": "x; DROP TABLE y"})).failing_sql(RuleContext(BATCH, RUN))


def test_registry_and_definitions() -> None:
    defs = load_rule_definitions(REPO / "config" / "dq_rules.yaml")
    assert {d.type for d in defs} == set(RULE_REGISTRY)
    assert len(defs) == 17 and len({d.version for d in defs}) == 17
    d = defs[0]
    assert d.version == RuleDefinition(**d.model_dump()).version  # stable hash


def test_definition_validation(tmp_path: Path) -> None:
    p = tmp_path / "rules.yaml"
    p.write_text(
        "rules:\n  - {id: A, type: nope, description: d, dimension: validity, severity: info, owner: o, policy_section: p}\n"
    )
    with pytest.raises(ValueError, match="Unknown rule types"):
        load_rule_definitions(p)
    p.write_text(
        "rules:\n"
        + "  - {id: A, type: not_null, description: d, dimension: validity, severity: info, owner: o, policy_section: p, params: {column: x}}\n"
        * 2
    )
    with pytest.raises(ValueError, match="Duplicate"):
        load_rule_definitions(p)
